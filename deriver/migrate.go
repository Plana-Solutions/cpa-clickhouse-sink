package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/bodystore"
)

type migrationReport struct {
	Rows           int    `json:"rows"`
	InlineBytes    uint64 `json:"inline_bytes"`
	ProjectedBytes uint64 `json:"projected_uncompressed_bytes"`
	DryRun         bool   `json:"dry_run"`
}

// The remaining legacy rows are the checkpoint. A crash can only leave verified copies.
func (d *deriver) migrate(ctx context.Context, dry bool) (migrationReport, error) {
	report := migrationReport{DryRun: dry}
	type candidate struct {
		ID string `json:"request_id"`
		TS string `json:"started_at"`
	}
	var candidates []candidate
	err := selectRows(ctx, d.ch, `SELECT DISTINCT request_id,started_at FROM `+d.db+`.requests
 WHERE request_body!='' AND request_id IN (SELECT request_id FROM `+d.db+`.request_facts)
 ORDER BY started_at,request_id SETTINGS max_threads=1`, nil, func(r candidate) { candidates = append(candidates, r) })
	if err != nil {
		return report, err
	}
	dryKeys := map[bodystore.Key]bool{}
	var verified []string
	flush := func() error {
		if len(verified) == 0 {
			return nil
		}
		err := d.ch.exec(ctx, `DELETE FROM `+d.db+`.requests WHERE body_sha256=repeat(char(0),32) AND request_id IN {ids:Array(String)} SETTINGS lightweight_deletes_sync=1`, map[string]string{"ids": chArray(verified)})
		if err == nil {
			verified = nil
		}
		return err
	}
	for _, c := range candidates {
		type row struct {
			ID      string            `json:"request_id"`
			TS      string            `json:"started_at"`
			Body    []byte            `json:"body"`
			Headers map[string]string `json:"request_headers"`
			SHA     string            `json:"sha"`
		}
		r := row{ID: c.ID, TS: c.TS}
		err := selectRows(ctx, d.ch, `SELECT request_id,started_at,base64Encode(request_body) AS body,request_headers,hex(SHA256(request_body)) AS sha FROM `+d.db+`.requests PREWHERE request_id={id:String} AND started_at={ts:DateTime64(9,'UTC')} WHERE request_body!='' LIMIT 1 SETTINGS max_threads=1,max_block_size=1,preferred_block_size_bytes=1048576`, map[string]string{"id": r.ID, "ts": r.TS}, func(v row) { r = v })
		if err != nil {
			return report, err
		}
		if r.SHA == "" {
			continue
		}

		sk, items := bodystore.Split(r.Body)
		lineage := bodystore.Lineage(r.Headers, r.Body)
		digest := sha256.Sum256(r.Body)
		if !strings.EqualFold(hex.EncodeToString(digest[:]), r.SHA) {
			return report, bodystore.ErrIncomplete
		}
		report.Rows++
		report.InlineBytes += uint64(len(r.Body))
		report.ProjectedBytes += uint64(len(sk.Residual) + len(sk.Refs)*20 + 36)
		// Restrict key reads to this body, including for long conversations.
		known := map[bodystore.Hash]bool{}
		hashes := make([]string, 0, len(items))
		for h := range items {
			hashes = append(hashes, hex.EncodeToString(h[:]))
		}
		err = selectRows(ctx, d.ch, `SELECT hex(hash) AS hash_hex FROM `+d.db+`.body_blobs WHERE lineage={l:UInt64} AND hash IN (SELECT unhex(arrayJoin({hashes:Array(String)})))`, map[string]string{"l": fmt.Sprint(lineage), "hashes": chArray(hashes)}, func(v struct {
			Hash string `json:"hash_hex"`
		}) {
			b, e := hex.DecodeString(v.Hash)
			if e == nil && len(b) == 16 {
				known[bodystore.Hash(b)] = true
			}
		})
		if err != nil {
			return report, err
		}
		for h, data := range items {
			if known[h] || dryKeys[bodystore.Key{Lineage: lineage, Hash: h}] {
				continue
			}
			report.ProjectedBytes += uint64(len(data) + 40)
			if dry {
				if len(dryKeys) >= 1000000 {
					return report, fmt.Errorf("dry-run exceeds the one-million-key memory limit")
				}
				dryKeys[bodystore.Key{Lineage: lineage, Hash: h}] = true
				continue
			}
			ts, err := time.Parse("2006-01-02 15:04:05.999999999", r.TS)
			if err != nil {
				return report, err
			}
			data := bodystore.EncodeBlob(lineage, h, data, ts)
			rc, err := d.ch.do(ctx, `INSERT INTO `+d.db+`.body_blobs (lineage,hash,data,first_seen) FORMAT RowBinary`, nil, data)
			if err != nil {
				return report, err
			}
			_, err = io.Copy(io.Discard, rc)
			_ = rc.Close()
			if err != nil {
				return report, err
			}
		}
		if dry {
			continue
		}
		params := map[string]string{"id": r.ID, "ts": r.TS}
		q := `INSERT INTO ` + d.db + `.requests SELECT req.* EXCEPT(duration_ms) REPLACE (
 '' AS request_body,sk.lineage AS lineage,sk.body_len AS body_len,sk.body_sha256 AS body_sha256,
 sk.body_residual AS body_residual,sk.body_ref_offsets AS body_ref_offsets,sk.body_refs AS body_refs)
 FROM (SELECT * FROM ` + d.db + `.requests PREWHERE request_id={id:String} AND started_at={ts:DateTime64(9,'UTC')} WHERE request_body!=''
 AND request_id NOT IN (SELECT request_id FROM ` + d.db + `.requests WHERE started_at={ts:DateTime64(9,'UTC')} AND body_sha256!=repeat(char(0),32))) AS req CROSS JOIN skeleton AS sk LIMIT 1`
		if err := d.ch.external(ctx, q, params, bodystore.SkeletonStructure, bodystore.EncodeSkeleton(lineage, sk)); err != nil {
			return report, err
		}

		err = d.ch.exec(ctx, "SELECT throwIf(count()=0,'migrated skeleton missing') FROM "+d.db+".requests PREWHERE request_id={id:String} AND started_at={ts:DateTime64(9,'UTC')} WHERE request_body='' AND body_sha256=unhex({sha:String})", map[string]string{"id": r.ID, "ts": r.TS, "sha": r.SHA})
		if err != nil {
			return report, fmt.Errorf("verify migrated skeleton %s: %w", r.ID, bodystore.ErrIncomplete)
		}
		b, err := d.bodyReader().Read(ctx, r.ID, r.TS)
		if err != nil || sha256.Sum256(b) != digest {
			return report, fmt.Errorf("verify migrated request %s: %w", r.ID, bodystore.ErrIncomplete)
		}
		verified = append(verified, r.ID)
		if len(verified) >= 100 {
			if err := flush(); err != nil {
				return report, err
			}
		}
	}
	return report, flush()
}
