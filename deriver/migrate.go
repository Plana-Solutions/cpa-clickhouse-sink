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
	Rows              int      `json:"rows"`
	InlineBytes       uint64   `json:"inline_bytes"`
	ProjectedBytes    uint64   `json:"projected_uncompressed_bytes"`
	DryRun            bool     `json:"dry_run"`
	SkippedRows       int      `json:"skipped_rows"`
	SkippedBytes      uint64   `json:"skipped_bytes"`
	SkippedRequestIDs []string `json:"skipped_request_ids"`
}

type migrationBody struct {
	migrationCandidate
	Body    []byte            `json:"body"`
	Headers map[string]string `json:"request_headers"`
	SHA     string            `json:"sha"`
}

func (d *deriver) migrate(ctx context.Context, dry bool) (migrationReport, error) {
	report := migrationReport{DryRun: dry, SkippedRequestIDs: []string{}}
	if err := d.migrationMemory(ctx); err != nil {
		return report, err
	}
	// Reserve eight times each inline body for encoding and full-row copies.
	limit := map[string]string{"body_limit": fmt.Sprint(d.ch.memoryBytes / 8)}
	skipped, err := migrationRows[struct {
		ID    string `json:"request_id"`
		Bytes uint64 `json:"bytes"`
	}](ctx, d.ch, "SELECT request_id,length(request_body) AS bytes FROM "+d.db+".requests WHERE length(request_body)>{body_limit:UInt64} ORDER BY request_id SETTINGS max_threads=1,max_block_size=1,max_execution_time=300", limit)
	if err != nil {
		return report, fmt.Errorf("migration size preflight: %w", err)
	}
	for _, r := range skipped {
		report.SkippedRows++
		report.SkippedBytes += r.Bytes
		report.SkippedRequestIDs = append(report.SkippedRequestIDs, r.ID)
		d.log.Info("migration skipping oversized inline body", "request_id", r.ID, "bytes", r.Bytes)
	}
	limit["skipped_ids"] = chArray(report.SkippedRequestIDs)
	cursor := migrationCursor{migrationCandidate: migrationCandidate{TS: epoch}}
	var dryCandidates []migrationCandidate
	if dry {
		var err error
		dryCandidates, err = migrationRows[migrationCandidate](ctx, d.ch, "SELECT DISTINCT request_id,started_at FROM "+d.db+`.requests PREWHERE request_id NOT IN {skipped_ids:Array(String)} WHERE length(request_body)>0 AND length(request_body)<={body_limit:UInt64} AND request_id IN (SELECT request_id FROM `+d.db+`.request_facts) ORDER BY started_at,request_id SETTINGS max_threads=1,max_block_size=1,preferred_block_size_bytes=1048576`, limit)
		if err != nil {
			return report, err
		}
	} else {
		var err error
		cursor, err = d.migrationQueue(ctx, report.SkippedRequestIDs)
		if err != nil {
			return report, err
		}
	}
	dryKeys := map[bodystore.Key]bool{}
	for {
		var candidates []migrationCandidate
		if dry {
			candidates = dryCandidates[:min(d.batchSize, len(dryCandidates))]

		} else {
			var err error
			candidates, err = d.nextMigrationCandidates(ctx, cursor)
			if err != nil {
				return report, err
			}
		}
		if len(candidates) == 0 {
			if !dry {
				cursor.Done = true
				return report, d.saveMigrationCursor(ctx, cursor)
			}
			return report, nil
		}
		var batch []migrationBody
		size := 0
		for _, c := range candidates {
			rows, err := migrationRows[migrationBody](ctx, d.ch, `SELECT request_id,started_at,base64Encode(request_body) AS body,request_headers,hex(SHA256(request_body)) AS sha FROM `+d.db+`.requests PREWHERE request_id={id:String} AND started_at={ts:DateTime64(9,'UTC')} WHERE length(request_body)>0 AND length(request_body)<={body_limit:UInt64} LIMIT 1 SETTINGS max_threads=1,max_block_size=1,preferred_block_size_bytes=1048576`, map[string]string{"id": c.ID, "ts": c.TS, "body_limit": limit["body_limit"]})
			if err != nil {
				return report, fmt.Errorf("read migration request %s: %w", c.ID, err)
			}
			if len(rows) != 0 {
				r := rows[0]
				if r.ID != c.ID || r.TS != c.TS {
					return report, fmt.Errorf("migration returned unexpected row for %s", c.ID)
				}
				if len(batch) > 0 && size+len(r.Body) > d.batchBytes {
					break
				}
				batch = append(batch, r)
				size += len(r.Body)
			}
			cursor.migrationCandidate = c
		}
		if dry {
			for len(dryCandidates) > 0 && (dryCandidates[0].TS < cursor.TS || (dryCandidates[0].TS == cursor.TS && dryCandidates[0].ID <= cursor.ID)) {
				dryCandidates = dryCandidates[1:]
			}
		}
		if err := d.migrationMemory(ctx); err != nil {
			return report, err
		}
		if err := migrationRetry(ctx, func() error { return d.migrateBatch(ctx, batch, dry, dryKeys, &report) }); err != nil {
			return report, err
		}
		if !dry {
			if err := d.saveMigrationCursor(ctx, cursor); err != nil {
				return report, err
			}
			select {
			case <-ctx.Done():
				return report, ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
}

func (d *deriver) migrateBatch(ctx context.Context, rows []migrationBody, dry bool, dryKeys map[bodystore.Key]bool, report *migrationReport) error {
	newDryKeys := map[bodystore.Key]bool{}
	var blobs, skeletons []byte
	ids := []string{}
	digests := map[string][32]byte{}
	keys := map[bodystore.Key]bool{}
	projected := uint64(0)
	inline := uint64(0)
	for _, r := range rows {
		digest := sha256.Sum256(r.Body)
		if !strings.EqualFold(hex.EncodeToString(digest[:]), r.SHA) {
			return fmt.Errorf("migration request %s: %w", r.ID, bodystore.ErrIncomplete)
		}
		sk, items := bodystore.Split(r.Body)
		lineage := bodystore.Lineage(r.Headers, r.Body)
		hashes := []string{}
		for h := range items {
			hashes = append(hashes, hex.EncodeToString(h[:]))
		}
		known, err := migrationRows[struct {
			Hash string `json:"hash_hex"`
		}](ctx, d.ch, `SELECT hex(hash) AS hash_hex FROM `+d.db+`.body_blobs WHERE lineage={l:UInt64} AND hash IN (SELECT unhex(arrayJoin({hashes:Array(String)})))`, map[string]string{"l": fmt.Sprint(lineage), "hashes": chArray(hashes)})
		if err != nil {
			return fmt.Errorf("load migration blobs %s: %w", r.ID, err)
		}
		for _, h := range known {
			b, err := hex.DecodeString(h.Hash)
			if err != nil || len(b) != 16 {
				return fmt.Errorf("invalid blob hash for %s", r.ID)
			}
			keys[bodystore.Key{Lineage: lineage, Hash: bodystore.Hash(b)}] = true
		}
		ts, err := time.Parse("2006-01-02 15:04:05.999999999", r.TS)
		if err != nil {
			return err
		}
		for h, data := range items {
			k := bodystore.Key{Lineage: lineage, Hash: h}
			if keys[k] || dryKeys[k] {
				continue
			}
			keys[k] = true
			projected += uint64(len(data) + 48)
			if dry {
				if len(dryKeys)+len(newDryKeys) >= 1000000 {
					return fmt.Errorf("dry-run exceeds the one-million-key memory limit")
				}
				newDryKeys[k] = true
			} else {
				blobs = append(blobs, bodystore.EncodeBlob(lineage, h, data, ts)...)
			}
		}
		skeletons, err = appendCandidate(skeletons, r.migrationCandidate)
		if err != nil {
			return err
		}
		skeletons = append(skeletons, bodystore.EncodeSkeleton(lineage, sk)...)
		ids = append(ids, r.ID)
		digests[r.ID] = digest
		inline += uint64(len(r.Body))
		// Eight bytes per length prefix also cover MergeTree array size streams.
		projected += uint64(len(sk.Residual) + len(sk.Offsets)*4 + len(sk.Refs)*16 + 68)
	}
	if !dry && len(ids) > 0 {
		if len(blobs) > 0 {
			rc, err := d.ch.do(ctx, "INSERT INTO "+d.db+".body_blobs (lineage,hash,data,first_seen) SETTINGS insert_deduplicate=0 FORMAT RowBinary", nil, blobs)
			if err != nil {
				return err
			}
			_, err = io.Copy(io.Discard, rc)
			_ = rc.Close()
			if err != nil {
				return err
			}
		}
		p := map[string]string{"ids": chArray(ids), "body_limit": fmt.Sprint(d.ch.memoryBytes / 8)}
		q := `INSERT INTO ` + d.db + `.requests SELECT req.* EXCEPT(duration_ms) REPLACE (
 '' AS request_body,sk.lineage AS lineage,sk.body_len AS body_len,sk.body_sha256 AS body_sha256,
 sk.body_residual AS body_residual,sk.body_ref_offsets AS body_ref_offsets,sk.body_refs AS body_refs)
 FROM (SELECT * FROM ` + d.db + `.requests PREWHERE request_id IN {ids:Array(String)}
 WHERE length(request_body)>0 AND length(request_body)<={body_limit:UInt64} AND request_id NOT IN (SELECT request_id FROM ` + d.db + `.requests PREWHERE request_id IN {ids:Array(String)} WHERE body_sha256!=repeat(char(0),32))) req
 INNER JOIN skeleton sk ON req.request_id=sk.request_id AND req.started_at=sk.started_at LIMIT 1 BY req.request_id SETTINGS max_threads=1,max_block_size=1`
		if err := d.ch.external(ctx, q, p, candidateStructure+bodystore.SkeletonStructure, skeletons); err != nil {
			return err
		}
		if err := d.verifyMigrationBatch(ctx, ids, digests, false); err != nil {
			return err
		}
		if err := d.ch.exec(ctx, "DELETE FROM "+d.db+".requests WHERE request_id IN {ids:Array(String)} AND body_sha256=repeat(char(0),32) AND length(request_body)<={body_limit:UInt64} SETTINGS max_threads=1,max_block_size=1,lightweight_deletes_sync=1,max_execution_time=300", p); err != nil {
			return err
		}
	}
	for k := range newDryKeys {
		dryKeys[k] = true
	}
	report.Rows += len(rows)
	report.InlineBytes += inline
	report.ProjectedBytes += projected
	return nil
}

func (d *deriver) verifyMigrationBatch(ctx context.Context, ids []string, want map[string][32]byte, inline bool) error {
	filter := "body_sha256!=repeat(char(0),32)"
	hash := "hex(body_sha256)"
	if inline {
		filter = "body_sha256=repeat(char(0),32)"
		hash = "hex(SHA256(request_body))"
	}
	rows, err := migrationRows[struct {
		ID  string `json:"request_id"`
		SHA string `json:"sha"`
	}](ctx, d.ch, "SELECT request_id,"+hash+" AS sha FROM "+d.db+".requests PREWHERE request_id IN {ids:Array(String)} WHERE "+filter+" LIMIT 1 BY request_id SETTINGS max_threads=1,max_block_size=1", map[string]string{"ids": chArray(ids)})
	if err != nil {
		return fmt.Errorf("verify migration batch: %w", err)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		digest, ok := want[r.ID]
		if !ok || !strings.EqualFold(r.SHA, hex.EncodeToString(digest[:])) {
			return fmt.Errorf("verify migration request %s: %w", r.ID, bodystore.ErrIncomplete)
		}
		seen[r.ID] = true
	}
	for _, id := range ids {
		if !seen[id] {
			return fmt.Errorf("verify migration request %s: %w", id, bodystore.ErrIncomplete)
		}
	}
	if inline {
		return nil
	}
	return d.bodyReader().ReadSkeletonBatch(ctx, ids, func(id, ts string, body []byte) error {
		if sha256.Sum256(body) != want[id] {
			return fmt.Errorf("verify migrated request %s: %w", id, bodystore.ErrIncomplete)
		}
		return nil
	})
}
