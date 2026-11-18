package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// A verified inline copy precedes deletion. Remaining skeletons are the checkpoint.
func (d *deriver) restoreBodies(ctx context.Context, dry bool) (migrationReport, error) {
	report := migrationReport{DryRun: dry}
	type candidate struct {
		ID string `json:"request_id"`
		TS string `json:"started_at"`
	}
	var rows []candidate
	err := selectRows(ctx, d.ch, `SELECT DISTINCT request_id,started_at FROM `+d.db+`.requests WHERE body_sha256!=repeat(char(0),32) ORDER BY started_at,request_id SETTINGS max_threads=1`, nil, func(r candidate) { rows = append(rows, r) })
	if err != nil {
		return report, err
	}
	for _, r := range rows {
		body, err := d.bodyReader().Read(ctx, r.ID, r.TS)
		if err != nil {
			return report, err
		}
		report.Rows++
		report.InlineBytes += uint64(len(body))
		if dry {
			continue
		}
		sha := sha256.Sum256(body)
		params := map[string]string{"id": r.ID, "ts": r.TS, "sha": hex.EncodeToString(sha[:])}
		data := binary.AppendUvarint(nil, uint64(len(body)))
		data = append(data, body...)
		predicate := `request_id={id:String} AND started_at={ts:DateTime64(9,'UTC')}`
		q := `INSERT INTO ` + d.db + `.requests SELECT req.* EXCEPT(duration_ms) REPLACE (
   sk.inline AS request_body, 0 AS body_len, repeat(char(0),32) AS body_sha256,
   '' AS body_residual, [] AS body_ref_offsets, [] AS body_refs)
   FROM (SELECT * FROM ` + d.db + `.requests PREWHERE ` + predicate + ` WHERE body_sha256!=repeat(char(0),32)
   AND request_id NOT IN (SELECT request_id FROM ` + d.db + `.requests PREWHERE ` + predicate + ` WHERE body_sha256=repeat(char(0),32) AND SHA256(request_body)=unhex({sha:String}))) req
   CROSS JOIN skeleton sk LIMIT 1`
		if err := d.ch.external(ctx, q, params, "inline String", data); err != nil {
			return report, err
		}
		q = `SELECT throwIf(count()=0,'restored inline copy missing') FROM ` + d.db + `.requests PREWHERE ` + predicate + ` WHERE body_sha256=repeat(char(0),32) AND SHA256(request_body)=unhex({sha:String})`
		if err := d.ch.exec(ctx, q, params); err != nil {
			return report, fmt.Errorf("verify restore: %w", err)
		}
		if err := d.ch.exec(ctx, `DELETE FROM `+d.db+`.requests WHERE `+predicate+` AND body_sha256!=repeat(char(0),32) SETTINGS lightweight_deletes_sync=1`, params); err != nil {
			return report, err
		}
	}
	return report, nil
}
