package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"
)

// A verified inline batch precedes deletion. Remaining skeletons are the checkpoint.
func (d *deriver) restoreBodies(ctx context.Context, dry bool) (migrationReport, error) {
	report := migrationReport{DryRun: dry}
	if err := d.migrationMemory(ctx, 0.60); err != nil {
		return report, err
	}
	rows, err := migrationRows[candidate](ctx, d.ch, "SELECT request_id,body_len AS bytes FROM "+d.db+".requests WHERE body_sha256!=repeat(char(0),32) ORDER BY started_at,request_id LIMIT 1 BY request_id", nil)
	if err != nil {
		return report, err
	}
	for len(rows) > 0 {
		batch := bodyBatch(rows[:min(d.batchSize, len(rows))], d.batchBytes)
		ids := make([]string, len(batch))
		for i, r := range batch {
			ids[i] = r.RequestID
		}
		if err := d.migrationMemory(ctx, 0.75); err != nil {
			return report, err
		}
		err := migrationRetry(ctx, func() error {
			var data []byte
			digests := map[string][32]byte{}
			size := uint64(0)
			err := d.bodyReader().ReadBatch(ctx, ids, func(id, ts string, body []byte) error {
				digest := sha256.Sum256(body)
				digests[id] = digest
				var err error
				data, err = appendCandidate(data, migrationCandidate{ID: id, TS: ts})
				if err != nil {
					return err
				}
				data = binary.AppendUvarint(data, uint64(len(body)))
				data = append(data, body...)
				size += uint64(len(body))
				return nil
			})
			if err != nil {
				return fmt.Errorf("restore batch: %w", err)
			}
			if !dry {
				p := map[string]string{"ids": chArray(ids)}
				q := `INSERT INTO ` + d.db + `.requests SELECT req.* EXCEPT(duration_ms) REPLACE (
 sk.inline AS request_body,0 AS body_len,repeat(char(0),32) AS body_sha256,
 '' AS body_residual,[] AS body_ref_offsets,[] AS body_refs)
 FROM (SELECT * FROM ` + d.db + `.requests PREWHERE request_id IN {ids:Array(String)}
 WHERE body_sha256!=repeat(char(0),32) AND request_id NOT IN (SELECT request_id FROM ` + d.db + `.requests PREWHERE request_id IN {ids:Array(String)} WHERE body_sha256=repeat(char(0),32))) req
 INNER JOIN skeleton sk ON req.request_id=sk.request_id AND req.started_at=sk.started_at LIMIT 1 BY req.request_id`
				if err := d.ch.external(ctx, q, p, candidateStructure+"inline String", data); err != nil {
					return err
				}
				if err := d.verifyMigrationBatch(ctx, ids, digests, true); err != nil {
					return err
				}
				if err := d.ch.exec(ctx, "DELETE FROM "+d.db+".requests WHERE request_id IN {ids:Array(String)} AND body_sha256!=repeat(char(0),32) SETTINGS lightweight_deletes_sync=1", p); err != nil {
					return err
				}
			}
			report.Rows += len(ids)
			report.InlineBytes += size
			return nil
		})
		if err != nil {
			return report, err
		}
		rows = rows[len(batch):]
		if !dry {
			select {
			case <-ctx.Done():
				return report, ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
	return report, nil
}
