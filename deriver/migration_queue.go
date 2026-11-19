package main

import (
	"context"
	"fmt"
)

type migrationCursor struct {
	migrationCandidate
	Done bool `json:"done"`
}

// The snapshot is durable before its cursor can advance. Only verified deletes advance it.
func (d *deriver) migrationQueue(ctx context.Context) (migrationCursor, error) {
	for _, q := range []string{
		"CREATE TABLE IF NOT EXISTS " + d.db + ".body_migration_candidates (request_id String, started_at DateTime64(9,'UTC')) ENGINE=MergeTree ORDER BY (started_at,request_id)",
		"CREATE TABLE IF NOT EXISTS " + d.db + ".body_migration_progress (request_id String, started_at DateTime64(9,'UTC'), done Bool, saved_at DateTime64(9,'UTC') DEFAULT now64(9)) ENGINE=MergeTree ORDER BY saved_at",
	} {
		if err := d.migrationExec(ctx, q, nil); err != nil {
			return migrationCursor{}, err
		}
	}
	rows, err := migrationRows[migrationCursor](ctx, d.ch, "SELECT request_id,started_at,done FROM "+d.db+".body_migration_progress ORDER BY saved_at DESC LIMIT 1", nil)
	if err != nil {
		return migrationCursor{}, err
	}
	if len(rows) != 0 && !rows[0].Done {
		return rows[0], nil
	}
	for _, table := range []string{"body_migration_candidates", "body_migration_progress"} {
		if err := d.migrationExec(ctx, "TRUNCATE TABLE "+d.db+"."+table, nil); err != nil {
			return migrationCursor{}, err
		}
	}
	q := "INSERT INTO " + d.db + `.body_migration_candidates SELECT DISTINCT request_id,started_at FROM ` + d.db + `.requests WHERE request_body!='' AND request_id IN (SELECT request_id FROM ` + d.db + `.request_facts) SETTINGS max_threads=1,max_block_size=1,preferred_block_size_bytes=1048576,insert_deduplicate=0`
	if err := d.migrationExec(ctx, q, nil); err != nil {
		return migrationCursor{}, err
	}
	cursor := migrationCursor{migrationCandidate: migrationCandidate{TS: epoch}}
	return cursor, d.saveMigrationCursor(ctx, cursor)
}

func (d *deriver) saveMigrationCursor(ctx context.Context, c migrationCursor) error {
	return migrationRetry(ctx, func() error {
		return d.ch.insertJSON(ctx, d.db+".body_migration_progress", []any{map[string]any{"request_id": c.ID, "started_at": c.TS, "done": c.Done}})
	})
}

func (d *deriver) nextMigrationCandidates(ctx context.Context, c migrationCursor) ([]migrationCandidate, error) {
	return migrationRows[migrationCandidate](ctx, d.ch, "SELECT DISTINCT request_id,started_at FROM "+d.db+`.body_migration_candidates WHERE (started_at,request_id)>({ts:DateTime64(9,'UTC')},{id:String}) ORDER BY started_at,request_id LIMIT {n:UInt32}`, map[string]string{"ts": c.TS, "id": c.ID, "n": fmt.Sprint(d.batchSize)})
}
