package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/bodystore"
)

func migrationRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		var ch *chError
		var network net.Error
		var stream *bodystore.StreamError
		code := 0
		if errors.As(err, &ch) {
			if m := chCode.FindStringSubmatch(ch.msg); m != nil {
				fmt.Sscan(m[1], &code)
			}
		} else if errors.As(err, &stream) {
			code = stream.Code
		}
		transient := code == 241 || code == 394 || code == 159 || errors.As(err, &network) || errors.Is(err, io.ErrUnexpectedEOF)
		if !transient {
			return err
		}
		if attempt == 4 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second << attempt):
		}
	}
	return err
}

func migrationRows[T any](ctx context.Context, c *clickhouse, query string, params map[string]string) ([]T, error) {
	var rows []T
	err := migrationRetry(ctx, func() error {
		rows = nil
		return selectRows(ctx, c, query, params, func(r T) { rows = append(rows, r) })
	})
	return rows, err
}

type migrationMemory struct {
	Resident float64 `json:"resident"`
	Tracked  float64 `json:"tracked"`
	Limit    float64 `json:"limit"`
	Total    float64 `json:"total"`
}

func (m migrationMemory) available(queryBytes int64) bool {
	return m.Tracked+float64(queryBytes)+(128<<20) < m.Limit && m.Resident+float64(queryBytes) < m.Total && m.Resident+(128<<20) < m.Limit
}

func (d *deriver) migrationMemory(ctx context.Context) error {
	for {
		rows, err := migrationRows[migrationMemory](ctx, d.ch, `SELECT
    (SELECT value FROM system.asynchronous_metrics WHERE metric='MemoryResident') AS resident,
    (SELECT value FROM system.asynchronous_metrics WHERE metric='TrackedMemory') AS tracked,
    (SELECT value FROM system.asynchronous_metrics WHERE metric='CGroupMemoryTotal') AS total,
    (SELECT toFloat64(value) FROM system.server_settings WHERE name='max_server_memory_usage') AS limit`, nil)
		if err != nil {
			return fmt.Errorf("server memory check requires SELECT on system.asynchronous_metrics and system.server_settings: %w", err)
		}
		if len(rows) != 1 || rows[0].Resident <= 0 || rows[0].Tracked <= 0 || rows[0].Limit <= 0 || rows[0].Total <= 0 {
			return fmt.Errorf("server memory metrics or cgroup limit unavailable; migration refused")
		}
		m := rows[0]
		if m.available(d.ch.memoryBytes) {
			return nil
		}
		if m.Resident+(128<<20) >= m.Limit {
			if err := d.ch.exec(ctx, "SYSTEM JEMALLOC PURGE", nil); err != nil {
				return fmt.Errorf("migration memory pressure requires SYSTEM JEMALLOC PURGE privilege: %w", err)
			}
		}
		d.log.Info("migration waiting for memory headroom", "resident", m.Resident, "tracked", m.Tracked, "server_limit", m.Limit, "container_limit", m.Total)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

type migrationCandidate struct {
	ID string `json:"request_id"`
	TS string `json:"started_at"`
}

func appendCandidate(data []byte, r migrationCandidate) ([]byte, error) {
	ts, err := time.Parse("2006-01-02 15:04:05.999999999", r.TS)
	if err != nil {
		return nil, err
	}
	data = binary.AppendUvarint(data, uint64(len(r.ID)))
	data = append(data, r.ID...)
	return binary.LittleEndian.AppendUint64(data, uint64(ts.UnixNano())), nil
}

const candidateStructure = "request_id String, started_at DateTime64(9,'UTC'), "

func (d *deriver) migrationExec(ctx context.Context, q string, p map[string]string) error {
	return migrationRetry(ctx, func() error { return d.ch.exec(ctx, q, p) })
}
