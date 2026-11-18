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
		transient := errors.As(err, &ch) && chCode.FindStringSubmatch(ch.msg) != nil && chCode.FindStringSubmatch(ch.msg)[1] == "241"
		transient = transient || errors.As(err, &network) || errors.Is(err, io.ErrUnexpectedEOF) || (errors.As(err, &stream) && stream.Code == 241)
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

func (d *deriver) migrationMemory(ctx context.Context, fraction float64) error {
	type metric struct {
		Resident float64 `json:"resident"`
		Limit    float64 `json:"limit"`
	}
	rows, err := migrationRows[metric](ctx, d.ch, `SELECT
    (SELECT value FROM system.asynchronous_metrics WHERE metric='MemoryResident') AS resident,
    (SELECT toFloat64(value) FROM system.server_settings WHERE name='max_server_memory_usage') AS limit`, nil)
	if err != nil {
		return fmt.Errorf("server memory check requires SELECT on system.asynchronous_metrics and system.server_settings: %w", err)
	}
	if len(rows) != 1 || rows[0].Resident <= 0 || rows[0].Limit <= 0 {
		return fmt.Errorf("server memory metrics unavailable; migration refused")
	}
	r := rows[0]
	if r.Resident >= r.Limit*fraction {
		return fmt.Errorf("server MemoryResident %.0f exceeds %.0f%% of server limit %.0f; stop and inspect memory", r.Resident, fraction*100, r.Limit)
	}
	return nil
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
