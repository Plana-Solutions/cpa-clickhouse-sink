package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/deriver/extract"
)

func TestLargeBodyReadPaths(t *testing.T) {
	endpoint := os.Getenv("LARGE_BODY_URL")
	if endpoint == "" {
		t.Skip("set LARGE_BODY_URL to the isolated large corpus")
	}
	ch := newClickHouse(endpoint, "measure", "")
	d := newDeriver(ch, "cliproxy", slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()
	for round := 0; round < 5; round++ {
		ids := []string{}
		for i := 0; i < 16; i++ {
			ids = append(ids, fmt.Sprintf("%X", sha256.Sum256([]byte(fmt.Sprintf("seed-%d", round*997+i)))))
		}
		want := map[string]string{}
		if err := selectRows(ctx, ch, "SELECT request_id,hex(SHA256(request_body)) AS sha FROM cliproxy.requests PREWHERE request_id IN {ids:Array(String)} SETTINGS max_block_size=1", map[string]string{"ids": chArray(ids)}, func(r struct {
			ID  string `json:"request_id"`
			SHA string `json:"sha"`
		}) {
			want[r.ID] = r.SHA
		}); err != nil {
			t.Fatal(err)
		}
		verify := func(id, ts string, b []byte) error {
			h := sha256.Sum256(b)
			if !strings.EqualFold(hex.EncodeToString(h[:]), want[id]) {
				return fmt.Errorf("SHA mismatch for %s", id)
			}
			return nil
		}
		body, err := d.bodyReader().Read(ctx, ids[0])
		if err != nil {
			t.Fatal(err)
		}
		if err = verify(ids[0], "", body); err != nil {
			t.Fatal(err)
		}
		if err = d.bodyReader().ReadBatch(ctx, ids, verify); err != nil {
			t.Fatal(err)
		}
		if err = d.fetch(ctx, ids, func(r extract.Row) error { return verify(r.RequestID, r.StartedAt, []byte(r.RequestBody)) }); err != nil {
			t.Fatal(err)
		}
	}
}
