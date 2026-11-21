package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// Qualification requires a disposable corpus and its pre-migration SHA-256 table.
func TestQualificationAudit(t *testing.T) {
	if os.Getenv("QUALIFICATION_AUDIT") != "1" {
		t.Skip("requires isolated production-shaped corpus")
	}
	ctx := context.Background()
	ch := newClickHouse(os.Getenv("CLICKHOUSE_URL"), "", "")
	ch.memoryBytes = 128 << 20
	d := newDeriver(ch, "cliproxy", slog.Default(), nil)
	type expected struct {
		ID  string `json:"request_id"`
		SHA string `json:"sha"`
	}
	want := map[string]string{}
	if err := selectRows(ctx, ch, "SELECT request_id,sha FROM cliproxy.expected", nil, func(r expected) { want[r.ID] = r.SHA }); err != nil {
		t.Fatal(err)
	}
	var live struct {
		Expected []string `json:"expected"`
		Failures []string `json:"failures"`
	}
	b, err := os.ReadFile(os.Getenv("QUALIFICATION_EXPECTED"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &live); err != nil {
		t.Fatal(err)
	}
	if len(live.Failures) > 0 {
		t.Fatal(live.Failures)
	}
	liveWant := map[string]bool{}
	for _, sha := range live.Expected {
		liveWant[sha] = true
	}
	if len(liveWant) != len(live.Expected) {
		t.Fatal("duplicate generated live body")
	}
	var candidates []candidate
	err = selectRows(ctx, ch, "SELECT request_id,body_len AS bytes FROM cliproxy.requests ORDER BY started_at,request_id", nil, func(c candidate) { candidates = append(candidates, c) })
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	legacy, liveCount := 0, 0
	for len(candidates) > 0 {
		batch := bodyBatch(candidates[:min(16, len(candidates))], 8<<20)
		ids := make([]string, len(batch))
		for i, c := range batch {
			ids[i] = c.RequestID
		}
		err = d.bodyReader().ReadBatch(ctx, ids, func(id, ts string, body []byte) error {
			if seen[id] {
				return fmt.Errorf("duplicate id %s", id)
			}
			seen[id] = true
			digest := sha256.Sum256(body)
			sha := strings.ToUpper(hex.EncodeToString(digest[:]))
			if expected, ok := want[id]; ok {
				if sha != expected {
					return fmt.Errorf("legacy digest mismatch %s", id)
				}
				legacy++
			} else {
				if !liveWant[sha] {
					return fmt.Errorf("unexpected live digest %s", id)
				}
				delete(liveWant, sha)
				liveCount++
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		candidates = candidates[len(batch):]
	}
	if legacy != len(want) || len(liveWant) != 0 {
		t.Fatalf("missing bodies legacy=%d/%d live=%d/%d", legacy, len(want), liveCount, len(live.Expected))
	}
	type count struct {
		N uint64 `json:"n"`
	}
	bad := uint64(0)
	err = selectRows(ctx, ch, `SELECT count() AS n FROM cliproxy.requests r INNER JOIN cliproxy.expected e USING(request_id) WHERE hex(SHA256(toJSONString(r.response_chunks)))!=e.response_sha SETTINGS max_block_size=1`, nil, func(r count) { bad = r.N })
	if err != nil || bad != 0 {
		t.Fatal("response chunk mismatch", bad, err)
	}
	err = selectRows(ctx, ch, `SELECT count() AS n FROM cliproxy.request_facts WHERE parse_status='body_incomplete'`, nil, func(r count) { bad = r.N })
	if err != nil || bad != 0 {
		t.Fatal("incomplete facts", bad, err)
	}
	t.Logf("byte-exact audit legacy=%d live=%d response_mismatches=0 duplicate_ids=0 missing_bodies=0 body_incomplete=0", legacy, liveCount)
}
