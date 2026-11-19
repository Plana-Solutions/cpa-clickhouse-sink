package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/bodystore"
)

func TestViewManyReferences(t *testing.T) {
	endpoint := os.Getenv("BODY_INTEGRATION_URL")
	if endpoint == "" {
		t.Skip("requires disposable ClickHouse 25.8")
	}
	const db = "view_memory_test"
	query := func(ctx context.Context, q string, p map[string]string) (io.ReadCloser, error) {
		v := url.Values{"max_memory_usage": {"134217728"}, "max_threads": {"1"}, "http_write_exception_in_output_format": {"0"}}
		for k, x := range p {
			v.Set("param_"+k, x)
		}
		req, _ := http.NewRequestWithContext(ctx, "POST", endpoint+"/?"+v.Encode(), strings.NewReader(q))
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		if r.StatusCode != 200 {
			b, _ := io.ReadAll(r.Body)
			r.Body.Close()
			return nil, fmt.Errorf("%s", b)
		}
		return r.Body, nil
	}
	q := func(sql string) string {
		t.Helper()
		rc, err := query(context.Background(), sql, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		b, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(b))
	}
	q("DROP DATABASE IF EXISTS " + db + " SYNC")
	defer q("DROP DATABASE IF EXISTS " + db + " SYNC")
	s := newSink(sinkConfig{URL: endpoint, Database: db, BodyDedup: true, StoreBodies: true})
	s.schemaOK = false
	if err := s.ensureSchema(s.cfg); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id           string
		n, size, cap int
	}{{"many", 385, 8500, 64 << 20}, {"large", 72, 218400, 128 << 20}} {
		t.Run(tc.id, func(t *testing.T) {
			items := make([]string, tc.n)
			for i := range items {
				items[i] = fmt.Sprintf(`{"role":"user","content":"%d%s"}`, i, strings.Repeat("x", tc.size))
			}
			body := `{"messages":[` + strings.Join(items, ",") + `]}`
			want := sha256.Sum256([]byte(body))
			ts := formatTime(time.Now())
			s.addRequest(requestRow{RequestID: tc.id, StartedAt: ts, CompletedAt: ts, RequestHeaders: map[string]string{"session-id": tc.id}, RequestBody: body})
			if err := s.flushBodies(s.cfg); err != nil {
				t.Fatal(err)
			}
			got := q(fmt.Sprintf("SELECT body_status,hex(SHA256(request_body)) FROM %s.raw_request(id='%s') SETTINGS max_memory_usage=%d", db, tc.id, tc.cap))
			if got != "complete\t"+strings.ToUpper(hex.EncodeToString(want[:])) {
				t.Fatal(got)
			}
			if got := q("SELECT length(body_refs) FROM " + db + ".requests WHERE request_id='" + tc.id + "'"); got != fmt.Sprint(tc.n) {
				t.Fatal(got)
			}
		})
	}
	q("INSERT INTO " + db + ".requests(request_id,started_at,completed_at,request_body) VALUES ('fallback',toDateTime64('2026-01-01',9,'UTC'),now64(9),'{}')")
	q("INSERT INTO " + db + ".requests(request_id,started_at,completed_at,body_len,body_sha256,body_refs,body_ref_offsets) VALUES ('fallback',toDateTime64('2026-01-01',9,'UTC'),now64(9),2,SHA256('{}'),[repeat('x',16)],[0])")
	if got := q("SELECT body_status,request_body FROM " + db + ".raw_request(id='fallback')"); got != "complete\t{}" {
		t.Fatal(got)
	}
	r := bodystore.Reader{Database: db, Query: query}
	b, err := r.Read(context.Background(), "fallback")
	if err != nil || string(b) != "{}" {
		t.Fatal(string(b), err)
	}
	err = r.ReadBatch(context.Background(), []string{"fallback"}, func(id, ts string, b []byte) error {
		if string(b) != "{}" {
			t.Fatal(string(b))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
