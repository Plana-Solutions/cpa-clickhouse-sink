package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/deriver/extract"
)

// ClickHouse 25.8 (http_write_exception_in_output_format=1) ends a failed stream with HTTP 200 + {"exception": ...}.
func TestMidStreamExceptionRejectsBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("http_write_exception_in_output_format") != "0" {
			t.Error("exception format setting missing")
		}
		b, _ := io.ReadAll(r.Body)
		q := string(b)
		switch {
		case strings.Contains(q, "FROM cliproxy.body_blobs"):
			// no blobs referenced
		case strings.Contains(q, "base64Encode(request_body) AS body_inline"):
			io.WriteString(w, `{"request_id":"a","ingested_at":"2026-11-18 10:00:00.000000000","body_inline":"e30=","lineage":0,"body_len":0,"body_sha":"`+strings.Repeat("0", 64)+`","body_residual_b64":"","body_ref_offsets":[],"body_hashes":[]}`+"\n")
			io.WriteString(w, `{"exception": "Code: 241. DB::Exception: (total) memory limit exceeded"}`+"\n")
		}
	}))
	defer srv.Close()
	d := newDeriver(newClickHouse(srv.URL, "", ""), "cliproxy", slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	var got []extract.Row
	err := d.fetch(context.Background(), []string{"a", "b"}, func(r extract.Row) error { got = append(got, r); return nil })
	if err == nil || len(got) != 1 {
		t.Fatalf("err=%v rows=%d", err, len(got))
	}
	for _, r := range got {
		t.Logf("row request_id=%q body_incomplete=%v", r.RequestID, r.BodyIncomplete)
	}
}

func TestFetchRejectsMissingDuplicateAndUnexpectedIDs(t *testing.T) {
	for _, ids := range [][]string{{"a"}, {"a", "a"}, {"a", "other"}} {
		t.Run(strings.Join(ids, ","), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				if !strings.Contains(string(b), "AS body_inline") {
					return
				}
				for _, id := range ids {
					fmt.Fprintf(w, `{"request_id":%q,"body_inline":"e30=","body_sha":%q}`+"\n", id, strings.Repeat("0", 64))
				}
			}))
			defer srv.Close()
			d := newDeriver(newClickHouse(srv.URL, "", ""), "cliproxy", slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			if err := d.fetch(context.Background(), []string{"a", "b"}, func(extract.Row) error { return nil }); err == nil {
				t.Fatal("invalid batch accepted")
			}
		})
	}
}
