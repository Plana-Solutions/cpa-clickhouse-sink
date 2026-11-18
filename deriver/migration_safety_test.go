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
)

func TestMigrationMemoryThresholds(t *testing.T) {
	for _, resident := range []int{59, 60, 74, 75} {
		t.Run(fmt.Sprint(resident), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"resident":%d,"limit":100}`, resident)
			}))
			defer srv.Close()
			d := newDeriver(newClickHouse(srv.URL, "", ""), "cliproxy", slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			for _, threshold := range []float64{.60, .75} {
				err := d.migrationMemory(context.Background(), threshold)
				if (err != nil) != (float64(resident) >= threshold*100) {
					t.Fatalf("resident=%d threshold=%f err=%v", resident, threshold, err)
				}
			}
		})
	}
}

func TestMigrationResumesDurableSnapshot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		q := string(b)
		if strings.Contains(q, "ORDER BY saved_at DESC") {
			fmt.Fprintln(w, `{"request_id":"verified","started_at":"2026-11-01 00:00:00.000000001","done":false}`)
		} else if !strings.HasPrefix(q, "CREATE TABLE") {
			t.Errorf("resume rescanned or rewrote snapshot: %s", q)
		}
	}))
	defer srv.Close()
	d := newDeriver(newClickHouse(srv.URL, "", ""), "cliproxy", slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	cursor, err := d.migrationQueue(context.Background())
	if err != nil || cursor.ID != "verified" {
		t.Fatal(cursor, err)
	}
}

func TestMigrationRetriesMemoryExceptionWithoutPartialRows(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			fmt.Fprintln(w, `{"request_id":"partial"}`)
			fmt.Fprintln(w, `{"exception":"Code: 241. memory limit"}`)
			return
		}
		fmt.Fprintln(w, `{"request_id":"complete"}`)
	}))
	defer srv.Close()
	rows, err := migrationRows[migrationCandidate](context.Background(), newClickHouse(srv.URL, "", ""), "SELECT request_id", nil)
	if err != nil || calls != 2 || len(rows) != 1 || rows[0].ID != "complete" {
		t.Fatal(rows, calls, err)
	}
}
