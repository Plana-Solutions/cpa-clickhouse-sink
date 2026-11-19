package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/bodystore"
	"time"
)

func TestMigrationMemoryHeadroom(t *testing.T) {
	for _, tc := range []struct {
		name              string
		tracked, resident float64
		want              bool
	}{
		{"normal RSS above old admission", 600 << 20, 1100 << 20, true},
		{"reserve live inserts", 1200 << 20, 1300 << 20, false},
		{"reserve container headroom", 600 << 20, 1850 << 20, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := migrationMemory{Tracked: tc.tracked, Resident: tc.resident, Limit: 1536 << 20, Total: 2048 << 20}
			if m.available(256<<20) != tc.want {
				t.Fatal(m)
			}
		})
	}
}

func TestMigrationMemoryWaitCancelsAndMissingGrantsRefuse(t *testing.T) {
	for _, denied := range []bool{false, true} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if denied {
				w.WriteHeader(403)
				return
			}
			fmt.Fprintln(w, `{"resident":1900000000,"tracked":1300000000,"limit":1610612736,"total":2147483648}`)
		}))
		d := newDeriver(newClickHouse(srv.URL, "", ""), "cliproxy", slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err := d.migrationMemory(ctx)
		cancel()
		srv.Close()
		if err == nil {
			t.Fatal("unsafe admission")
		}
		if denied && !strings.Contains(err.Error(), "requires SELECT") {
			t.Fatal(err)
		}
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

func TestDryRunCommitsKeysOnlyAfterSuccess(t *testing.T) {
	fail := true
	lookups := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lookups++
		if fail && lookups == 2 {
			w.WriteHeader(400)
		}
	}))
	defer srv.Close()
	d := newDeriver(newClickHouse(srv.URL, "", ""), "cliproxy", slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	var rows []migrationBody
	minimum := uint64(0)
	for i, body := range []string{`{"messages":["one"]}`, `{"messages":["two"]}`} {
		raw := []byte(body)
		sha := sha256.Sum256(raw)
		rows = append(rows, migrationBody{migrationCandidate: migrationCandidate{ID: fmt.Sprint(i), TS: "2026-01-01 00:00:00.000000000"}, Body: raw, SHA: hex.EncodeToString(sha[:])})
		sk, items := bodystore.Split(raw)
		minimum += uint64(len(bodystore.EncodeSkeleton(0, sk)))
		for h, data := range items {
			minimum += uint64(len(bodystore.EncodeBlob(0, h, data, time.Now())) + 8)
		}
	}
	keys := map[bodystore.Key]bool{}
	report := migrationReport{DryRun: true}
	if err := d.migrateBatch(context.Background(), rows, true, keys, &report); err == nil || len(keys) != 0 || report.Rows != 0 {
		t.Fatal("failed dry run changed accounting", err, report, len(keys))
	}
	fail = false
	if err := d.migrateBatch(context.Background(), rows, true, keys, &report); err != nil {
		t.Fatal(err)
	}
	if report.Rows != 2 || report.ProjectedBytes < minimum {
		t.Fatal("dry run underestimates serialized body storage", report, minimum)
	}
}

func TestMigrationRetriesCancelledDelete(t *testing.T) {
	for _, code := range []int{394, 159} {
		calls := 0
		err := migrationRetry(context.Background(), func() error {
			calls++
			if calls == 1 {
				return &chError{status: 500, msg: fmt.Sprintf("Code: %d. pending delete", code)}
			}
			return nil
		})
		if err != nil || calls != 2 {
			t.Fatal(code, calls, err)
		}
	}
}
