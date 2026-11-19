package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestMigrationKillHelper(t *testing.T) {
	if os.Getenv("MIGRATION_KILL_HELPER") != "1" {
		t.Skip("subprocess only")
	}
	ch := newClickHouse(os.Getenv("CLICKHOUSE_URL"), "", "")
	ch.memoryBytes = 128 << 20
	d := newDeriver(ch, testDB, slog.New(slog.NewTextHandler(os.Stdout, nil)), nil)
	d.batchSize = 1
	if _, err := d.migrate(context.Background(), false); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationSkipsOversized(t *testing.T) {
	d := testDeriver(t)
	d.ch.memoryBytes = 512 << 20
	ctx := context.Background()
	want := map[string][32]byte{}
	sizes := map[string]int{}
	for i, n := range []int{1024, 16 << 20, (16 << 20) + 1, 34092805} {
		r := fixtureRow(t, "chat_stream", fmt.Sprintf("limit-%d", i), time.Now().Add(-time.Hour))
		prefix, suffix := `{"messages":[{"role":"user","content":"`, `"}]}`
		r.RequestBody = prefix + strings.Repeat("b", n-len(prefix)-len(suffix)) + suffix
		insertRaw(t, d, r)
		want[r.RequestID] = sha256.Sum256([]byte(r.RequestBody))
		sizes[r.RequestID] = n
	}
	for i := 0; i < 40; i++ {
		r := fixtureRow(t, "chat_stream", fmt.Sprintf("normal-%02d", i), time.Now().Add(-30*time.Minute))
		insertRaw(t, d, r)
		want[r.RequestID] = sha256.Sum256([]byte(r.RequestBody))
		sizes[r.RequestID] = len(r.RequestBody)
	}
	if _, err := d.rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	d.ch.memoryBytes = 128 << 20
	prepareMigrationMemory(t, d)
	dry, err := d.migrate(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	checkSkips := func(r migrationReport) {
		t.Helper()
		if r.SkippedRows != 2 || r.SkippedBytes != 50870022 || strings.Join(r.SkippedRequestIDs, ",") != "limit-2,limit-3" {
			t.Fatal(r)
		}
	}
	checkSkips(dry)
	var expected uint64
	for id, n := range sizes {
		if id != "limit-2" && id != "limit-3" {
			expected += uint64(n)
		}
	}
	if dry.Rows != 42 || dry.InlineBytes != expected {
		t.Fatal(dry, expected)
	}
	if got := scalar(t, d, "SELECT countIf(length(request_body)>0) FROM "+testDB+".requests SETTINGS max_threads=1,max_block_size=1"); got != "44" {
		t.Fatal(got)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.CreateTemp(t.TempDir(), "kill-log")
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(exe, "-test.run=^TestMigrationKillHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "MIGRATION_KILL_HELPER=1")
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	deadline := time.Now().Add(60 * time.Second)
	for {
		rows, err := migrationRows[migrationCursor](ctx, d.ch, "SELECT request_id,started_at,done FROM "+testDB+".body_migration_progress ORDER BY saved_at DESC LIMIT 1", nil)
		// The progress table may not exist before the child creates its snapshot.
		if err == nil && len(rows) > 0 && rows[0].ID != "" && !rows[0].Done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child made no progress")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	logged, _ := os.ReadFile(log.Name())
	if strings.Count(string(logged), "migration skipping oversized inline body") != 2 {
		t.Fatalf("skip logs: %s", logged)
	}
	if got := scalar(t, d, "SELECT countIf(request_id IN ('limit-2','limit-3')) FROM "+testDB+".body_migration_candidates"); got != "0" {
		t.Fatal("snapshot included oversized rows", got)
	}
	// An unfinished plana.9 snapshot can still contain oversized candidates.
	if err := d.ch.exec(ctx, "INSERT INTO "+testDB+".body_migration_candidates SELECT request_id,started_at FROM "+testDB+".requests PREWHERE request_id IN ('limit-2','limit-3')", nil); err != nil {
		t.Fatal(err)
	}
	report, err := d.migrate(ctx, false)
	if err != nil {
		t.Fatal(report, err)
	}
	checkSkips(report)
	for id, hash := range want {
		if sizes[id] > 16<<20 {
			rows, err := migrationRows[struct {
				SHA string `json:"sha"`
			}](ctx, d.ch, "SELECT hex(SHA256(request_body)) AS sha FROM "+testDB+".requests PREWHERE request_id={id:String} SETTINGS max_threads=1,max_block_size=1", map[string]string{"id": id})
			if err != nil || len(rows) != 1 || !strings.EqualFold(rows[0].SHA, fmt.Sprintf("%x", hash)) {
				t.Fatal("skipped inline body changed", id, err)
			}
			continue
		}
		b, err := d.bodyReader().Read(ctx, id)
		if err != nil || sha256.Sum256(b) != hash {
			t.Fatal("body changed", id, err)
		}
	}
	if got := scalar(t, d, "SELECT count(),countIf(length(request_body)>0) FROM "+testDB+".requests SETTINGS max_threads=1,max_block_size=1"); got != "44\t2" {
		t.Fatal(got)
	}
	if got := scalar(t, d, "SELECT done FROM "+testDB+".body_migration_progress ORDER BY saved_at DESC LIMIT 1"); got != "true" && got != "1" {
		t.Fatal(got)
	}
	again, err := d.migrate(ctx, false)
	if err != nil || again.Rows != 0 || again.InlineBytes != 0 {
		t.Fatal(again, err)
	}
	checkSkips(again)
	t.Logf("44/44 byte-exact; 42 eligible (%d bytes); 2 skipped (50870022 bytes); SIGKILL/resume; second run 0 rows; dry projected=%d", expected, dry.ProjectedBytes)
}
