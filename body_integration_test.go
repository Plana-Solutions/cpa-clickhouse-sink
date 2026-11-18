package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/bodystore"
)

func TestBodyIntegration(t *testing.T) {
	endpoint := os.Getenv("BODY_INTEGRATION_URL")
	if endpoint == "" {
		t.Skip("set BODY_INTEGRATION_URL for throwaway ClickHouse")
	}
	bin := os.Getenv("DERIVER_BINARY")
	if bin == "" {
		t.Fatal("DERIVER_BINARY required")
	}
	const db = "cliproxy_body_test"
	runStarted := time.Now().UTC().Format("2006-01-02 15:04:05.000000")
	query := func(ctx context.Context, q string, p map[string]string) (io.ReadCloser, error) {
		vals := url.Values{"output_format_json_quote_64bit_integers": {"0"}, "http_write_exception_in_output_format": {"0"}, "max_threads": {"1"}, "max_memory_usage": {"1073741824"}}
		for k, v := range p {
			vals.Set("param_"+k, v)
		}
		req, err := http.NewRequestWithContext(ctx, "POST", endpoint+"/?"+vals.Encode(), strings.NewReader(q))
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d executing %.100s", resp.StatusCode, q)
		}
		return resp.Body, nil
	}
	q := func(sql string) []byte {
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
		return bytes.TrimSpace(b)
	}
	q("DROP DATABASE IF EXISTS " + db + " SYNC")
	defer q("DROP DATABASE IF EXISTS " + db + " SYNC")
	s := newSink(sinkConfig{URL: endpoint, Database: db, StoreBodies: true})
	if err := s.ensureSchema(s.cfg); err != nil {
		t.Fatal(err)
	}
	s.schemaOK = true
	reader := bodystore.Reader{Database: db, Query: query}
	var rows []requestRow
	files, _ := filepath.Glob("deriver/extract/testdata/*/request.json")
	for i, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		ts := formatTime(time.Now().Add(-16 * time.Minute).Add(time.Duration(i) * time.Millisecond))
		rows = append(rows, requestRow{RequestID: fmt.Sprintf("fixture-%03d", i), TraceID: fmt.Sprintf("trace-%03d", i), StartedAt: ts, CompletedAt: ts, Model: "fixture", Path: "/v1/chat/completions", SourceFormat: "openai", StatusCode: 200, Outcome: "succeeded", RequestHeaders: map[string]string{"session-id": "integration"}, RequestBody: string(body)})
	}
	if path := os.Getenv("BODY_LIVE_FIXTURES"); path != "" {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		for {
			var row struct {
				requestRow
				Body     []byte   `json:"request_body_b64"`
				Response []byte   `json:"response_body_b64"`
				Chunks   [][]byte `json:"response_chunks_b64"`
			}
			err := dec.Decode(&row)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			row.RequestBody = string(row.Body)
			row.ResponseBody = string(row.Response)
			for _, c := range row.Chunks {
				row.ResponseChunks = append(row.ResponseChunks, string(c))
			}
			rows = append(rows, row.requestRow)
		}
	}
	// Include a byte string JSON cannot transport losslessly.
	ts := formatTime(time.Now().Add(-15 * time.Minute))
	rows = append(rows, requestRow{RequestID: "binary", StartedAt: ts, CompletedAt: ts, Model: "fixture", RequestBody: string([]byte{255, 0, 128, '{', '"'})})
	run := func(args ...string) []byte {
		t.Helper()
		if len(args) > 0 && (args[0] == "migrate-bodies" || args[0] == "restore-bodies") {
			q("SYSTEM JEMALLOC PURGE")
			ready := false
			for attempt := 0; attempt < 20; attempt++ {
				if string(q("SELECT (SELECT value FROM system.asynchronous_metrics WHERE metric='MemoryResident') < 0.60*(SELECT toFloat64(value) FROM system.server_settings WHERE name='max_server_memory_usage')")) == "1" {
					ready = true
					break
				}
				time.Sleep(250 * time.Millisecond)
			}
			if !ready {
				t.Fatal("migration test memory precondition not met after purge")
			}
		}
		cmd := exec.Command(bin, args...)
		cmd.Env = append(os.Environ(), "CLICKHOUSE_URL="+endpoint, "CLICKHOUSE_DB="+db, "CLICKHOUSE_USER=", "CLICKHOUSE_PASSWORD=", "CLICKHOUSE_PASSWORD_FILE=")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("deriver %v: %v %s", args, err, out)
		}
		return out
	}
	factsMatched := len(rows) - 1
	// Legacy baseline excludes invalid UTF-8, which JSONEachRow cannot encode.
	for _, r := range rows[:len(rows)-1] {
		s.addRequest(r)
	}
	s.flushNow()
	q("ALTER TABLE " + db + ".requests UPDATE ingested_at=toDateTime64('2026-01-01 00:00:00',9,'UTC') WHERE 1 SETTINGS mutations_sync=1")
	run("--rebuild")
	q("OPTIMIZE TABLE " + db + ".requests FINAL")
	t.Logf("legacy_fixture_bytes %s", q("SELECT sum(data_compressed_bytes),sum(bytes_on_disk) FROM system.parts WHERE active AND database='"+db+"' AND table='requests'"))
	baseline := q("SELECT * EXCEPT(ingested_at) FROM " + db + ".request_facts ORDER BY request_id FORMAT JSONEachRow")
	q("TRUNCATE TABLE " + db + ".requests")
	q("TRUNCATE TABLE " + db + ".request_facts")
	q("TRUNCATE TABLE " + db + ".deriver_progress")
	s.cfg.BodyDedup = true
	for _, r := range rows {
		s.addRequest(r)
	}
	s.flushNow()
	for _, table := range []string{"requests", "body_blobs"} {
		q("OPTIMIZE TABLE " + db + "." + table + " FINAL")
	}
	t.Logf("dedup_fixture_bytes %s", q("SELECT table,sum(data_compressed_bytes),sum(bytes_on_disk) FROM system.parts WHERE active AND database='"+db+"' AND table IN ('requests','body_blobs') GROUP BY table ORDER BY table"))
	for _, r := range rows {
		got, err := reader.Read(context.Background(), r.RequestID)
		if err != nil || !bytes.Equal(got, []byte(r.RequestBody)) {
			t.Fatalf("reader roundtrip %s: %v", r.RequestID, err)
		}
		rc, err := query(context.Background(), "SELECT base64Encode(request_body) AS body,body_status FROM "+db+".raw_request(id={id:String}) FORMAT JSONEachRow", map[string]string{"id": r.RequestID})
		if err != nil {
			t.Fatal(err)
		}
		var result struct {
			Body   []byte `json:"body"`
			Status string `json:"body_status"`
		}
		err = json.NewDecoder(rc).Decode(&result)
		rc.Close()
		if err != nil || result.Status != "complete" || !bytes.Equal(result.Body, []byte(r.RequestBody)) {
			t.Fatalf("SQL roundtrip %s status=%s err=%v", r.RequestID, result.Status, err)
		}
	}
	q("ALTER TABLE " + db + ".requests UPDATE ingested_at=toDateTime64('2026-01-01 00:00:00',9,'UTC') WHERE 1 SETTINGS mutations_sync=1")
	run("--rebuild")
	actual := q("SELECT * EXCEPT(ingested_at) FROM " + db + ".request_facts WHERE request_id!='binary' ORDER BY request_id FORMAT JSONEachRow")
	if !bytes.Equal(actual, baseline) {
		bm := map[string]map[string]any{}
		am := map[string]map[string]any{}
		for _, pair := range []struct {
			data []byte
			m    map[string]map[string]any
		}{{baseline, bm}, {actual, am}} {
			for _, line := range bytes.Split(pair.data, []byte{'\n'}) {
				var row map[string]any
				_ = json.Unmarshal(line, &row)
				pair.m[fmt.Sprint(row["request_id"])] = row
			}
		}
		for id, b := range bm {
			a := am[id]
			for k, v := range b {
				x, _ := json.Marshal(v)
				y, _ := json.Marshal(a[k])
				if !bytes.Equal(x, y) {
					t.Logf("fact difference id=%s column=%s", id, k)
				}
			}
		}

		t.Fatalf("facts differ legacy digest=%x dedup digest=%x", sha256.Sum256(baseline), sha256.Sum256(actual))
	}
	// A mixed table and an interrupted migration must retain one preferred body per request.
	s.cfg.BodyDedup = false
	for _, r := range rows[:len(rows)-1] {
		s.addRequest(r)
	}
	s.flushNow()
	extra := rows[0]
	extra.RequestID = "migration-only"
	s.addRequest(extra)
	s.flushNow()
	rows = append(rows, extra)
	run("--rebuild")
	before := q("SELECT count() FROM " + db + ".requests")
	dry := run("migrate-bodies", "--dry-run")
	if !bytes.Equal(before, q("SELECT count() FROM "+db+".requests")) {
		t.Fatal("dry-run changed rows")
	}
	t.Logf("dry-run %s", dry)
	t.Logf("migration %s", run("migrate-bodies"))
	t.Logf("migration rerun %s", run("migrate-bodies"))
	if got := string(q("SELECT count() FROM " + db + ".requests")); got != fmt.Sprint(len(rows)) {
		t.Fatalf("migration rows %s want %d", got, len(rows))
	}
	for _, r := range rows {
		b, err := reader.Read(context.Background(), r.RequestID)
		if err != nil || !bytes.Equal(b, []byte(r.RequestBody)) {
			t.Fatal("migrated body", r.RequestID, err)
		}
	}
	t.Logf("restore dry-run %s", run("restore-bodies", "--dry-run"))
	t.Logf("restore %s", run("restore-bodies"))
	t.Logf("restore rerun %s", run("restore-bodies"))
	if got := string(q("SELECT countIf(body_sha256!=repeat(char(0),32)),count() FROM " + db + ".requests")); got != fmt.Sprintf("0\t%d", len(rows)) {
		t.Fatal("restore rows", got)
	}
	for _, r := range rows {
		b, err := reader.Read(context.Background(), r.RequestID, r.StartedAt)
		if err != nil || !bytes.Equal(b, []byte(r.RequestBody)) {
			t.Fatal("restored body", r.RequestID, err)
		}
	}
	t.Logf("second migration %s", run("migrate-bodies"))
	for _, r := range rows {
		b, err := reader.Read(context.Background(), r.RequestID, r.StartedAt)
		if err != nil || !bytes.Equal(b, []byte(r.RequestBody)) {
			t.Fatal("second migration body", r.RequestID, err)
		}
	}
	// Restore corruption checks after the backlog run so every prior comparison uses valid data.
	const backlog = 3200
	for i := 0; i < backlog; i++ {
		r := rows[0]
		r.RequestID = fmt.Sprintf("backlog-%05d", i)
		r.StartedAt = formatTime(time.Now().Add(-15 * time.Minute))
		r.CompletedAt = r.StartedAt
		r.RequestBody = `{"messages":[{"role":"user","content":"` + strings.Repeat("repeated history ", 4096) + `"}]}`
		s.cfg.BodyDedup = true
		s.addRequest(r)
		if i%100 == 99 {
			s.flushNow()
		}
	}
	s.flushNow()
	q("TRUNCATE TABLE " + db + ".request_facts")
	q("TRUNCATE TABLE " + db + ".deriver_progress")
	backlogStarted := time.Now().UTC().Format("2006-01-02 15:04:05.000000")
	q("ALTER TABLE " + db + ".requests UPDATE ingested_at=now64(9)-INTERVAL 15 MINUTE WHERE 1 SETTINGS mutations_sync=1")
	loopCtx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(loopCtx, bin, "--interval", "100ms")
	cmd.Env = append(os.Environ(), "CLICKHOUSE_URL="+endpoint, "CLICKHOUSE_DB="+db, "CLICKHOUSE_USER=", "CLICKHOUSE_PASSWORD=", "CLICKHOUSE_PASSWORD_FILE=")
	var loopLog bytes.Buffer
	cmd.Stdout = &loopLog
	cmd.Stderr = &loopLog
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		if string(q("SELECT count() FROM "+db+".request_facts")) == fmt.Sprint(len(rows)+backlog) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	cancel()
	_ = cmd.Wait()
	if got := string(q("SELECT count(),uniqExact(request_id) FROM " + db + ".request_facts")); got != fmt.Sprintf("%d\t%d", len(rows)+backlog, len(rows)+backlog) {
		t.Fatal("backlog", got)
	}
	for _, table := range []string{"requests", "body_blobs"} {
		q("OPTIMIZE TABLE " + db + "." + table + " FINAL")
	}
	q("SYSTEM FLUSH LOGS")
	t.Logf("reconstructed=%d facts_equal=%d backlog=%d", len(rows), factsMatched, backlog)
	t.Logf("sizes %s", q("SELECT table,sum(rows),sum(data_compressed_bytes),sum(bytes_on_disk) FROM system.parts WHERE active AND database='"+db+"' GROUP BY table ORDER BY table FORMAT TSV"))
	for _, measurement := range []struct{ name, since string }{{"peak_query_memory", runStarted}, {"peak_backlog_query_memory", backlogStarted}} {
		t.Logf("%s %s", measurement.name, q("SELECT max(memory_usage) FROM system.query_log WHERE type='QueryFinish' AND query LIKE '%"+db+"%' AND query NOT LIKE '%system.query_log%' AND event_time_microseconds>=toDateTime64('"+measurement.since+"',6,'UTC')"))
	}
	q("TRUNCATE TABLE " + db + ".body_blobs")
	if b, err := reader.Read(context.Background(), "backlog-00000"); err == nil || b != nil {
		t.Fatal("missing blob accepted")
	}
	if got := string(q("SELECT body_status FROM " + db + ".raw_request(id='backlog-00000')")); got != "incomplete" {
		t.Fatal("SQL missing blob", got)
	}
}
