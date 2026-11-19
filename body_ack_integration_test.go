package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

type bodyAckTransport func(*http.Request) (*http.Response, error)

func (f bodyAckTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestBodyGuardIntegration(t *testing.T) {
	endpoint := os.Getenv("BODY_INTEGRATION_URL")
	if endpoint == "" {
		t.Skip("set BODY_INTEGRATION_URL for throwaway ClickHouse")
	}
	const db = "cliproxy_body_guard_test"
	q := func(sql string) string {
		t.Helper()
		r, err := http.Post(endpoint+"/?http_write_exception_in_output_format=0", "text/plain", strings.NewReader(sql))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		if err != nil || r.StatusCode != 200 {
			t.Fatalf("query failed status=%d err=%v", r.StatusCode, err)
		}
		return strings.TrimSpace(string(data))
	}
	q("DROP DATABASE IF EXISTS " + db + " SYNC")
	defer q("DROP DATABASE IF EXISTS " + db + " SYNC")
	q("DROP USER IF EXISTS body_guard_writer")
	q("CREATE USER body_guard_writer IDENTIFIED WITH no_password")
	q("GRANT ALL ON " + db + ".* TO body_guard_writer")
	defer q("DROP USER body_guard_writer")
	s := newSink(sinkConfig{URL: endpoint, Database: db, Username: "body_guard_writer", BodyDedup: true})
	if err := s.ensureSchema(s.cfg); err != nil {
		t.Fatal(err)
	}
	s.schemaOK = true
	lost := false
	s.client.Transport = bodyAckTransport(func(r *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(r)
		if err == nil && !lost && strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO "+db+".requests ") {
			if _, err := io.Copy(io.Discard, response.Body); err != nil {
				return nil, err
			}
			response.Body.Close()
			lost = true
			return nil, io.ErrUnexpectedEOF
		}
		return response, err
	})
	ts := formatTime(time.Now())
	row := requestRow{RequestID: "lost-ack", StartedAt: ts, CompletedAt: ts, RequestBody: `{"messages":["exact body"]}`}
	s.addRequest(row)
	s.flushNow()
	if !lost || s.bodyRetry == nil || q("SELECT count() FROM "+db+".requests") != "1" {
		t.Fatal("failed to lose a committed acknowledgement")
	}
	s.flushNow()
	if s.bodyRetry != nil || s.buffered != 0 || q("SELECT count() FROM "+db+".requests") != "1" {
		t.Fatal("retry duplicated or lost a request")
	}
	q("DELETE FROM " + db + ".requests WHERE 1 SETTINGS lightweight_deletes_sync=1")
	s.addRequest(row)
	s.flushNow()
	if got := q("SELECT count() FROM " + db + ".requests"); got != "1" {
		t.Fatal("repair re-insert rejected", got)
	}
	t.Log(fmt.Sprintf("lost acknowledgement retried once without duplication; deleted request re-inserted; rows=%s", q("SELECT count() FROM "+db+".requests")))
}
