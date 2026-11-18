package main

import (
	"fmt"
	"github.com/Plana-Solutions/cpa-clickhouse-sink/bodystore"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBodyUnits(t *testing.T) {
	for _, failure := range []string{"blobs", "requests", "badblob", "none"} {
		t.Run(failure, func(t *testing.T) {
			var calls []string
			failed := false
			preloads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("query") == "" {
					b, _ := io.ReadAll(r.Body)
					if strings.HasPrefix(string(b), "SELECT hex(hash)") {
						preloads++
					}
					return
				}
				q := r.URL.Query().Get("query")
				kind := "requests"
				if strings.Contains(q, "body_blobs") {
					kind = "blobs"
				}
				calls = append(calls, kind)
				if !failed && ((failure == "blobs" && kind == "blobs") || (failure == "requests" && kind == "requests") || (failure == "badblob" && kind == "blobs")) {
					failed = true
					status := 503
					if failure == "badblob" {
						status = 400
					}
					w.WriteHeader(status)
				}
			}))
			defer server.Close()
			s := newSink(sinkConfig{URL: server.URL, Database: "test", BodyDedup: true})
			s.schemaOK = true
			row := requestRow{RequestID: "r", StartedAt: formatTime(time.Now()), CompletedAt: formatTime(time.Now()), RequestHeaders: map[string]string{"session-id": "s"}, RequestBody: `{"messages":["same"]}`}
			s.addRequest(row)
			s.flushNow()
			if failure == "blobs" && strings.Join(calls, ",") != "blobs" {
				t.Fatal(calls)
			}
			if failure == "requests" {
				s.addRequest(row)
			}
			s.flushNow()
			if s.buffered != 0 || s.bodyRetry != nil {
				t.Fatalf("buffered=%d retry=%v", s.buffered, s.bodyRetry != nil)
			}
			got := strings.Join(calls, ",")
			want := map[string]string{"blobs": "blobs,blobs,requests", "requests": "blobs,requests,requests,requests", "badblob": "blobs,requests", "none": "blobs,requests"}[failure]
			if got != want {
				t.Fatalf("got %s want %s", got, want)
			}
			if preloads != 1 {
				t.Fatalf("preloads %d", preloads)
			}
		})
	}
}
func TestBodyDedupDefaultOff(t *testing.T) {
	c, err := parseConfig(nil)
	if err != nil || c.BodyDedup {
		t.Fatal(c, err)
	}
	c, err = parseConfig([]byte("body-dedup: true"))
	if err != nil || !c.BodyDedup {
		t.Fatal(c, err)
	}
}

func TestDedupRedactsBeforeHashing(t *testing.T) {
	dir := t.TempDir()
	secret := "dedup-test-secret-0123456789"
	file := dir + "/secrets.txt"
	writeFile(t, file, secret+"\n")
	s := newSink(sinkConfig{BodyDedup: true})
	p := &plugin{sink: s, capture: newCapture(), redact: mustRedactor(t, file)}
	call(t, p, "request.intercept_before", map[string]any{"RequestID": "redaction", "Body": []byte(`{"messages":[{"role":"user","content":"` + secret + `"}]}`)})
	call(t, p, "request.complete", completion{RequestID: "redaction", Model: "m", StartedAt: time.Now(), CompletedAt: time.Now()})
	if len(s.bodyPending) != 1 {
		t.Fatal("request missing")
	}
	r := s.bodyPending[0]
	b, err := bodystore.Join(r.sk, r.blobs)
	if err != nil || strings.Contains(string(b), secret) {
		t.Fatal("body was not redacted")
	}
	for _, v := range r.blobs {
		if strings.Contains(string(v), secret) {
			t.Fatal("unredacted blob")
		}
	}
}

func TestPreloadAfterRestart(t *testing.T) {
	h := bodystore.HashBytes([]byte(`"same"`))
	inserts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Query().Get("query"), "body_blobs") {
			t.Error("resent committed blob")
		}
		if r.URL.Query().Get("query") == "" {
			fmt.Fprintf(w, "{\"hash\":\"%X\"}\n", h)
			return
		}
		inserts++
	}))
	defer server.Close()
	s := newSink(sinkConfig{URL: server.URL, Database: "test", BodyDedup: true})
	s.schemaOK = true
	s.addRequest(requestRow{RequestHeaders: map[string]string{"session-id": "restart"}, RequestBody: `{"messages":["same"]}`})
	s.flushNow()
	if inserts != 1 {
		t.Fatal(inserts)
	}
}

func TestSchemaCopiesMatch(t *testing.T) {
	b, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(strings.TrimSpace(string(b)), strings.TrimSpace(bodystore.Schema)) {
		t.Fatal("raw and embedded body schemas differ")
	}
}

func TestBodyRetryKeepsDestination(t *testing.T) {
	var a, b []string
	failed := false
	server := func(calls *[]string, fail bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query().Get("query")
			if q == "" {
				return
			}
			kind := "requests"
			if strings.Contains(q, "body_blobs") {
				kind = "blobs"
			}
			*calls = append(*calls, kind)
			if fail && kind == "requests" && !failed {
				failed = true
				w.WriteHeader(503)
			}
		}))
	}
	first := server(&a, true)
	defer first.Close()
	second := server(&b, false)
	defer second.Close()
	cfg := sinkConfig{URL: first.URL, Database: "test", BodyDedup: true}
	s := newSink(cfg)
	s.schemaOK = true
	row := requestRow{RequestHeaders: map[string]string{"session-id": "same"}, RequestBody: `{"messages":["same"]}`}
	s.addRequest(row)
	s.flushNow()
	if s.schemaOK {
		t.Fatal("failed body insert retained schema state")
	}
	cfg.URL = second.URL
	s.reconfigure(cfg)
	s.addRequest(row)
	s.flushNow()
	if strings.Join(a, ",") != "blobs,requests,requests" || strings.Join(b, ",") != "blobs,requests" {
		t.Fatal(a, b)
	}
	if s.buffered != 0 {
		t.Fatal("buffer not released")
	}
}
