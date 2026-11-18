package main

import (
	"io"
	"net/http"
	"net/http/httptest"
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
