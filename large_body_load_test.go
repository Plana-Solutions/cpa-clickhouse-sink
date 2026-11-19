package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestLargeBodyLiveLoad(t *testing.T) {
	endpoint := strings.TrimRight(os.Getenv("LARGE_BODY_URL"), "/")
	if endpoint == "" {
		t.Skip("set LARGE_BODY_URL to the isolated large corpus")
	}
	seconds, _ := strconv.Atoi(os.Getenv("LARGE_BODY_SECONDS"))
	if seconds < 1 {
		seconds = 600
	}
	response, err := http.Post(endpoint, "text/plain", strings.NewReader("SELECT request_body FROM cliproxy.requests PREWHERE request_id=hex(SHA256('seed-3000')) SETTINGS max_threads=1,max_block_size=1 FORMAT JSONEachRow"))
	if err != nil {
		t.Fatal(err)
	}
	var seed struct {
		Body string `json:"request_body"`
	}
	err = json.NewDecoder(response.Body).Decode(&seed)
	response.Body.Close()
	if err != nil || len(seed.Body) < 500000 {
		t.Fatal("seed", err)
	}
	var failures, code241 atomic.Int64
	s := newSink(sinkConfig{URL: endpoint, Database: "cliproxy", Username: "database_only", BodyDedup: true})
	s.client.Transport = bodyAckTransport(func(r *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			failures.Add(1)
			return resp, err
		}
		if resp.StatusCode != 200 {
			failures.Add(1)
			if resp.Header.Get("X-ClickHouse-Exception-Code") == "241" {
				code241.Add(1)
			}
		}
		return resp, nil
	})
	s.start()
	count := 0
	start := time.Now()
	for time.Since(start) < time.Duration(seconds)*time.Second {
		ts := formatTime(time.Now())
		body := strings.TrimSuffix(seed.Body, "]}") + fmt.Sprintf(", {\"role\":\"user\",\"content\":\"live turn %d\"}]}", count)
		s.addRequest(requestRow{RequestID: fmt.Sprintf("live-%d-%d", start.Unix(), count), StartedAt: ts, CompletedAt: ts, SourceFormat: "claude", Model: "synthetic", Path: "/v1/messages", Outcome: "succeeded", StatusCode: 200, RequestHeaders: map[string]string{"session-id": "live-test"}, RequestBody: body})
		count++
		time.Sleep(time.Second)
	}
	s.stop()
	t.Logf("live rows=%d insert_errors=%d code241=%d buffered=%d dropped=%d seconds=%.1f", count, failures.Load(), code241.Load(), s.buffered, s.dropped, time.Since(start).Seconds())
	if failures.Load() != 0 || s.buffered != 0 || s.dropped != 0 {
		t.Fatal("live capture failed")
	}
}
