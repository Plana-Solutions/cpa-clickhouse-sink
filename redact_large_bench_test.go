package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/redact"
)

// BenchmarkRedactRequestSegments measures a new image or one new message after cached history.
// Run with -benchtime=1x so every sample scans a new segment.
func BenchmarkRedactRequestSegments(b *testing.B) {
	for _, size := range []int{5, 20, 34} {
		b.Run(fmt.Sprintf("image_%dMiB", size), func(b *testing.B) {
			data := make([]byte, (size<<20)*3/4)
			rand.New(rand.NewSource(42)).Read(data)
			image := base64.StdEncoding.EncodeToString(data)
			body := `{"messages":[{"role":"user","content":[{"type":"text","text":"GITHUB_TOKEN=` + patternKey + `"},{"type":"image_url","image_url":{"url":"data:image/png;base64,` + image + `"}}]}]}`
			benchmarkRedactSegments(b, "", body)
		})
	}
	for _, size := range []int{1, 5} {
		b.Run(fmt.Sprintf("cached_text_%dMiB", size), func(b *testing.B) {
			var history strings.Builder
			for i := 0; history.Len() < size<<20; i++ {
				fmt.Fprintf(&history, `{"role":"user","content":"message %d %s"},`, i, strings.Repeat("Please inspect the source and run the relevant tests. ", 100))
			}
			prefix := `{"messages":[` + history.String()
			benchmarkRedactSegments(b, prefix+`{}]}`, prefix+`{"role":"user","content":"GITHUB_TOKEN=`+patternKey+`"}]}`)
		})
	}
}

func benchmarkRedactSegments(b *testing.B, prior, body string) {
	b.Helper()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r, err := redact.New()
		if err != nil {
			b.Fatal(err)
		}
		r.Configure(nil, nil)
		if prior != "" {
			row := requestRow{RequestBody: prior}
			redactRequest(r, &row)
			if row.RedactionCount < 0 {
				b.Fatal("warmup failed closed")
			}
		}
		row := requestRow{RequestBody: body}
		b.StartTimer()
		redactRequest(r, &row)
		b.StopTimer()
		if row.RedactionCount < 1 || row.RequestBody == "" || strings.Contains(row.RequestBody, patternKey) {
			b.Fatal("redaction failed")
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			b.Fatal(err)
		}
		b.Logf("output_sha256=%x bytes=%d count=%d rules=%v", sha256.Sum256(encoded), len(body), row.RedactionCount, row.RedactionRules)
	}
}
