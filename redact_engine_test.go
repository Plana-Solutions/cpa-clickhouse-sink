package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/redact"
)

func TestRedactImageDataURLPattern(t *testing.T) {
	// This synthetic key uses only base64 characters and has a valid base64 length.
	key := "AKIAJQPLDZ6FNV3M2R7X"
	if _, err := base64.StdEncoding.DecodeString(key); err != nil {
		t.Fatal("invalid synthetic base64")
	}
	body := `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,` + key + `"}}]}]}`
	r := mustRedactor(t)
	row := requestRow{RequestBody: body}
	redactRequest(r, &row)
	if strings.Contains(row.RequestBody, key) || !strings.Contains(row.RequestBody, redact.Marker("aws-access-token", key)) {
		t.Fatal("image data URL pattern secret was not redacted")
	}
}

func TestRedactionEngineCorpus(t *testing.T) {
	authDir, env := writeSecretFiles(t)
	r := mustRedactor(t, authDir, env)
	rows := []requestRow{
		{RequestBody: `{"input":"token ` + knownToken + `"}`, Error: "GITHUB_TOKEN=" + patternKey, RequestHeaders: map[string]string{"x-token": envSecret}},
		{RequestBody: `{"prompt_cache_key":"0123456789abcdef0123456789abcdef","input":[{"role":"user","content":"ordinary text"}]}`, ResponseChunks: []string{knownToken[:20], knownToken[20:]}},
		{RequestBody: `{"input":"` + knownToken[:20] + `"}`, ResponseBody: "safe response"},
	}
	for i := range rows {
		redactRequest(r, &rows[i])
		if rows[i].RedactionCount < 1 {
			t.Fatal("corpus did not exercise redaction")
		}
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	if digest != "d29280b128575e3177e4c322fd629d0986cbe727b2be8ae168b3337aed08ead5" {
		t.Fatalf("redaction corpus digest changed: %s", digest)
	}
	t.Logf("output_sha256=%s", digest)
}
