package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestClickHousePassword(t *testing.T) {
	t.Setenv("CLICKHOUSE_PASSWORD", "environment")
	t.Setenv("CLICKHOUSE_PASSWORD_FILE", "")
	if got, err := clickHousePassword(); err != nil || got != "environment" {
		t.Fatal("environment fallback failed")
	}
	path := filepath.Join(t.TempDir(), "password")
	t.Setenv("CLICKHOUSE_PASSWORD_FILE", path)
	if got, err := clickHousePassword(); err == nil || got != "" {
		t.Fatal("unreadable file must fail closed")
	}
	for _, content := range []string{"file-value\n", "file-value\r\n", "file-value", "", " keep spaces \n"} {
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		want := "file-value"
		if content == "" {
			want = ""
		}
		if content == " keep spaces \n" {
			want = " keep spaces "
		}
		if got, err := clickHousePassword(); err != nil || got != want {
			t.Fatal("file precedence or newline handling failed")
		}
	}
}
