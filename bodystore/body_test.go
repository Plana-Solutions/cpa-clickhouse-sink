package bodystore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestProtocols(t *testing.T) {
	files, _ := filepath.Glob("../deriver/extract/testdata/*/request.json")
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			b, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			check(t, b)
		})
	}
	for _, b := range [][]byte{nil, []byte(`{"`), []byte(`{"messages":[{"role":"user","content":"x"}],"tools":[{}],"system": ["x"], "instructions":"abc"}`), {0, 255, 128}} {
		check(t, b)
	}
}
func check(t testing.TB, b []byte) {
	t.Helper()
	sk, items := Split(b)
	got, err := Join(sk, items)
	if err != nil || !bytes.Equal(got, b) {
		t.Fatalf("roundtrip length=%d: %v", len(b), err)
	}
	if sha256.Sum256(got) != sk.SHA256 {
		t.Fatal("digest")
	}
}
func FuzzRoundTrip(f *testing.F) {
	for _, s := range []string{"", `{"`, `{"messages":[{},{}]}`, `{"input":["test"],"systemInstruction":{"parts":[]}}`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) { check(t, b) })
}
func TestCorruption(t *testing.T) {
	sk, items := Split([]byte(`{"messages":["a"]}`))
	delete(items, sk.Refs[0])
	if b, err := Join(sk, items); err == nil || b != nil {
		t.Fatal("missing accepted")
	}
	items[sk.Refs[0]] = []byte(`"b"`)
	if _, err := Join(sk, items); err == nil {
		t.Fatal("corruption accepted")
	}
	sk.Offsets[0] = 999
	if _, err := Join(sk, items); err == nil {
		t.Fatal("offset accepted")
	}
}
func TestLineage(t *testing.T) {
	if Lineage(map[string]string{"SESSION-ID": "session"}, []byte(`{"metadata":{"user_id":"other"}}`)) != LineageKey("session") {
		t.Fatal("priority")
	}
	if Lineage(nil, []byte(`{"metadata":{"user_id":"u"}}`)) != LineageKey("u") {
		t.Fatal("body")
	}
	if Lineage(nil, nil) != 0 {
		t.Fatal("missing")
	}
	h := HashBytes([]byte("abc"))
	if hex.EncodeToString(h[:]) != "ba7816bf8f01cfea414140de5dae2223" {
		t.Fatal("hash")
	}
}
func BenchmarkSplit(b *testing.B) {
	body := []byte(`{"messages":[{"role":"user","content":"hello"}]}`)
	for b.Loop() {
		Split(body)
	}
}

func TestReaderRejectsCorruptSkeleton(t *testing.T) {
	sk, items := Split([]byte(`{"input":["same"]}`))
	refs := make([]string, len(sk.Refs))
	blobs := map[Key][]byte{}
	for i, h := range sk.Refs {
		refs[i] = hex.EncodeToString(h[:])
		blobs[Key{42, h}] = items[h]
	}
	row := Body{Lineage: 42, Length: sk.Length, SHA: hex.EncodeToString(sk.SHA256[:]), Residual: sk.Residual, Offsets: sk.Offsets, Refs: refs}
	if _, err := row.Restore(blobs); err != nil {
		t.Fatal(err)
	}
	row.SHA = hex.EncodeToString(make([]byte, 32))
	if _, err := row.Restore(blobs); err == nil {
		t.Fatal("zeroed digest accepted")
	}
	row.SHA = hex.EncodeToString(sk.SHA256[:])
	for k := range blobs {
		blobs[k] = []byte("corrupt")
	}
	if _, err := row.Restore(blobs); err == nil {
		t.Fatal("corrupt item accepted")
	}
}
