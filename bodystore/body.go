// Package bodystore stores exact redacted body bytes as a skeleton and content-addressed items.
package bodystore

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"github.com/tidwall/gjson"
	"strings"
)

type Hash [16]byte

type Skeleton struct {
	Residual []byte
	Offsets  []uint32
	Refs     []Hash
	Length   uint32
	SHA256   [32]byte
}

type Key struct {
	Lineage uint64
	Hash    Hash
}

var ErrIncomplete = errors.New("body incomplete")

func HashBytes(b []byte) Hash { s := sha256.Sum256(b); return Hash(s[:16]) }

// Split owns its output, so retaining an item never retains the full request.
func Split(body []byte) (Skeleton, map[Hash][]byte) {
	sk := Skeleton{Length: uint32(len(body)), SHA256: sha256.Sum256(body)}
	blobs := map[Hash][]byte{}
	prev := 0
	for _, s := range itemSpans(body) {
		sk.Residual = append(sk.Residual, body[prev:s.start]...)
		piece := body[s.start:s.end]
		h := HashBytes(piece)
		if _, ok := blobs[h]; !ok {
			blobs[h] = append([]byte(nil), piece...)
		}
		sk.Offsets = append(sk.Offsets, uint32(len(sk.Residual)))
		sk.Refs = append(sk.Refs, h)
		prev = s.end
	}
	sk.Residual = append(sk.Residual, body[prev:]...)
	return sk, blobs
}

// Join rejects malformed skeletons and missing or corrupt items before exposing bytes.
func Join(sk Skeleton, blobs map[Hash][]byte) ([]byte, error) {
	if len(sk.Offsets) != len(sk.Refs) {
		return nil, ErrIncomplete
	}
	size := uint64(len(sk.Residual))
	prev := uint32(0)
	for i, h := range sk.Refs {
		off := sk.Offsets[i]
		b, ok := blobs[h]
		if !ok || HashBytes(b) != h || off < prev || uint64(off) > uint64(len(sk.Residual)) {
			return nil, ErrIncomplete
		}
		prev = off
		size += uint64(len(b))
	}
	if size != uint64(sk.Length) {
		return nil, ErrIncomplete
	}
	out := make([]byte, 0, int(size))
	prev = 0
	for i, h := range sk.Refs {
		off := sk.Offsets[i]
		out = append(out, sk.Residual[prev:off]...)
		out = append(out, blobs[h]...)
		prev = off
	}
	out = append(out, sk.Residual[prev:]...)
	if sha256.Sum256(out) != sk.SHA256 {
		return nil, ErrIncomplete
	}
	return out, nil
}

func Lineage(headers map[string]string, body []byte) uint64 {
	for _, name := range []string{"x-claude-code-session-id", "session-id", "x-session-id", "thread-id"} {
		for k, v := range headers {
			if strings.EqualFold(k, name) && v != "" {
				return LineageKey(v)
			}
		}
	}
	key := gjson.GetBytes(body, "metadata.user_id")
	if key.Type == gjson.String && key.Str != "" {
		return LineageKey(key.Str)
	}
	key = gjson.GetBytes(body, "prompt_cache_key")
	if key.Type == gjson.String && key.Str != "" {
		return LineageKey(key.Str)
	}

	return 0
}

func LineageKey(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.LittleEndian.Uint64(h[:8])
}
