package bodystore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestReadBatchValidatesIDsAndStreamErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ids       []string
		wantError bool
	}{
		{"complete", []string{"a", "b"}, false}, {"missing", []string{"a"}, true}, {"duplicate", []string{"a", "a"}, true}, {"unexpected", []string{"a", "c"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := Reader{Database: "cliproxy", Query: func(ctx context.Context, q string, p map[string]string) (io.ReadCloser, error) {
				if strings.Contains(q, "FROM cliproxy.body_blobs") {
					return io.NopCloser(strings.NewReader("")), nil
				}
				var data strings.Builder
				for _, id := range tc.ids {
					fmt.Fprintf(&data, `{"request_id":%q,"started_at":"2026-01-01 00:00:00.000000000","body_inline":"e30=","body_sha":%q}`+"\n", id, strings.Repeat("0", 64))
				}
				return io.NopCloser(strings.NewReader(data.String())), nil
			}}
			err := reader.ReadBatch(context.Background(), []string{"a", "b"}, func(id, ts string, b []byte) error {
				if string(b) != "{}" {
					t.Fatal(string(b))
				}
				return nil
			})
			if (err != nil) != tc.wantError {
				t.Fatal(err)
			}
		})
	}
	for _, raw := range []string{`{"exception":"Code: 241. synthetic"}`, "Code: 241. synthetic"} {
		var row Body
		err := DecodeRow(json.NewDecoder(strings.NewReader(raw)), &row)
		var stream *StreamError
		if !errors.As(err, &stream) || stream.Code != 241 || errors.Is(err, ErrIncomplete) {
			t.Fatalf("server error misclassified: %v", err)
		}
	}
}

func TestSkeletonBatchDoesNotUseInline(t *testing.T) {
	reader := Reader{Database: "cliproxy", Query: func(ctx context.Context, q string, p map[string]string) (io.ReadCloser, error) {
		if strings.Contains(q, "FROM cliproxy.body_blobs") {
			return io.NopCloser(strings.NewReader("")), nil
		}
		data := fmt.Sprintf(`{"request_id":"a","body_inline":"e30=","body_len":2,"body_sha":"%x","body_hashes":["%s"],"body_ref_offsets":[0]}`, sha256.Sum256([]byte("{}")), strings.Repeat("12", 16))
		return io.NopCloser(strings.NewReader(data)), nil
	}}
	fn := func(id, ts string, b []byte) error { return nil }
	if err := reader.ReadBatch(context.Background(), []string{"a"}, fn); err != nil {
		t.Fatal(err)
	}
	if err := reader.ReadSkeletonBatch(context.Background(), []string{"a"}, fn); !errors.Is(err, ErrIncomplete) {
		t.Fatal("skeleton verification used inline bytes", err)
	}
}
