package bodystore

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Query returns a JSONEachRow stream. Values are passed as ClickHouse parameters.
type Query func(context.Context, string, map[string]string) (io.ReadCloser, error)

type Reader struct {
	Database string
	Query    Query
}

// Body is a binary-safe JSON projection of the body columns.
type Body struct {
	Inline   string   `json:"body_inline"`
	Lineage  uint64   `json:"lineage"`
	Length   uint32   `json:"body_len"`
	SHA      string   `json:"body_sha"`
	Residual []byte   `json:"body_residual_b64"`
	Offsets  []uint32 `json:"body_ref_offsets"`
	Refs     []string `json:"body_hashes"`
}

const Projection = `base64Encode(request_body) AS body_inline, lineage, body_len, hex(body_sha256) AS body_sha,
 base64Encode(body_residual) AS body_residual_b64, body_ref_offsets, arrayMap(x -> hex(x), body_refs) AS body_hashes`

// Read uses the primary-key time when supplied. Otherwise it resolves that metadata first.
func (r Reader) Read(ctx context.Context, id string, startedAt ...string) ([]byte, error) {
	params := map[string]string{"id": id}
	if len(startedAt) > 0 {
		params["ts"] = startedAt[0]
	} else {
		rc, err := r.Query(ctx, "SELECT started_at FROM "+r.Database+`.requests WHERE request_id={id:String} LIMIT 1 FORMAT JSONEachRow`, params)
		if err != nil {
			return nil, err
		}
		var row struct {
			TS string `json:"started_at"`
		}
		err = json.NewDecoder(rc).Decode(&row)
		rc.Close()
		if err != nil {
			return nil, err
		}
		params["ts"] = row.TS
	}
	predicate := `request_id={id:String} AND started_at={ts:DateTime64(9,'UTC')}`
	rc, err := r.Query(ctx, "SELECT "+Projection+" FROM "+r.Database+`.requests PREWHERE `+predicate+` ORDER BY request_body = '' DESC LIMIT 1 FORMAT JSONEachRow`, params)
	if err != nil {
		return nil, err
	}
	var row Body
	err = json.NewDecoder(rc).Decode(&row)
	rc.Close()
	if err != nil {
		return nil, err
	}
	if len(row.Refs) == 0 {
		return row.Restore(nil)
	}
	blobs, err := r.load(ctx, predicate, params)
	if err != nil {
		return nil, err
	}
	return row.Restore(blobs)
}

// Load fetches only the distinct items referenced by this bounded batch.
func (r Reader) Load(ctx context.Context, ids []string) (map[Key][]byte, error) {
	vals := make([]string, len(ids))
	for i, id := range ids {
		vals[i] = "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(id) + "'"
	}
	return r.load(ctx, `request_id IN {ids:Array(String)}`, map[string]string{"ids": "[" + strings.Join(vals, ",") + "]"})
}

func (r Reader) load(ctx context.Context, predicate string, params map[string]string) (map[Key][]byte, error) {
	rc, err := r.Query(ctx, `SELECT lineage, hex(hash) AS hash_hex, base64Encode(data) AS data FROM `+r.Database+`.body_blobs
 WHERE (lineage,hash) IN (SELECT lineage,arrayJoin(body_refs) FROM `+r.Database+`.requests PREWHERE `+predicate+`) LIMIT 1 BY lineage,hash SETTINGS max_threads=1 FORMAT JSONEachRow`, params)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	blobs := map[Key][]byte{}
	dec := json.NewDecoder(rc)
	for {
		var row struct {
			Lineage uint64 `json:"lineage"`
			Hash    string `json:"hash_hex"`
			Data    []byte `json:"data"`
		}
		if err := dec.Decode(&row); err == io.EOF {
			return blobs, nil
		} else if err != nil {
			return nil, err
		}
		h, err := parseHash(row.Hash)
		if err != nil {
			return nil, err
		}
		blobs[Key{row.Lineage, h}] = row.Data
	}
}

func parseHash(s string) (Hash, error) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 16 {
		return Hash{}, ErrIncomplete
	}
	return Hash(b), nil
}

func (row Body) Restore(blobs map[Key][]byte) ([]byte, error) {
	sha, err := hex.DecodeString(row.SHA)
	if err != nil || len(sha) != 32 {
		return nil, ErrIncomplete
	}
	if [32]byte(sha) == [32]byte{} && (row.Length != 0 || len(row.Residual) != 0 || len(row.Offsets) != 0 || len(row.Refs) != 0) {
		return nil, ErrIncomplete
	}
	sk := Skeleton{Length: row.Length, SHA256: [32]byte(sha), Residual: row.Residual, Offsets: row.Offsets}
	// Legacy rows have the default zero digest, including legacy empty bodies.
	if row.Inline != "" || sk.SHA256 == [32]byte{} {
		b, err := base64.StdEncoding.DecodeString(row.Inline)
		if err != nil {
			return nil, ErrIncomplete
		}
		if sk.SHA256 != [32]byte{} {
			sk.Residual = b
			if uint64(len(b)) != uint64(row.Length) {
				return nil, ErrIncomplete
			}
			sk.Offsets = nil
			return Join(sk, nil)
		}
		return b, nil
	}
	items := map[Hash][]byte{}
	for _, ref := range row.Refs {
		h, err := parseHash(ref)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid reference", ErrIncomplete)
		}
		sk.Refs = append(sk.Refs, h)
		items[h] = blobs[Key{row.Lineage, h}]
	}
	return Join(sk, items)
}
