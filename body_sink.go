package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/bodystore"
)

type bodyRequest struct {
	row     requestRow
	lineage uint64
	sk      bodystore.Skeleton
	blobs   map[bodystore.Hash][]byte
}
type bodyUnit struct {
	cfg         sinkConfig
	schemaOK    bool
	requests    []bodyRequest
	blobs, rows *batch
	keys        []bodystore.Key
	size        int
}
type knownLineage struct {
	hashes map[bodystore.Hash]bool
	used   time.Time
}

func (s *sink) addRequest(row requestRow) {
	s.mu.Lock()
	enabled := s.cfg.BodyDedup
	s.mu.Unlock()
	if !enabled {
		s.add(tableRequests, row)
		return
	}
	body := []byte(row.RequestBody)
	r := bodyRequest{row: row, lineage: bodystore.Lineage(row.RequestHeaders, body)}
	r.sk, r.blobs = bodystore.Split(body)
	r.row.RequestBody = ""
	size := len(encodeBodyRequest(r))
	for _, v := range r.blobs {
		size += len(v) + 40
	}
	s.mu.Lock()
	if s.buffered+size > maxBufferedBytes {
		s.dropped++
		s.mu.Unlock()
		return
	}
	s.bodyPending = append(s.bodyPending, r)
	s.bodyPendingSize += size
	s.buffered += size
	full := s.buffered >= flushBytes
	s.mu.Unlock()
	if full {
		select {
		case s.kick <- struct{}{}:
		default:
		}
	}
}

// Only the flush goroutine owns the acknowledged-hash cache.
func (s *sink) lineage(ctx context.Context, cfg sinkConfig, l uint64) *knownLineage {
	now := time.Now()
	if !s.cacheCfg.equal(cfg) {
		s.known = nil
		s.cacheCfg = cfg
	}
	if s.known == nil {
		s.known = map[uint64]*knownLineage{}
	}
	for k, e := range s.known {
		if now.Sub(e.used) > 24*time.Hour {
			delete(s.known, k)
		}
	}
	if e := s.known[l]; e != nil {
		e.used = now
		return e
	}
	if len(s.known) >= 512 {
		var oldest uint64
		var ts time.Time
		for k, e := range s.known {
			if ts.IsZero() || e.used.Before(ts) {
				oldest = k
				ts = e.used
			}
		}
		delete(s.known, oldest)
	}
	e := &knownLineage{hashes: map[bodystore.Hash]bool{}, used: now}
	if l == 0 || ctx.Err() != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL+"/?http_write_exception_in_output_format=0", strings.NewReader(fmt.Sprintf("SELECT hex(hash) AS hash FROM %s.body_blobs WHERE lineage = %d LIMIT 100000 FORMAT JSONEachRow", cfg.Database, l)))
	if err != nil {
		return e
	}
	if cfg.Username != "" {
		req.SetBasicAuth(cfg.Username, cfg.Password)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		logf("body preload failed")
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		logf("body preload failed")
		return e
	}
	dec := json.NewDecoder(resp.Body)
	hashes := map[bodystore.Hash]bool{}
	for {
		var row struct {
			Hash string `json:"hash"`
		}
		err := dec.Decode(&row)
		if err == io.EOF {
			e.hashes = hashes
			s.known[l] = e
			s.trimKnown()
			break
		}
		if err != nil {
			logf("body preload failed")
			break
		}
		b, err := hex.DecodeString(row.Hash)
		if err != nil || len(b) != 16 {
			logf("body preload failed")
			break
		}
		hashes[bodystore.Hash(b)] = true
	}
	return e
}

func binaryBatch(table, columns string, rows [][]byte) *batch {
	b := newBatch(table, rows)
	b.format = "RowBinary"
	b.columns = columns
	return &b
}

func (s *sink) prepareBodies(ctx context.Context, cfg sinkConfig, u *bodyUnit) {
	rows := [][]byte{}
	seen := map[bodystore.Key]bool{}
	for _, r := range u.requests {
		e := s.lineage(ctx, cfg, r.lineage)
		hs := make([]bodystore.Hash, 0, len(r.blobs))
		for h := range r.blobs {
			hs = append(hs, h)
		}
		sort.Slice(hs, func(i, j int) bool { return string(hs[i][:]) < string(hs[j][:]) })
		for _, h := range hs {
			k := bodystore.Key{Lineage: r.lineage, Hash: h}
			if e.hashes[h] || seen[k] {
				s.blobsSkipped++
				continue
			}
			seen[k] = true
			u.keys = append(u.keys, k)
			rows = append(rows, encodeBlob(r.lineage, h, r.blobs[h], r.row.StartedAt))
		}
	}
	u.blobs = binaryBatch("body_blobs", "lineage,hash,data,first_seen", rows)
}

// Bad blob rows fall back to inline requests.
func (s *sink) insertBlobs(cfg sinkConfig, b batch, keys []bodystore.Key, bad map[bodystore.Key]bool) error {
	if len(b.rows) == 0 {
		return nil
	}
	err := s.insert(cfg, b)
	if err == nil {
		budget := maxKnownBytes
		for _, e := range s.known {
			budget -= 256 + 64*len(e.hashes)
		}
		for _, k := range keys {
			e := s.known[k.Lineage]
			if e == nil {
				if budget < 320 || len(s.known) >= 512 {
					continue
				}
				budget -= 256
				e = &knownLineage{hashes: map[bodystore.Hash]bool{}, used: time.Now()}
				s.known[k.Lineage] = e
			}
			if !e.hashes[k.Hash] && len(e.hashes) < 100000 && budget >= 64 {
				budget -= 64
				e.hashes[k.Hash] = true
			}
		}
		s.trimKnown()
		s.blobsWritten += len(keys)
		return nil
	}
	if !isBadData(err) {
		return err
	}
	if len(b.rows) == 1 {
		bad[keys[0]] = true
		return nil
	}
	mid := len(b.rows) / 2
	for _, rng := range [][2]int{{0, mid}, {mid, len(b.rows)}} {
		part := binaryBatch(b.table, b.columns, b.rows[rng[0]:rng[1]])
		if err := s.insertBlobs(cfg, *part, keys[rng[0]:rng[1]], bad); err != nil {
			return err
		}
	}
	return nil
}

func (s *sink) flushBodies(cfg sinkConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		s.mu.Lock()
		u := s.bodyRetry
		if u == nil && len(s.bodyPending) > 0 {
			u = &bodyUnit{requests: s.bodyPending, size: s.bodyPendingSize, cfg: cfg, schemaOK: true}
			s.bodyPending = nil
			s.bodyPendingSize = 0
			s.bodyRetry = u
		}
		s.mu.Unlock()
		if u == nil {
			return nil
		}
		if !u.cfg.equal(cfg) {
			u.cfg = cfg
			u.schemaOK = false
			u.rows, u.blobs, u.keys = nil, nil, nil
		}
		if !u.schemaOK {
			if err := s.ensureSchema(u.cfg); err != nil {
				return err
			}
			u.schemaOK = true
		}
		if u.rows == nil && !u.cfg.BodyDedup {
			rows := make([][]byte, 0, len(u.requests))
			for _, r := range u.requests {
				body, err := bodystore.Join(r.sk, r.blobs)
				if err != nil {
					return err
				}
				r.row.RequestBody = string(body)
				r.sk = bodystore.Skeleton{}
				rows = append(rows, encodeBodyRequest(r))
			}
			u.rows = bodyRows(u.requests, rows)
		}
		if u.rows == nil {
			if u.blobs == nil {
				s.prepareBodies(ctx, u.cfg, u)
			}
			bad := map[bodystore.Key]bool{}
			if err := s.insertBlobs(u.cfg, *u.blobs, u.keys, bad); err != nil {
				u.schemaOK = false
				return err
			}
			rows := make([][]byte, 0, len(u.requests))
			for _, r := range u.requests {
				for _, h := range r.sk.Refs {
					if bad[bodystore.Key{Lineage: r.lineage, Hash: h}] {
						body, err := bodystore.Join(r.sk, r.blobs)
						if err != nil {
							return err
						}
						r.row.RequestBody = string(body)
						r.sk = bodystore.Skeleton{}
						break
					}
				}
				rows = append(rows, encodeBodyRequest(r))
			}
			u.rows = bodyRows(u.requests, rows)
			u.blobs = nil
			u.keys = nil
		}
		if err := s.insertBatch(u.cfg, *u.rows); err != nil {
			u.schemaOK = false
			return err
		}
		s.release(u.size)
		s.mu.Lock()
		s.bodyRetry = nil
		s.mu.Unlock()
		logf("body blobs written=%d skipped=%d", s.blobsWritten, s.blobsSkipped)
	}
}

// Charge 64 bytes per hash, including map slack, and 256 bytes per lineage.
const maxKnownBytes = 16 << 20

func (s *sink) trimKnown() {
	size := 0
	for _, e := range s.known {
		size += 256 + 64*len(e.hashes)
	}
	for size > maxKnownBytes || len(s.known) > 512 {
		var oldest uint64
		var ts time.Time
		for k, e := range s.known {
			if ts.IsZero() || e.used.Before(ts) {
				oldest, ts = k, e.used
			}
		}
		size -= 256 + 64*len(s.known[oldest].hashes)
		delete(s.known, oldest)
	}
}
