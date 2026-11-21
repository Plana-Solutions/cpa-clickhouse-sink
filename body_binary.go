package main

import (
	"bytes"
	"encoding/binary"
	"sort"
	"time"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/bodystore"
)

type binaryRow struct{ bytes.Buffer }

func (w *binaryRow) u(v uint64) { var b [8]byte; binary.LittleEndian.PutUint64(b[:], v); w.Write(b[:]) }
func (w *binaryRow) n(v int)    { var b [10]byte; n := binary.PutUvarint(b[:], uint64(v)); w.Write(b[:n]) }
func (w *binaryRow) s(v string) { w.n(len(v)); w.WriteString(v) }
func (w *binaryRow) u32(v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	w.Write(b[:])
}
func (w *binaryRow) strs(v []string) {
	w.n(len(v))
	for _, s := range v {
		w.s(s)
	}
}
func (w *binaryRow) m(v map[string]string) {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	w.n(len(keys))
	for _, k := range keys {
		w.s(k)
		w.s(v[k])
	}
}
func (w *binaryRow) ts(v string) {
	t, _ := time.Parse("2006-01-02 15:04:05.000000000", v)
	w.u(uint64(t.UnixNano()))
}

const bodyRequestColumns = `request_id,trace_id,started_at,completed_at,path,source_format,model,requested_model,stream,outcome,status_code,error,request_headers,request_body,response_headers,response_body,response_chunks,redaction_count,redaction_rules,lineage,body_len,body_sha256,body_residual,body_ref_offsets,body_refs`

const bodyRequestStructure = "request_id String,trace_id String,started_at DateTime64(9,'UTC'),completed_at DateTime64(9,'UTC'),path String,source_format String,model String,requested_model String,stream Bool,outcome String,status_code UInt16,error String,request_headers Map(String,String),request_body String,response_headers Map(String,String),response_body String,response_chunks Array(String),redaction_count Int32,redaction_rules Array(String)," + bodystore.SkeletonStructure

func bodyRows(requests []bodyRequest, rows [][]byte) *batch {
	b := binaryBatch("requests", bodyRequestColumns, rows)
	for _, r := range requests {
		if b.first == "" || r.row.StartedAt < b.first {
			b.first = r.row.StartedAt
		}
		if b.last == "" || r.row.StartedAt > b.last {
			b.last = r.row.StartedAt
		}
	}
	return b
}

func encodeBodyRequest(r bodyRequest) []byte {
	w := new(binaryRow)
	w.s(r.row.RequestID)
	w.s(r.row.TraceID)
	w.ts(r.row.StartedAt)
	w.ts(r.row.CompletedAt)
	w.s(r.row.Path)
	w.s(r.row.SourceFormat)
	w.s(r.row.Model)
	w.s(r.row.RequestedModel)
	if r.row.Stream {
		w.WriteByte(1)
	} else {
		w.WriteByte(0)
	}
	w.s(r.row.Outcome)
	w.WriteByte(byte(r.row.StatusCode))
	w.WriteByte(byte(r.row.StatusCode >> 8))
	w.s(r.row.Error)
	w.m(r.row.RequestHeaders)
	w.s(r.row.RequestBody)
	w.m(r.row.ResponseHeaders)
	w.s(r.row.ResponseBody)
	w.strs(r.row.ResponseChunks)
	w.u32(uint32(r.row.RedactionCount))
	w.strs(r.row.RedactionRules)
	w.u(r.lineage)
	w.u32(r.sk.Length)
	w.Write(r.sk.SHA256[:])
	w.s(string(r.sk.Residual))
	w.n(len(r.sk.Offsets))
	for _, o := range r.sk.Offsets {
		w.u32(o)
	}
	w.n(len(r.sk.Refs))
	for _, h := range r.sk.Refs {
		w.Write(h[:])
	}
	return w.Bytes()
}
func encodeBlob(lineage uint64, h bodystore.Hash, data []byte, ts string) []byte {
	w := new(binaryRow)
	w.u(lineage)
	w.Write(h[:])
	w.s(string(data))
	w.ts(ts)
	return w.Bytes()
}
