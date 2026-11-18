package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/deriver/extract"
	"github.com/Plana-Solutions/cpa-clickhouse-sink/redact"
)

const epoch = "1970-01-01 00:00:00.000000000"

// deriver turns raw requests into request_facts rows.
type deriver struct {
	ch         *clickhouse
	db         string
	batchSize  int
	batchBytes int
	maxRows    int
	redactor   *redact.Redactor
	log        *slog.Logger
	// extract is extract.Extract; tests replace it to inject a poison row.
	extract func(extract.Row, bool) extract.Facts
}

func newDeriver(ch *clickhouse, db string, log *slog.Logger, r *redact.Redactor) *deriver {
	return &deriver{ch: ch, db: db, batchSize: 16, batchBytes: 8 << 20, maxRows: 512, log: log, extract: extract.Extract, redactor: r}
}

// setup creates the derived table and views.
func (d *deriver) setup(ctx context.Context) error {
	for _, stmt := range schemaFor(derivedSchema, d.db) {
		if err := d.ch.exec(ctx, stmt, nil); err != nil {
			return err
		}
	}
	return nil
}

// rebuild truncates request_facts and derives everything again.
func (d *deriver) rebuild(ctx context.Context) (int, error) {
	if err := d.ch.exec(ctx, "TRUNCATE TABLE "+d.db+".request_facts", nil); err != nil {
		return 0, fmt.Errorf("truncate: %w", err)
	}
	if err := d.ch.exec(ctx, "TRUNCATE TABLE "+d.db+".deriver_progress", nil); err != nil {
		return 0, err
	}
	total := 0
	for {
		n, err := d.runWindow(ctx, epoch, "2100-01-01 00:00:00.000000000", d.maxRows, true)
		total += n
		if err != nil || n == 0 {
			return total, err
		}
	}
}

// Fresh work cannot advance the durable backlog checkpoint.
func (d *deriver) pass(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cutoff := time.Now().UTC().Add(-5 * time.Minute).Format("2006-01-02 15:04:05.000000000")
	freshLimit := d.maxRows / 2
	if freshLimit == 0 {
		freshLimit = 1
	}
	n, err := d.runWindow(ctx, cutoff, "2100-01-01 00:00:00.000000000", freshLimit, false)
	if err != nil {
		return n, err
	}
	lower, err := d.watermark(ctx)
	if err != nil {
		return n, err
	}
	backlog, err := d.runWindow(ctx, lower, cutoff, d.maxRows-n, true)
	return n + backlog, err
}

func (d *deriver) runWindow(ctx context.Context, lower, upper string, limit int, checkpoint bool) (int, error) {
	start := time.Now()
	cursorTS, cursorID := lower, ""
	total := 0
	for total < limit {
		count := limit - total
		candidates, lastScanned, err := d.candidates(ctx, lower, upper, cursorTS, cursorID, min(count, d.batchSize))
		if err != nil {
			return total, fmt.Errorf("candidates: %w", err)
		}
		if lastScanned == nil {
			break
		}
		cursorTS, cursorID = lastScanned.IngestedAt, lastScanned.RequestID
		for len(candidates) > 0 {
			batch := bodyBatch(candidates[:min(d.batchSize, len(candidates))], d.batchBytes)
			ids := make([]string, len(batch))
			for i, c := range batch {
				ids[i] = c.RequestID
			}
			var facts []any
			seen := map[string]bool{}
			err := d.fetch(ctx, ids, func(r extract.Row) error {
				conv := extract.Conversation(r)
				prior := seen[conv]
				if !prior && conv != "" {
					known, err := d.knownConversations(ctx, []string{conv})
					if err != nil {
						return err
					}
					prior = known[conv]
				}
				f := d.safeExtract(r, prior)
				if f.ConversationID != "" {
					seen[f.ConversationID] = true
				}
				facts = append(facts, toFactsRow(f))
				return nil
			})
			if err != nil {
				return total, fmt.Errorf("bodies: %w", err)
			}
			if len(facts) != len(batch) {
				return total, fmt.Errorf("expected %d raw rows, got %d", len(batch), len(facts))
			}
			if err := d.ch.insertJSON(ctx, d.db+".request_facts", facts); err != nil {
				return total, fmt.Errorf("insert facts: %w", err)
			}
			last := batch[len(batch)-1]
			total += len(batch)
			if checkpoint {
				if err := d.ch.insertJSON(ctx, d.db+".deriver_progress", []any{map[string]string{"ingested_at": last.IngestedAt}}); err != nil {
					return total, fmt.Errorf("checkpoint: %w", err)
				}
			}
			candidates = candidates[len(batch):]
		}
	}
	if total > 0 {
		d.log.Info("window done", "rows", total, "since", lower, "took", time.Since(start))
	}
	return total, nil
}

// safeExtract is a second guard: extract.Extract recovers its own panics, but
// an injected or future extractor must not take the deriver down either.
func (d *deriver) safeExtract(r extract.Row, prior bool) (f extract.Facts) {
	defer func() {
		if p := recover(); p != nil {
			d.log.Warn("extractor panic, storing unparseable row", "request_id", r.RequestID)
			f = extract.Unparseable(r, fmt.Sprintf("panic: %v", p))
		}
	}()
	f = d.extract(r, prior)
	redactFacts(d.redactor, &f)
	if f.ParseStatus == extract.StatusUnparseable {
		d.log.Warn("unparseable request", "request_id", r.RequestID, "reason", f.ParseError)
	}
	return f
}

func (d *deriver) watermark(ctx context.Context) (string, error) {
	type wm struct {
		Lower string `json:"lower"`
		Rows  uint64 `json:"rows"`
	}
	var got wm
	err := selectRows(ctx, d.ch, "SELECT max(ingested_at) - INTERVAL 1 HOUR AS lower, count() AS rows FROM "+d.db+".deriver_progress", nil,
		func(r wm) { got = r })
	if err != nil {
		return "", err
	}
	if got.Rows == 0 {
		return epoch, nil
	}
	return got.Lower, nil
}

// candidate contains only scheduling metadata. Bodies never enter the sort.
type candidate struct {
	RequestID  string `json:"request_id"`
	IngestedAt string `json:"ingested_at"`
	Bytes      uint64 `json:"bytes"`
}

func bodyBatch(rows []candidate, budget int) []candidate {
	var size uint64
	for i, r := range rows {
		if i > 0 && (r.Bytes > uint64(budget) || size > uint64(budget)-r.Bytes) {
			return rows[:i]
		}
		size += r.Bytes
	}
	return rows
}

func (d *deriver) candidates(ctx context.Context, lower, upper, cursorTS, cursorID string, limit int) ([]candidate, *candidate, error) {
	q := `WITH (SELECT min(ingested_at) + INTERVAL 1 HOUR FROM ` + d.db + `.requests
 WHERE ingested_at >= {lower:DateTime64(9, 'UTC')} AND ingested_at < {upper:DateTime64(9, 'UTC')}
 AND (ingested_at, request_id) > ({cts:DateTime64(9, 'UTC')}, {cid:String})) AS window_upper
SELECT request_id, ingested_at
FROM ` + d.db + `.requests
WHERE ingested_at >= {lower:DateTime64(9, 'UTC')} AND ingested_at < {upper:DateTime64(9, 'UTC')}
 AND ingested_at < window_upper
 AND (ingested_at, request_id) > ({cts:DateTime64(9, 'UTC')}, {cid:String})
ORDER BY ingested_at, request_id LIMIT {limit:UInt32} SETTINGS max_threads=1`
	params := map[string]string{"lower": lower, "upper": upper, "cts": cursorTS, "cid": cursorID, "limit": fmt.Sprint(limit)}
	var rows []candidate
	err := selectRows(ctx, d.ch, q, params, func(r candidate) { rows = append(rows, r) })
	if err != nil || len(rows) == 0 {
		return rows, nil, err
	}
	last := rows[len(rows)-1]
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.RequestID
	}
	known := map[string]bool{}
	err = selectRows(ctx, d.ch, "SELECT DISTINCT request_id FROM "+d.db+".request_facts WHERE request_id IN {ids:Array(String)}", map[string]string{"ids": chArray(ids)}, func(r candidate) { known[r.RequestID] = true })
	if err != nil {
		return nil, nil, err
	}
	pending := rows[:0]
	ids = nil
	for _, r := range rows {
		if !known[r.RequestID] {
			pending = append(pending, r)
			ids = append(ids, r.RequestID)
		}
	}
	rows = pending
	if len(rows) == 0 {
		return rows, &last, nil
	}
	sizes := map[string]uint64{}
	err = selectRows(ctx, d.ch, `SELECT request_id, length(request_body)+length(response_body)+arraySum(arrayMap(x -> length(x),response_chunks))+length(toJSONString(request_headers))+length(error) AS bytes FROM `+d.db+`.requests PREWHERE request_id IN {ids:Array(String)} SETTINGS max_threads=1, max_block_size=1, preferred_block_size_bytes=1048576`, map[string]string{"ids": chArray(ids)}, func(r candidate) { sizes[r.RequestID] = r.Bytes })
	for i := range rows {
		rows[i].Bytes = sizes[rows[i].RequestID]
	}
	return rows, &last, err
}

func (d *deriver) fetch(ctx context.Context, ids []string, fn func(extract.Row) error) error {
	q := `SELECT request_id, trace_id, started_at, ingested_at,
 duration_ms, path, source_format, model, stream, outcome, status_code, error,
 request_headers, request_body, response_body, response_chunks, redaction_count
FROM ` + d.db + `.requests PREWHERE request_id IN {ids:Array(String)}
ORDER BY ingested_at, request_id
SETTINGS max_threads=1, max_block_size=1, preferred_block_size_bytes=1048576`
	return streamRows(ctx, d.ch, q, map[string]string{"ids": chArray(ids)}, fn)
}

func (d *deriver) knownConversations(ctx context.Context, ids []string) (map[string]bool, error) {
	known := map[string]bool{}
	if len(ids) == 0 {
		return known, nil
	}
	type conv struct {
		ID string `json:"conversation_id"`
	}
	err := selectRows(ctx, d.ch, "SELECT DISTINCT conversation_id FROM "+d.db+".request_facts WHERE conversation_id IN {ids:Array(String)}",
		map[string]string{"ids": chArray(ids)}, func(c conv) { known[c.ID] = true })
	return known, err
}

// factsRow is the JSONEachRow shape of request_facts.
type factsRow struct {
	RequestID            string   `json:"request_id"`
	StartedAt            string   `json:"started_at"`
	IngestedAt           string   `json:"ingested_at"`
	Model                string   `json:"model"`
	SourceFormat         string   `json:"source_format"`
	Outcome              string   `json:"outcome"`
	StatusCode           uint16   `json:"status_code"`
	Stream               bool     `json:"stream"`
	DurationMs           uint64   `json:"duration_ms"`
	Error                string   `json:"error"`
	Path                 string   `json:"path"`
	Kind                 string   `json:"kind"`
	RequestKind          string   `json:"request_kind"`
	ThreadSource         string   `json:"thread_source"`
	TurnID               string   `json:"turn_id"`
	Client               string   `json:"client"`
	ConversationID       string   `json:"conversation_id"`
	ConversationSource   string   `json:"conversation_source"`
	ParentConversationID string   `json:"parent_conversation_id"`
	ResponseID           string   `json:"response_id"`
	PreviousResponseID   string   `json:"previous_response_id"`
	SystemPromptHash     uint64   `json:"system_prompt_hash"`
	LastUserText         string   `json:"last_user_text"`
	ReplyText            string   `json:"reply_text"`
	ReasoningText        string   `json:"reasoning_text"`
	ToolsOffered         []string `json:"tools_offered"`
	ToolsCalled          []string `json:"tools_called"`
	StopReason           string   `json:"stop_reason"`
	PriorSeen            bool     `json:"prior_seen"`
	InputItems           uint32   `json:"input_items"`
	MsgDirection         []string `json:"msg_direction"`
	MsgPosition          []uint32 `json:"msg_position"`
	MsgRole              []string `json:"msg_role"`
	MsgType              []string `json:"msg_type"`
	MsgToolName          []string `json:"msg_tool_name"`
	MsgCallID            []string `json:"msg_call_id"`
	MsgArguments         []string `json:"msg_arguments"`
	MsgIsError           []bool   `json:"msg_is_error"`
	MsgText              []string `json:"msg_text"`
	MsgTextHash          []uint64 `json:"msg_text_hash"`
	ParseStatus          string   `json:"parse_status"`
	ParseError           string   `json:"parse_error"`
	ExtractorVersion     uint16   `json:"extractor_version"`
}

func toFactsRow(f extract.Facts) factsRow {
	r := factsRow{
		RequestID: f.RequestID, StartedAt: orEpoch(f.StartedAt), IngestedAt: orEpoch(f.IngestedAt),
		Model: f.Model, SourceFormat: f.SourceFormat, Outcome: f.Outcome, StatusCode: f.StatusCode,
		Stream: f.Stream, DurationMs: f.DurationMs, Error: f.Error, Path: f.Path, Kind: f.Kind,
		RequestKind: f.RequestKind, ThreadSource: f.ThreadSource, TurnID: f.TurnID, Client: f.Client,
		ConversationID: f.ConversationID, ConversationSource: f.ConversationSource,
		ParentConversationID: f.ParentConversationID, ResponseID: f.ResponseID,
		PreviousResponseID: f.PreviousResponseID, SystemPromptHash: f.SystemPromptHash,
		LastUserText: f.LastUserText, ReplyText: f.ReplyText, ReasoningText: f.ReasoningText,
		ToolsOffered: nonNil(f.ToolsOffered), ToolsCalled: nonNil(f.ToolsCalled), StopReason: f.StopReason,
		PriorSeen: f.PriorSeen, InputItems: f.InputItems, ParseStatus: f.ParseStatus,
		ParseError: f.ParseError, ExtractorVersion: f.ExtractorVersion,
	}
	n := len(f.Messages)
	r.MsgDirection, r.MsgRole, r.MsgType = make([]string, n), make([]string, n), make([]string, n)
	r.MsgToolName, r.MsgCallID, r.MsgArguments, r.MsgText = make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	r.MsgPosition, r.MsgIsError, r.MsgTextHash = make([]uint32, n), make([]bool, n), make([]uint64, n)
	for i, m := range f.Messages {
		r.MsgDirection[i], r.MsgPosition[i], r.MsgRole[i], r.MsgType[i] = m.Direction, m.Position, m.Role, m.Type
		r.MsgToolName[i], r.MsgCallID[i], r.MsgArguments[i] = m.ToolName, m.CallID, m.Arguments
		r.MsgIsError[i], r.MsgText[i], r.MsgTextHash[i] = m.IsError, m.Text, m.TextHash
	}
	return r
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func orEpoch(ts string) string {
	if ts == "" {
		return epoch
	}
	return ts
}
