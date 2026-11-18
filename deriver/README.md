# deriver

Small Go service that turns the raw rows the `clickhouse-sink` plugin writes
(`requests`, `usage`, see `../schema.sql`) into searchable derived data. It runs
as its own compose service, outside CLIProxyAPI. Design: `../DESIGN.md` section B.

## What it creates

On startup (idempotent: `CREATE TABLE IF NOT EXISTS`, `CREATE OR REPLACE VIEW`):

| Object | What it is |
|---|---|
| `deriver_progress` | Durable backlog checkpoint, independent of live facts. |
| `request_facts` | One row per raw request: identity (`conversation_id`, `parent_conversation_id`, `client`, `kind`, Codex `request_kind`/`turn_id`), `reply_text`, `reasoning_text`, `tools_offered`/`tools_called`, `stop_reason`, `parse_status`, and the emitted messages as parallel `msg_*` arrays. `ORDER BY (started_at, request_id)`. |
| `messages` (view) | `request_facts ARRAY JOIN msg_*`: one row per emitted message (`direction`, `position`, `role`, `type`, `tool_name`, `call_id`, `arguments`, `is_error`, `text`, `text_hash`). |
| `request_summary` (view) | `request_facts` without the arrays, plus `usage_by_request` aggregated per request: summed tokens, `attempts`, `failed_attempts`, `final_provider`/`final_auth_id` (last attempt), `cpa_session_id`. `attempts = 0` means no usage matched. |

Every table and column has a `COMMENT`, so `DESCRIBE` / `system.columns` explains
the schema. Views carry a table-level comment.

## How rows are derived

- **Loop**: every 30 s, process up to `--max-rows-per-pass` rows. Reserve
  half for the newest five minutes, then spend the remaining allowance on the
  oldest backlog. Each window runs in `(ingested_at, request_id)` order.
  Candidates contain only IDs and timestamps, selected in one-hour windows.
  Payload sizes choose batches before streaming bodies with `PREWHERE`, one read thread and one-row blocks.
- **Checkpoint**: `deriver_progress` records copied raw ingestion times only
  after successful backlog inserts. The backlog starts at its maximum minus one
  hour, or the epoch when empty. Fresh inserts cannot advance this checkpoint.
  The exact anti-join checks facts only for the candidate page IDs.
  Failed inserts stop the pass without retry. A committed but unacknowledged
  batch is skipped by the next anti-join. Existing installations start with an
  empty checkpoint and rescan narrow IDs safely.
- **Ordering**: `prior_seen` means previously processed. An old imported request
  can see a newer live request from the same conversation. Rebuilds process
  oldest first without the live lane, across bounded passes until empty.
- **Messages emitted (tail rule)**: the first request seen for a
  `conversation_id` emits its whole input. Later requests emit only the input
  after the last model-produced item (assistant message, reasoning, thinking,
  any `*_call` / `tool_use`), ignoring a trailing assistant prefill message.
  The response's output items are always emitted. `prior_seen` records which case applied.
- **conversation_id** (from the raw request only): Claude Code session header,
  then Claude `metadata.user_id` session (`claude:<session>[:agent:<id>]`),
  then Codex session/thread ids (`codex:<thread>`; the body's
  `client_metadata` wins over headers, because websocket headers belong to the
  upgrade request), then generic session headers (`session:<id>`), else a
  fingerprint of protocol + client + system prompt + first user message
  (`fp:<hash>`, `conversation_source = 'inferred'`). A Codex `request_kind`
  other than `turn` is appended (`codex:<thread>:memory`).
- **Streams** are reassembled from `response_chunks` (unframed SSE lines,
  framed SSE blocks, or bare JSON websocket messages). Responses: the terminal
  event's `output`, else `response.output_item.done` items (Codex sends
  `response.completed` with an empty `output`), else added items plus deltas
  for canceled or incomplete streams. Claude: per-block deltas, including
  `input_json_delta` and thinking. Chat: `tool_calls` by index.
- **Text** has inline base64 media and `encrypted_content` replaced by
  placeholders. Redaction markers from the plugin are kept as-is.
  `text_hash` is for grouping repeated prompts, not for dropping rows.
- **parse_status**: `ok`; `partial` (stream without a terminal event, e.g.
  canceled); `unsupported` (protocol not handled: metadata only); `empty`;
  `redaction_failed` (plugin emptied the bodies); `unparseable` (request body is
  not JSON, or the extractor panicked; the row is still written, so a bad row
  can't block the loop).
- `extractor_version` is stored per row. When extraction logic changes, bump
  `extract.Version` and run a rebuild.

Logs go to stdout (`log/slog` text) and never include body content. ClickHouse
insert errors are logged as code and name only, because parse errors quote input.

## Configuration

| Env | Default |
|---|---|
| `CLICKHOUSE_URL` | `http://clickhouse:8123` |
| `CLICKHOUSE_USER` | (none) |
| `CLICKHOUSE_PASSWORD` | (none) |
| `CLICKHOUSE_DB` | `cliproxy` |

The user needs write access to the database. The deriver waits, retrying every
interval, until the plugin has created the raw schema.

Recommended starting limits for a 1.5 GB ClickHouse container:

| Flag | Default | Meaning |
|---|---|---|
| `--batch-rows` | `16` | Maximum bodies and facts per batch. |
| `--batch-bytes` | `8388608` | Raw payload bytes per batch. One oversized row runs alone. |
| `--max-rows-per-pass` | `512` | Live plus backlog rows per pass, at least 2. |
| `--query-memory-bytes` | `268435456` | ClickHouse memory cap per deriver query. |
| `--interval` | `30s` | Delay after each pass. |

The byte budget includes bodies, chunks, request headers and error text. It
bounds payload, not total memory. JSON, extracted facts, decompression and one
oversized row need extra memory. Every deriver query uses one thread and a 30-second execution limit. Normal
passes have a two-minute deadline, so slow backlog work yields to the live lane
on the next pass. Keep the writer's query memory cap enabled.
Latency depends on live arrival rate and server load. See
`../benchmarks/deriver-memory.md` for local measurements.

## Compose service

Add to `~/.cli-proxy-api/compose.yml`:

```yaml
  deriver:
    build: ../git/Cli-Proxy-API-Management-Center/plugins/clickhouse-sink/deriver
    container_name: inference-deriver
    restart: unless-stopped
    environment:
      CLICKHOUSE_URL: http://clickhouse:8123
      CLICKHOUSE_USER: cliproxy
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
      CLICKHOUSE_DB: cliproxy
    depends_on:
      - clickhouse
```

## Rebuild

A one-off run that truncates `request_facts`, derives every raw row and exits.
Stop the loop service first so the two don't race. Run this from `~/.cli-proxy-api`:

```sh
docker compose stop deriver
docker compose run --rm deriver --rebuild
docker compose start deriver
```

If a rebuild is interrupted, the loop service resumes it safely from the watermark.

## Development

```sh
cd plugins/clickhouse-sink/deriver
gofmt -l . ; go vet ./... ; go test ./...
```

`extract` is a pure package: `extract.Extract(row, priorSeen)` returns one facts
row, and `extract.Conversation(row)` returns the conversation_id ahead of time so the
caller can look up `priorSeen`. Its fixtures in `extract/testdata/` are
synthetic: never paste real prompts, IDs or credentials there.

Integration tests (crash between passes, poison row, rebuild idempotence,
views and comments) use a scratch database `cliproxy_deriver_test`, created
from `../schema.sql` and dropped afterwards. They are skipped unless enabled:

```sh
env DERIVER_INTEGRATION=1 CLICKHOUSE_URL=http://127.0.0.1:8123 CLICKHOUSE_USER=cliproxy CLICKHOUSE_PASSWORD=$(grep '^CLICKHOUSE_PASSWORD=' ~/.cli-proxy-api/.env | cut -d= -f2-) go test -count=1 ./...
```

(Works in bash and fish 3.4+.)

## Mounted password secret

Set `CLICKHOUSE_PASSWORD_FILE=/run/secrets/ch_writer` to read the password directly
with the stock distroless image. The file takes precedence over `CLICKHOUSE_PASSWORD`.
Trailing CR and LF characters are removed. Other whitespace is preserved.
An unreadable file stops startup without logging its path or contents.
The mounted file must be readable by the image's nonroot user.
