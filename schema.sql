-- Raw layer, owned by the clickhouse-sink plugin. The plugin runs these
-- statements on startup and after a failed insert. {db} is replaced with the
-- configured database. Statements are separated by a line holding only ';'.
-- Derived tables and views (request_facts, messages, request_summary) are
-- owned by the deriver service, not by this file. See DESIGN.md.

CREATE DATABASE IF NOT EXISTS {db}
;
CREATE TABLE IF NOT EXISTS {db}.requests
(
    request_id       String COMMENT 'Host lifecycle request ID (UUID), one per client request',
    trace_id         String COMMENT 'Inbound HTTP request/connection ID; shared by all websocket turns on one connection',
    started_at       DateTime64(9, 'UTC') COMMENT 'When the proxy started handling the request',
    completed_at     DateTime64(9, 'UTC') COMMENT 'When the request reached its terminal outcome',
    ingested_at      DateTime64(9, 'UTC') DEFAULT now64(9) COMMENT 'When the row was inserted; the deriver watermark',
    duration_ms      UInt64 MATERIALIZED toUInt64(greatest(0, dateDiff('millisecond', started_at, completed_at))) COMMENT 'completed_at - started_at',
    path             LowCardinality(String) COMMENT 'Client request path, e.g. /v1/responses, /v1/messages/count_tokens',
    source_format    LowCardinality(String) COMMENT 'Client protocol: openai, openai-response, claude, gemini, ...',
    model            LowCardinality(String) COMMENT 'Model that executed the request',
    requested_model  LowCardinality(String) COMMENT 'Model the client asked for, before aliasing',
    stream           Bool,
    outcome          LowCardinality(String) COMMENT 'succeeded, failed, rejected or canceled',
    status_code      UInt16,
    error            String COMMENT 'Terminal error message, redacted',
    request_headers  Map(String, String) COMMENT 'Client request headers, lower-case; credentials removed, values redacted',
    request_body     String COMMENT 'Client request body as sent, redacted' CODEC(ZSTD(3)),
    response_headers Map(String, String) COMMENT 'Response headers delivered to the client, redacted',
    response_body    String COMMENT 'Non-streaming response body, redacted; empty for streams' CODEC(ZSTD(3)),
    response_chunks  Array(String) COMMENT 'Stream chunks exactly as delivered (one element each), redacted; empty for non-streams' CODEC(ZSTD(3)),
    redaction_count  Int32 COMMENT 'Secrets replaced in this row; -1 means redaction failed and bodies were emptied',
    redaction_rules  Array(LowCardinality(String)) COMMENT 'Distinct rule names that matched (known-value, gitleaks rule IDs)'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (started_at, request_id)
SETTINGS non_replicated_deduplication_window = 1000
COMMENT 'Raw client requests and responses, one row per request. Source of truth; large bodies, query request_facts/messages first.'
;
CREATE TABLE IF NOT EXISTS {db}.usage
(
    request_id             String COMMENT 'Per-attempt usage ID (not the lifecycle request_id; see usage_by_request)',
    trace_id               String,
    selection_id           String,
    requested_at           DateTime64(9, 'UTC') COMMENT 'When this upstream attempt started',
    ingested_at            DateTime64(9, 'UTC') DEFAULT now64(9),
    provider               LowCardinality(String),
    executor_type          LowCardinality(String),
    base_url               LowCardinality(String),
    model                  LowCardinality(String),
    alias                  LowCardinality(String),
    response_model         LowCardinality(String),
    api_key_hash           LowCardinality(String) COMMENT 'First 16 hex of sha256(client API key)',
    access_provider        LowCardinality(String),
    session_id             String COMMENT 'CPA session identity',
    parent_session_id      String,
    auth_id                LowCardinality(String) COMMENT 'Upstream credential used',
    auth_index             LowCardinality(String),
    auth_type              LowCardinality(String),
    source                 LowCardinality(String),
    reasoning_effort       LowCardinality(String),
    service_tier           LowCardinality(String),
    effective_service_tier LowCardinality(String),
    response_service_tier  LowCardinality(String),
    stream                 Bool,
    generate               Bool,
    latency_ms             UInt64,
    ttft_ms                UInt64,
    failed                 Bool,
    failure_status         UInt16,
    failure_body           String COMMENT 'Upstream failure body, redacted' CODEC(ZSTD(3)),
    input_tokens           Int64,
    output_tokens          Int64,
    reasoning_tokens       Int64,
    cached_tokens          Int64,
    cache_read_tokens      Int64,
    cache_creation_tokens  Int64,
    total_tokens           Int64,
    response_headers       Map(String, String) COMMENT 'Upstream response headers, redacted'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(requested_at)
ORDER BY (requested_at, trace_id)
SETTINGS non_replicated_deduplication_window = 1000
COMMENT 'One row per upstream attempt; retries and credential fallbacks add rows.'
;
-- Attributes each usage attempt to the latest request on the same trace that
-- started at or before it (see DESIGN.md A1). lifecycle_request_id is empty
-- when no request matched.
CREATE OR REPLACE VIEW {db}.usage_by_request AS
SELECT
    r.request_id AS lifecycle_request_id,
    u.*
FROM {db}.usage AS u
ASOF LEFT JOIN
(
    SELECT request_id, trace_id, started_at FROM {db}.requests WHERE trace_id != ''
) AS r
ON u.trace_id = r.trace_id AND u.requested_at >= r.started_at

;
ALTER TABLE {db}.requests
    ADD COLUMN IF NOT EXISTS lineage          UInt64 COMMENT 'Conversation scope: first 8 bytes (LE) of SHA-256 of the client session identity; 0 = none. Locality hint only',
    ADD COLUMN IF NOT EXISTS body_len         UInt32 COMMENT 'Length of the redacted request body in bytes',
    ADD COLUMN IF NOT EXISTS body_sha256      FixedString(32) COMMENT 'SHA-256 of the redacted request body; verifies every reconstruction',
    ADD COLUMN IF NOT EXISTS body_residual    String COMMENT 'Request body bytes outside its blobs' CODEC(ZSTD(3)),
    ADD COLUMN IF NOT EXISTS body_ref_offsets Array(UInt32) COMMENT 'Residual offset where body_refs[i] is spliced in (non-decreasing)' CODEC(Delta, ZSTD(3)),
    ADD COLUMN IF NOT EXISTS body_refs        Array(FixedString(16)) COMMENT 'body_blobs.hash of each spliced item, in body order' CODEC(ZSTD(3))
;
ALTER TABLE {db}.requests ADD INDEX IF NOT EXISTS lineage_bf lineage TYPE bloom_filter GRANULARITY 4
;
CREATE TABLE IF NOT EXISTS {db}.body_blobs
(
    lineage     UInt64 COMMENT 'requests.lineage of the conversation that stored it',
    hash        FixedString(16) COMMENT 'substring(SHA256(data), 1, 16)',
    data        String COMMENT 'Exact redacted bytes of one protocol item (messages/input/contents/tools/system element or system/instructions value)' CODEC(ZSTD(3)),
    first_seen  DateTime64(9, 'UTC') COMMENT 'started_at of the first request that carried it',
    ingested_at DateTime64(9, 'UTC') DEFAULT now64(9),
    INDEX hash_bf hash TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(first_seen)
ORDER BY (lineage, first_seen, hash)
SETTINGS index_granularity = 1024, index_granularity_bytes = 1048576, non_replicated_deduplication_window = 1000
COMMENT 'Deduplicated request-body items. Reconstruct with raw_request(id) or by splicing requests.body_refs into body_residual.'
;
CREATE OR REPLACE VIEW {db}.raw_request AS
WITH
 r AS (SELECT * FROM {db}.requests WHERE request_id = {id:String} ORDER BY request_body = '' DESC LIMIT 1),
 b AS (SELECT groupArray(hash) AS hashes, groupArray(data) AS pieces FROM (
   SELECT hash, data FROM {db}.body_blobs WHERE (lineage, hash) IN (SELECT lineage, arrayJoin(body_refs) FROM r) LIMIT 1 BY lineage, hash)),
 joined AS (SELECT r.*, b.*,
   concat(arrayStringConcat(arrayMap(k -> concat(
     substring(body_residual, if(k=1,1,body_ref_offsets[k-1]+1),
       greatest(toInt64(body_ref_offsets[k])-if(k=1,0,body_ref_offsets[k-1]),0)),
     pieces[indexOf(hashes,body_refs[k])]), arrayEnumerate(body_refs))),
     substring(body_residual,if(empty(body_ref_offsets),1,body_ref_offsets[-1]+1))) AS restored
   FROM r CROSS JOIN b),
 checked AS (SELECT *,
   if(request_body != '' OR body_sha256 = repeat(char(0),32), request_body, restored) AS candidate,
   ((body_sha256 = repeat(char(0),32) AND body_len=0 AND empty(body_refs) AND empty(body_ref_offsets) AND empty(body_residual)) OR (
     length(candidate) = body_len AND SHA256(candidate) = body_sha256
     AND length(body_refs) = length(body_ref_offsets)
     AND arrayAll(k -> body_ref_offsets[k] <= length(body_residual) AND (k=1 OR body_ref_offsets[k]>=body_ref_offsets[k-1]),arrayEnumerate(body_refs))
     AND arrayAll(h -> indexOf(hashes,h)>0 AND substring(SHA256(pieces[indexOf(hashes,h)]),1,16)=h, body_refs))) AS complete
   FROM joined)
SELECT request_id, if(complete,candidate,'') AS request_body, if(complete,'complete','incomplete') AS body_status FROM checked
;
