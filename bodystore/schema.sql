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
 r AS (SELECT * FROM {db}.requests PREWHERE request_id = {id:String} ORDER BY body_sha256 = repeat(char(0),32) LIMIT 1),
 b AS (SELECT groupArray(hash) AS hashes, groupArray(data) AS pieces FROM (
   SELECT hash, data FROM {db}.body_blobs PREWHERE (lineage, hash) IN (SELECT lineage, arrayJoin(body_refs) FROM r) LIMIT 1 BY lineage, hash)),
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
SETTINGS max_threads=1, max_block_size=1, preferred_block_size_bytes=1048576
;
