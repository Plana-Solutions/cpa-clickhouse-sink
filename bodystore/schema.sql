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
 b AS (SELECT lineage, hash, data, substring(SHA256(data),1,16)=hash AS valid
   FROM {db}.body_blobs PREWHERE (lineage,hash) IN (SELECT lineage,arrayJoin(body_refs) FROM r)
   LIMIT 1 BY lineage,hash),
 fragments AS (
   SELECT ordinal, concat(
     substring((SELECT body_residual FROM r), if(ordinal<=1,1,body_ref_offsets[ordinal-1]+1),
       greatest(toInt64(body_ref_offsets[ordinal])-if(ordinal<=1,0,body_ref_offsets[ordinal-1]),0)),
     b.data) AS fragment,
     (ordinal=0 OR (b.valid AND body_ref_offsets[ordinal]<=length((SELECT body_residual FROM r))
       AND (ordinal=1 OR body_ref_offsets[ordinal]>=body_ref_offsets[ordinal-1]))) AS valid
   FROM (SELECT lineage,body_refs,body_ref_offsets FROM r) AS refs LEFT ARRAY JOIN body_refs AS ref, arrayEnumerate(body_refs) AS ordinal
   LEFT JOIN b ON refs.lineage=b.lineage AND ref=b.hash),
 assembled AS (SELECT arrayStringConcat(groupArray(fragment)) AS prefix,
   min(valid) AS valid FROM (SELECT fragment,valid FROM fragments ORDER BY ordinal)),
 checked AS (SELECT r.request_id,
   if(request_body!='' OR body_sha256=repeat(char(0),32),request_body,
     concat(prefix,substring(body_residual,if(empty(body_ref_offsets),1,body_ref_offsets[-1]+1)))) AS candidate,
   ((body_sha256=repeat(char(0),32) AND body_len=0 AND empty(body_refs) AND empty(body_ref_offsets) AND empty(body_residual))
     OR (length(candidate)=body_len AND SHA256(candidate)=body_sha256
       AND (request_body!='' OR (valid AND length(body_refs)=length(body_ref_offsets))))) AS complete
   FROM r CROSS JOIN assembled),
 inline_copy AS (SELECT if(body_sha256=repeat(char(0),32)
     OR (length(request_body)=body_len AND SHA256(request_body)=body_sha256),request_body,'') AS inline_body
   FROM {db}.requests PREWHERE request_id={id:String} AND length(request_body)>0
   ORDER BY body_sha256=repeat(char(0),32) LIMIT 1)
SELECT request_id, if(complete,candidate,inline_body) AS request_body,
 if(complete OR inline_body!='','complete','incomplete') AS body_status
FROM checked LEFT JOIN inline_copy ON 1=1
SETTINGS max_threads=1, max_block_size=1, preferred_block_size_bytes=1048576

;
