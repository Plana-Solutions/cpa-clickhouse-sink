# Store each repeated request item once

`body-dedup: true` stores redacted protocol items in `body_blobs` and a byte skeleton in `requests`. The default is **false** in v0.1.0-plana.3. Responses stay inline. The plugin redacts the complete row before it computes a lineage or a hash.

`bodystore.Split` extracts messages, Responses input items, Gemini contents, tools, and system or instruction values. Invalid input falls back to a whole-body item. It never reserializes JSON. Every reconstruction checks each item's truncated SHA-256 and the request's full SHA-256. Missing or corrupt content returns `body incomplete`. The SQL view returns an empty body and `body_status = 'incomplete'`.

The wire format for new plugin rows is RowBinary. JSON strings cannot preserve arbitrary bytes or binary hashes. The reader projects bodies as base64 and hashes as hex, then verifies the original bytes in Go.

## Roll out without changing existing rows

1. Deploy `clickhouse-deriver-linux-arm64` from v0.1.0-plana.3. Keep the existing password-file mount and existing resource settings. Its startup adds columns with `ADD COLUMN IF NOT EXISTS`, creates `body_blobs` and `raw_request`, and keeps reading legacy rows. It retains 16-row, 8 MiB batches, 512 rows per pass, a 256 MiB query limit, and the fresh/backlog split.
2. Deploy every UI and export reader with `bodystore.Reader` before enabling dedup. This repository does not deploy the archive UI. Old readers see an empty request body for dedup rows.
3. Install the plugin from the same release. Enable the following setting in the existing plugin config. Keep the existing URL, database, username, password injection, and redaction settings.

   ```yaml
   plugins:
     configs:
       clickhouse-sink:
         store-bodies: true
         body-dedup: true
   ```

4. Run the missing-blob and hash audits in `queries.sql`. Check SQL `body_status` and reconstructed bytes for newly written requests. Observe the plugin's cumulative blobs-written, blobs-skipped, and preload-failure logs.
5. Derive existing rows first. With the writer credentials already used by the deriver, run `clickhouse-deriver-linux-arm64 migrate-bodies --dry-run`. Then run `clickhouse-deriver-linux-arm64 migrate-bodies`. Never run the migration with a legacy deriver or legacy UI reader.

This release publication does not perform any of these production steps.

## Migrate existing rows

The command uses `CLICKHOUSE_URL`, `CLICKHOUSE_DB`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD_FILE` or `CLICKHOUSE_PASSWORD`, just like the deriver. Stop concurrent migration commands. Normal plugin and deriver writes may continue.

Only nonempty inline rows whose request ID already has facts qualify. The command reads one body at a time, inserts missing items, inserts a skeleton copy with the original `ingested_at`, and verifies it against the server's SHA-256 of the inline body. Only then does it issue synchronous lightweight deletes of verified inline rows in groups of at most 100. An interrupted run resumes from the remaining inline rows. An existing skeleton is verified before its legacy copy can disappear. The reader prefers a skeleton while both copies exist.

`--dry-run` writes nothing and reports input bytes and a projected **uncompressed upper bound** for skeletons plus new items. It checks committed blob keys but does not simulate dedup between hypothetical migrated rows, so repeated new items can be counted more than once. It does not predict compressed disk bytes. Lightweight deletes reclaim disk during subsequent merges, so immediate free space can differ from the projected reduction.

## Retry and retention rules

The plugin buffers skeletons and owned item bytes, never the original full request body. It inserts blobs before their requests. A transient blob failure blocks later units. A request failure retries the unchanged request batch without reinserting its successful blob batch. Each in-flight unit retains its original destination across config changes. The hash cache is scoped to the destination, and failed body writes invalidate schema state before retry. HTTP 400 bisection of blobs converts affected requests back to inline storage.

The acknowledged-hash cache holds at most 512 lineages and 100,000 hashes per lineage. Entries expire after 24 hours idle. First sight loads committed hashes with a lineage range query. Lineage zero is never preloaded. A failed preload resends identical items. Duplicate items from concurrent writers are harmless; readers choose one copy per key.

Never drop blob partitions to implement time retention. Items first seen in an older month can still serve current requests. Drop old request partitions first, then remove blobs for lineages with no remaining requests. Sweep unreferenced items only for lineages idle more than seven days. Delete a conversation's requests and blobs together by lineage. Re-redaction must re-key affected blobs and recompute request digests.

## Reproduce acceptance

Use the shared ClickHouse binary with a disposable data directory and a server memory cap of 1503238553 bytes. Keep at least 3 GiB free. The test database is `cliproxy_body_test` and is dropped on exit.

```sh
(cd deriver && go build -o /tmp/clickhouse-deriver .)
BODY_INTEGRATION_URL=http://127.0.0.1:19123 \
DERIVER_BINARY=/tmp/clickhouse-deriver \
go test -race -run TestBodyIntegration -v .
```

`BODY_LIVE_FIXTURES` optionally points to JSONL rows with base64 request and response bodies. The test compares legacy and dedup facts, verifies reader and SQL byte equality, exercises mixed rows and migration reruns, and catches up 3,200 rows timestamped fifteen minutes before the loop starts. It prints compressed sizes and peak query memory. Delete fetched traffic and the entire disposable ClickHouse data directory after the run.

For the production-image check, run `BODY_DEDUP=true scripts/verify_host_release.sh PLUGIN.zip DERIVER` on the cluster. It creates an isolated internal Docker network, the exact CPA production image, ClickHouse 25.8, and a fake upstream. It verifies input hashes through `raw_request`, facts, and usage, then removes its containers, network, and files.
