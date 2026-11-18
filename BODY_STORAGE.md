# Store each repeated request item once

`body-dedup: true` stores redacted protocol items in `body_blobs` and a byte skeleton in `requests`. The default is **false** in v0.1.0-plana.4. Responses stay inline. The plugin redacts the complete row before it computes a lineage or a hash.

`bodystore.Split` extracts messages, Responses input items, Gemini contents, tools, and system or instruction values. Invalid input falls back to a whole-body item. It never reserializes JSON. Every reconstruction checks each item's truncated SHA-256 and the request's full SHA-256. Missing or corrupt content returns `body incomplete`. The SQL view returns an empty body and `body_status = 'incomplete'`.

The wire format for new plugin rows is RowBinary. JSON strings cannot preserve arbitrary bytes or binary hashes. The reader projects bodies as base64 and hashes as hex, then verifies the original bytes in Go.

## Roll out without changing existing rows

1. Deploy `clickhouse-deriver-linux-arm64` from v0.1.0-plana.4. Keep the existing password-file mount and existing resource settings. Its startup adds columns with `ADD COLUMN IF NOT EXISTS`, creates `body_blobs` and `raw_request`, and keeps reading legacy rows. It retains 16-row, 8 MiB batches, 512 rows per pass, a 256 MiB query limit, and the fresh/backlog split.
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

Candidate IDs and timestamps are listed in one scan before any bodies are loaded. Body and verification reads use the primary-key timestamp. Only nonempty inline rows whose request ID already has facts qualify. The command reads one body at a time, inserts missing items, inserts a skeleton copy with the original `ingested_at`, and verifies it against the server's SHA-256 of the inline body. Only then does it issue synchronous lightweight deletes of verified inline rows in groups of at most 100. An interrupted run resumes from the remaining inline rows. An existing skeleton is verified before its legacy copy can disappear. The reader prefers a skeleton while both copies exist.

`--dry-run` writes nothing and reports input bytes and a projected **uncompressed upper bound** for skeletons plus new items. It counts each new item once across the whole dry run and checks committed keys. It refuses runs above one million new keys to bound memory. Candidate metadata uses memory proportional to the number of requests. Bodies are read one at a time. It does not predict compressed disk bytes. Lightweight deletes reclaim disk during subsequent merges. Run `OPTIMIZE TABLE cliproxy.requests FINAL` after migration to reclaim bytes now, with enough free disk for the merge. The projection does not estimate that temporary merge space.

## Retry and retention rules

The plugin buffers skeletons and owned item bytes, never the original full request body. It inserts blobs before their requests. A transient blob failure blocks later units. A request failure retries the unchanged request batch without reinserting its successful blob batch. A destination or credential change re-prepares the whole retained unit against the current config, including its blobs. Usage and legacy batches flush before body retries and continue when a body unit fails. Reconfigure never takes the flush lock, with either flag value. The hash cache is scoped to the destination, and failed body writes invalidate schema state before retry. HTTP 400 bisection of blobs converts affected requests back to inline storage.

The acknowledged-hash cache holds at most 512 lineages and 100,000 hashes per lineage, with a 16 MiB accounting cap charging 64 bytes per hash and 256 bytes per lineage. A preload has a 1.5 second deadline inside a three second budget shared by the flush. Failed preloads are never cached. Entries expire after 24 hours idle. First sight loads committed hashes with a lineage range query. Lineage zero is never preloaded. A failed preload resends identical items. Duplicate items from concurrent writers are harmless; readers choose one copy per key.

Never drop blob partitions to implement time retention. Items first seen in an older month can still serve current requests. Drop old request partitions first, then remove blobs for lineages with no remaining requests. Sweep unreferenced items only for lineages idle more than seven days. Before deleting a conversation, stop all writers and discard their caches by restarting them after deletion. Deleting requests and blobs while a writer is active races its acknowledged-hash cache and queued units. Idle time alone does not protect a resumed conversation. Re-redaction must re-key affected blobs and recompute request digests.

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

## Restore inline bodies for rollback

Disable dedup and stop other migration commands before running `clickhouse-deriver-linux-arm64 restore-bodies --dry-run`, then `restore-bodies`. Keep the new reader and deriver until restore completes. Each row is reconstructed and verified in Go, copied to inline storage with its original metadata, and checked against its SHA-256 in ClickHouse before the skeleton is deleted. Interrupted runs resume from remaining skeletons. Empty and binary bodies are supported. Blob rows remain available until you audit the restore and remove unreferenced blobs. Run `OPTIMIZE TABLE cliproxy.requests FINAL` to reclaim deleted skeleton copies. Rollback needs space for expanded inline bodies and merges.

The loop writes metadata-only facts with `parse_status='body_incomplete'` when reconstruction fails, then continues. Repair the blobs and delete the affected facts to retry them. Rebuild validates all dedup bodies before truncating facts and refuses an incomplete input. Stop the loop, capture writers, migrations, and blob deletion during rebuild so the validated input stays stable.

## Grants and release identity

Both plugin and deriver execute schema DDL even with dedup disabled. The writer needs SELECT, INSERT, CREATE TABLE, ALTER TABLE, CREATE VIEW, and DROP VIEW in the target database. Migration and restore also need ALTER DELETE. The existing production grant `GRANT ALL ON cliproxy.* TO cliproxy` covers this release. This document does not change grants or production settings.

Release tags and uploaded assets are immutable. Cut a new version for every changed binary, never force-move a tag or replace an existing release. Pin downloads by the full SHA-256 in the release's `SHA256SUMS`, verify the manifest and every downloaded asset before installation, and record the tag's commit. The release job refuses to publish over an existing release. ClickHouse 25.8 CI and the host test pin the service image by digest.

Blob repair must use `INSERT INTO cliproxy.body_blobs ... SETTINGS insert_deduplicate=0 FORMAT RowBinary` so ClickHouse accepts a previously deleted blob. Stop writers before repair and restart them after repair to discard their caches.
