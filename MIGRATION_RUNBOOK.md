# Migrate existing inline bodies on the 2 GB production server

This procedure targets ClickHouse 25.8 with a 2 GiB container, a 0.75 server memory ratio, a 64 MiB mark cache, four background merge workers, `merge_max_block_size=256`, and a 2 GiB maximum merge size. New capture already uses dedup. The production logger runs at warning level. Production also disables text_log, trace_log, metric_log, asynchronous_metric_log, query_thread_log, part_log, processors_profile_log and query_log, and sets uncompressed_cache_size=0. These settings were verified through a read-only Portainer archive request. They matter to the memory qualification. Publication does not apply these steps to production.

## Prepare permissions and a rollback copy

1. Verify the plana.9 assets against `SHA256SUMS`. Deploy the new reader view and deriver before migration. Keep the existing 16-row and 8 MiB batch limits. The normal deriver retains its 256 MiB query cap. Migration and restore default to a 128 MiB query cap. This covers inline request bodies up to 16 MiB. Migration refuses larger bodies at the size preflight before the candidate snapshot or payload reads.
2. Confirm normal capture and derivation work without code 241 or OOM kills. Stop any other migration or restore command. Keep capture and the normal deriver running.
3. An operator must add the following entries to the existing migration user's `users.d` XML grants. Keep its existing `GRANT ALL ON cliproxy.*` entry. These grants are required only by migration and restore. The normal reader needs no system grants.

   ```xml
   <query>GRANT SELECT(metric,value) ON system.asynchronous_metrics</query>
   <query>GRANT SELECT(name,value) ON system.server_settings</query>
   <query>GRANT SELECT ON system.errors</query>
   <query>GRANT SELECT ON system.mutations</query>
   <query>GRANT SYSTEM JEMALLOC PURGE ON *.*</query>
   ```

   Edit `/opt/cpa/clickhouse/users.d/plana-users.xml` in place. It is a single read-only bind-mounted file. Preserve its inode with `sudo -n sh -c 'cat prepared-users.xml > /opt/cpa/clickhouse/users.d/plana-users.xml'`. Never use `sed -i`, an atomic rename, or an editor that replaces the file. Those writes leave the container on the old inode and silently retain the old grants until restart. Wait at least five seconds, then test every grant as `cliproxy`, including the monitoring queries below. Do not put a password in shell arguments or logs. A database-only user fails safely at the memory preflight. A missing purge privilege fails safely if memory pressure later requires a purge.
4. Confirm that every legacy row has a fact. The command only snapshots derived rows. Use size predicates so ClickHouse does not select full body rows. Check the largest inline body first.

   ```sql
   SELECT max(length(request_body)) AS max_inline_body_bytes
   FROM cliproxy.requests
   SETTINGS max_threads=1,max_execution_time=300,max_memory_usage=134217728;

   SELECT count() FROM cliproxy.requests
   WHERE length(request_body)>0 AND request_id NOT IN
     (SELECT request_id FROM cliproxy.request_facts)
   SETTINGS max_threads=1,max_block_size=1,max_memory_usage=268435456;
   ```

   Require zero underived legacy rows and `max_inline_body_bytes <= 16777216` with the shipped defaults. The tool repeats the maximum-length preflight, requires `query-memory-bytes >= 8 * max_inline_body_bytes`, and checks available server headroom. Do not raise the query cap without repeating the live-load qualification for the larger bodies and full response fields.

5. Save `SHOW CREATE TABLE cliproxy.requests`. Create a rollback table and attach each active request partition from the source. This uses hard links, keeps the original body bytes available, and avoids a full-body backup query. Run as an operator with the database permissions. Replace the month with each existing partition. Keep at least 10 GB free for diverging parts and merges.

   ```sql
   CREATE TABLE cliproxy.requests_legacy_backup AS cliproxy.requests;
   ALTER TABLE cliproxy.requests_legacy_backup
     ATTACH PARTITION 202611 FROM cliproxy.requests;
   ```

## Run and monitor

On the production Docker host for Portainer stack 29, use its `cpa-clickhouse_default` network and existing writer secret mount. Build and verify `cpa-local/clickhouse-deriver:v0.1.0-plana.9` from the released binary before running. The normal deriver remains on its own 384 MiB container. The one-off migration container uses 768 MiB for Go body splitting and verification. Do not pass secrets on the command line.

```sh
sudo -n docker run --rm --name cpa-body-migration-dry \
  --network cpa-clickhouse_default --read-only --memory 768m --cpus 0.5 \
  -e GOMAXPROCS=1 \
  -e CLICKHOUSE_URL=http://clickhouse:8123 \
  -e CLICKHOUSE_DB=cliproxy -e CLICKHOUSE_USER=cliproxy \
  -e CLICKHOUSE_PASSWORD_FILE=/run/secrets/ch_writer \
  -v /opt/cpa/clickhouse/secrets/ch_writer:/run/secrets/ch_writer:ro \
  cpa-local/clickhouse-deriver:v0.1.0-plana.9 migrate-bodies --dry-run

sudo -n docker run --rm --name cpa-body-migration \
  --network cpa-clickhouse_default --read-only --memory 768m --cpus 0.5 \
  -e GOMAXPROCS=1 \
  -e CLICKHOUSE_URL=http://clickhouse:8123 \
  -e CLICKHOUSE_DB=cliproxy -e CLICKHOUSE_USER=cliproxy \
  -e CLICKHOUSE_PASSWORD_FILE=/run/secrets/ch_writer \
  -v /opt/cpa/clickhouse/secrets/ch_writer:/run/secrets/ch_writer:ro \
  cpa-local/clickhouse-deriver:v0.1.0-plana.9 migrate-bodies
```

The dry run writes nothing. The real run saves its candidate snapshot and verified cursor in the database. It writes blobs, writes guarded skeleton copies, checks every reconstructed SHA-256, synchronously deletes verified inline copies, then commits the cursor. Repeating the same command resumes unfinished work. A completed run takes a new snapshot on the next invocation.

The memory admission check reserves 256 MiB of tracked headroom with its default 128 MiB query cap. RSS must stay below 1.375 GiB, and below the 2 GiB container limit minus the query cap. At RSS pressure the command purges unused jemalloc pages and waits five seconds before checking again. It never restarts ClickHouse. Sustained memory pressure pauses progress until the load subsides. Stop with SIGTERM if it remains stalled for ten minutes and inspect the live load.

Check the following with an operator account. Production has no `system.part_log` and its database reader and writer lack system metric grants unless explicitly added above.

```sql
SELECT name,value,last_error_time,last_error_message
FROM system.errors WHERE code IN (241,394,159);
SELECT database,table,mutation_id,is_done,latest_fail_reason
FROM system.mutations WHERE database='cliproxy' AND NOT is_done;
SELECT metric,value FROM system.asynchronous_metrics
WHERE metric IN ('MemoryResident','TrackedMemory','CGroupMemoryTotal','CGroupMemoryUsed');
SELECT request_id,started_at,done FROM cliproxy.body_migration_progress
ORDER BY saved_at DESC LIMIT 1;
```

Record `system.errors` counters before the run. Its code-241 count includes reader-limit failures and is not a live-insert error count. Take live-insert 241s from the CPA capture log, matching `clickhouse-sink` insert failures and `Code: 241`. Capture the log since migration start, preserve it before cleanup, and distinguish retries from lost rows. Check the ClickHouse server log too. The plugin now cuts both dedup and legacy queued inserts at 8 MiB. One oversized row runs alone.

Abort migration on any live-insert code 241, OOM kill, capture drop, repeated insert retries, incomplete reconstruction, SHA mismatch, failed mutation, or a growing fact backlog. SIGTERM leaves verified skeletons and unverified source copies available. Do not restart ClickHouse to force admission.

## Verify and roll back

Sample `SELECT body_status FROM cliproxy.raw_request(id='...')`. Require `complete`, no new duplicate IDs, no missing blobs, and no new `body_incomplete` facts. Use the audits in `queries.sql`. Deleted inline bytes remain on disk until background merges reclaim them. Do not force a full-table `OPTIMIZE FINAL` during capture.

Stopping migration is the immediate rollback. Readers support both formats, so completed batches need no reversal. Preserve the backup table until independent verification and an agreed retention period have passed.

To restore inline storage, disable new dedup writes, stop migration, and run `restore-bodies --dry-run`, then `restore-bodies` with the same grants and memory limits. This expands disk usage. Keep the new reader and deriver until restoration is verified. The backup hard links remain the recovery source if reconstruction cannot proceed. Do not automate backup reinsertion and skeleton deletion without isolating affected IDs and checking the restored hashes.

## Measurements and planning

See [the plana.9 qualification record](verification/plana9.md) for the production-shaped corpus, per-view memory bounds, concurrent load, fault injections, and byte-exact audit. Earlier [plana.8 numbers](verification/plana8.md) use uniform single-item bodies and do not qualify migration of production's large rows.

The synthetic qualification converted 8,000 historical rows in 1,345 seconds including upgrade and injected faults (5.95 rows/s). It observed 11 memory-admission waits and a 165-second progress gap across the deliberate outage. Plan additional time for the dry run and full audit. Production response fields and live load may differ, so this is not a production timing guarantee. Background disk reclamation is asynchronous and is not included. Keep the backup until independent acceptance passes.
