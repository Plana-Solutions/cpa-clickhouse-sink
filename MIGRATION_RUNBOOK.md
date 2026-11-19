# Migrate existing inline bodies on the 2 GB production server

This procedure targets ClickHouse 25.8 with a 2 GiB container, a 0.75 server memory ratio, a 64 MiB mark cache, four background merge workers, `merge_max_block_size=256`, and a 2 GiB maximum merge size. New capture already uses dedup. Publication does not apply these steps to production.

## Prepare permissions and a rollback copy

1. Verify the plana.8 assets against `SHA256SUMS`. Deploy the new reader view and deriver before migration. Keep the existing 16-row, 8 MiB batch limits and 256 MiB query cap.
2. Confirm normal capture and derivation work without code 241 or OOM kills. Stop any other migration or restore command. Keep capture and the normal deriver running.
3. An operator must add the following entries to the existing migration user's `users.d` XML grants. Keep its existing `GRANT ALL ON cliproxy.*` entry. These grants are required only by migration and restore. The normal reader needs no system grants.

   ```xml
   <query>GRANT SELECT(metric,value) ON system.asynchronous_metrics</query>
   <query>GRANT SELECT(name,value) ON system.server_settings</query>
   <query>GRANT SYSTEM JEMALLOC PURGE ON *.*</query>
   ```

   Reload the XML and test those privileges as that user. Do not put a password in shell arguments or logs. A database-only user fails safely at the memory preflight. A missing purge privilege fails safely if memory pressure later requires a purge.
4. Confirm that every legacy row has a fact. The command only snapshots derived rows. This reads the body column once, so keep blocks small.

   ```sql
   SELECT count() FROM cliproxy.requests
   WHERE request_body!='' AND request_id NOT IN
     (SELECT request_id FROM cliproxy.request_facts)
   SETTINGS max_threads=1,max_block_size=1,max_memory_usage=268435456;
   ```

5. Save `SHOW CREATE TABLE cliproxy.requests`. Create a rollback table and attach each active request partition from the source. This uses hard links, keeps the original body bytes available, and avoids a full-body backup query. Run as an operator with the database permissions. Replace the month with each existing partition. Keep at least 10 GB free for diverging parts and merges.

   ```sql
   CREATE TABLE cliproxy.requests_legacy_backup AS cliproxy.requests;
   ALTER TABLE cliproxy.requests_legacy_backup
     ATTACH PARTITION 202611 FROM cliproxy.requests;
   ```

## Run and monitor

Use the deriver's existing environment and password-file mount. Do not pass secrets on the command line.

```sh
clickhouse-deriver-linux-arm64 migrate-bodies --dry-run
clickhouse-deriver-linux-arm64 migrate-bodies
```

The dry run writes nothing. The real run saves its candidate snapshot and verified cursor in the database. It writes blobs, writes guarded skeleton copies, checks every reconstructed SHA-256, synchronously deletes verified inline copies, then commits the cursor. Repeating the same command resumes unfinished work. A completed run takes a new snapshot on the next invocation.

The memory admission check reserves 384 MiB of tracked headroom with the default query cap. RSS must stay below 1.375 GiB, and below the 2 GiB container limit minus the query cap. At RSS pressure the command purges unused jemalloc pages and waits five seconds before checking again. It never restarts ClickHouse. Sustained memory pressure pauses progress until the load subsides. Stop with SIGTERM if it remains stalled for ten minutes and inspect the live load.

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

Record `system.errors` counters before the run so old errors do not count as new failures. Check the ClickHouse server log and the capture log too. The plugin now cuts both dedup and legacy queued inserts at 8 MiB. One oversized row runs alone.

Abort migration on any live-insert code 241, OOM kill, capture drop, repeated insert retries, incomplete reconstruction, SHA mismatch, failed mutation, or a growing fact backlog. SIGTERM leaves verified skeletons and unverified source copies available. Do not restart ClickHouse to force admission.

## Verify and roll back

Sample `SELECT body_status FROM cliproxy.raw_request(id='...')`. Require `complete`, no new duplicate IDs, no missing blobs, and no new `body_incomplete` facts. Use the audits in `queries.sql`. Deleted inline bytes remain on disk until background merges reclaim them. Do not force a full-table `OPTIMIZE FINAL` during capture.

Stopping migration is the immediate rollback. Readers support both formats, so completed batches need no reversal. Preserve the backup table until independent verification and an agreed retention period have passed.

To restore inline storage, disable new dedup writes, stop migration, and run `restore-bodies --dry-run`, then `restore-bodies` with the same grants and memory limits. This expands disk usage. Keep the new reader and deriver until restoration is verified. The backup hard links remain the recovery source if reconstruction cannot proceed. Do not automate backup reinsertion and skeleton deletion without isolating affected IDs and checking the restored hashes.
