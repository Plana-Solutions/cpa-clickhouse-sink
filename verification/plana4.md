# v0.1.0-plana.4 verification

Released source is `e64a23254cb0032873da20fb2c64640f3a311071`.
Release is https://github.com/Plana-Solutions/cpa-clickhouse-sink/releases/tag/v0.1.0-plana.4.
No production writes or production migration ran. No live bodies were fetched. All test requests are synthetic.

## Findings

| Finding | Result | Implementation commit |
|---|---|---|
| 1 | Fixed. Retain the source unit until requests commit. Config changes reprepare blobs and rows at the current destination. Usage and legacy batches flush before body retries. | 28e973d |
| 2 | Fixed. Missing or corrupt bodies produce metadata-only body_incomplete facts and the pass continues. Rebuild validates all dedup bodies before truncation. Poison-row test includes a missing blob and asserts rebuild preserves existing facts. Stop capture, migrations, GC and the loop during rebuild. | 28e973d, e64a232 |
| 3 | Fixed. List candidate IDs and timestamps once, read bodies by time and ID, query only this body's blob keys, and prefer skeletons using a digest filter before reading inline body columns. | 28e973d, b63ceda |
| 4 | Fixed. Reconfigure holds only the short configuration mutex. Cache reset belongs to the flush goroutine. Regression checks cover both flag values during a blocked flush. | 28e973d |
| 5 | Fixed. Preloads have a 1.5 second deadline within a three second flush budget. Failed loads do not populate the cache. Acknowledged inserts can populate it. | 28e973d |
| 6 | Documented. DDL remains. Both writers need schema grants, with ALTER DELETE for migrations. Existing production ALL ON cliproxy.* covers them, per the supplied review resolution. Production grants were not changed. | 28e973d |
| 7 | Outside this task. Archive repository was not touched. | none |
| 8 | Fixed. Dry run deduplicates new items across rows. restore-bodies verifies an inline copy before deleting its skeleton, supports reruns and arbitrary bytes, and refuses incomplete bodies. OPTIMIZE FINAL and rollback disk requirements are documented. | 28e973d |
| Low cache | Fixed. 16 MiB accounting cap, charging 64 bytes per hash and 256 per lineage, with existing lineage/count/idle limits. Acknowledgements stop adding keys at the byte budget. | 28e973d |
| Low lineage | Fixed. Use installed gjson to inspect only lineage strings. A non-string metadata.user_id can fall back to prompt_cache_key. | 28e973d |
| Low deletion race | Documented. Stop writers and discard caches before deleting a conversation. Idle time does not protect resumed sessions. | 28e973d |
| Low CI | Fixed. Both modules' race tests, vet and both integration suites run against digest-pinned ClickHouse 25.8 with a 1503238553-byte container limit. | 28e973d, c286458 |
| Low release identity | New tag and pinned asset digests below. All older tag refs verified unchanged. Workflow refuses to overwrite an existing release. | 28e973d |

## ClickHouse 25.8 read volume

Cluster Docker reported 25.8.33.6. Its image digest is `sha256:0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77`.
The throwaway server had a 1503238553-byte memory cap and two CPUs. The cluster had two CPUs and a load average of 3.10 before measurement. Other workloads remained running. These are read-volume measurements, not elapsed-speed claims.

One functional run per binary and size used repeated synthetic 512 KiB bodies, inserted in four-row sorted parts with production table settings. Before is bfd9118; after uses the migration and reader code in b63ceda, unchanged in the release. Each migration completed and verified reconstructed bodies before deleting inline copies. Full per-step counts, totals, and maxima are in [plana4-migration-read-bytes.json](plana4-migration-read-bytes.json). Reproduce with scripts/measure_migration.py and separately built before/after deriver binaries.

`system.query_log` QueryFinish read_bytes counts logical query reads, including INSERT SELECT. It excludes asynchronous mutation-worker and background merge I/O. Wide-part granule reads bound the remaining inline-copy cost, with observed maxima below 10 MB per inline read. The candidate scan is once per run. Metadata and the one-million-key dry-run cap bound retained state; migration holds one full body at a time.

| Rows | Before total bytes | After total bytes | Before candidate bytes | After candidate bytes |
|---:|---:|---:|---:|---:|
| 32 | 1,292,793,083 | 366,736,460 | 503,942,260 | 16,780,064 |
| 64 | 2,874,086,913 | 842,969,340 | 1,441,012,216 | 33,560,128 |
| 128 | 7,272,321,940 | 1,706,595,734 | 4,368,192,759 | 67,120,256 |

128-row read bytes per step follow. Delete query reads are zero in QueryFinish; mutation-worker reads are excluded.

| Step | Before calls | Before bytes | After calls | After bytes |
|---|---:|---:|---:|---:|
| blob_read | 129 | 67,524,831 | 129 | 67,276,971 |
| candidates | 129 | 4,368,192,759 | 1 | 67,120,256 |
| delete | 2 | 0 | 2 | 0 |
| inline_read | 128 | 785,569,896 | 128 | 785,569,896 |
| insert | 129 | 1,264,777,286 | 129 | 786,194,704 |
| known_keys | 128 | 4,064 | 128 | 4,192 |
| reader | 128 | 785,822,197 | 128 | 279,774 |
| verify | 135 | 430,907 | 135 | 149,941 |

## CI and native acceptance

Both PR runs passed on released source.

- https://github.com/Plana-Solutions/cpa-clickhouse-sink/actions/runs/44388759945
- https://github.com/Plana-Solutions/cpa-clickhouse-sink/actions/runs/44388744555
- Native ARM64 release and ABI check passed at https://github.com/Plana-Solutions/cpa-clickhouse-sink/actions/runs/44389278296.

CI reported 10 byte-exact bodies, eight legacy/dedup fact comparisons, and 3,200 backlog rows. Peak query memory was 21,734,710 bytes under the container cap. The body integration test passed in 51.02 seconds. Migration/restore reruns, binary and empty bodies, poison-row continuation, incomplete restore refusal and rebuild refusal ran in the suites. Local vet and race checks passed in both modules. The local full body run initially failed an outdated post-rollback missing-blob assertion; the corrected assertion passed in CI. Initial CI health-command quoting failed before tests; corrected runs above passed.

The released ZIP and deriver ran on cluster in a throwaway internal Docker network with ClickHouse 25.8 and exact CPA image `cpa-local/image-results:v8.0.77-a5e8f69`, local image ID `sha256:dc4d2f44086747d68311303a384cf88c4efac5f9275cc211da41fc7b4b9c74e8`.

The test first revoked only blob insertion, leaving usage writes working, and observed a queued failed body unit. It then changed the throwaway ClickHouse password, restored blob insertion, and hot-reloaded the corrected plugin config. All nine real client requests and usage rows arrived without a CPA restart. Every captured body matched its expected SHA-256 through raw_request. After deleting blobs, a new incomplete skeleton produced body_incomplete facts, and a subsequent valid inline row was derived. Total facts were 11. Output is [plana4-host.log](plana4-host.log).

The first host setup attempt failed because the disposable admin lacked CREATE USER. The harness now enables access management only in its throwaway ClickHouse. The harness change also ensures a body unit is already failing before rotating credentials, so the test proves retry recovery rather than merely delayed admission. The final harness is newer than the released source; the tested plugin and deriver are the unmodified released assets.

## Immutable asset pins

[plana4-SHA256SUMS](plana4-SHA256SUMS) contains every asset digest. The downloaded ZIP contains only clickhouse-sink.so, byte-identical to the standalone asset. All five manifest entries passed verification and matched GitHub's reported asset digests.

```
SHA256SUMS  4b9d70e37a38b6af692b9551aa7681106f491f7ca4b85ea4df78836e241d4c82
ZIP         28163c090bef807cf0a96d2aeeb17c2b12b27102500c225a7f19ccd14049538a
plugin.so   d19eadbddd1c17f611c36395e4265985d6029cca40d732a5069f868be64c8e7f
deriver     c86077ef4331d5f51d18ee3c1036d93fe41ee11e69eeac3f0a43ce9976d7fd9d
```

Verify the manifest against the pinned SHA256SUMS digest above before trusting its contents. No tag was moved and no existing release asset was replaced.

## Remaining limit

The release tag and artifact names are .4, but plugin.register still reports `0.1.0-plana.3`. This registration string was missed before tagging. Host loading and capture passed with that string. The .4 tag remains fixed; use the asset digests and source commit above to identify the actual build. A future release must correct the registration version without replacing .4 assets.

Rebuild preflight requires stable input. Dry-run refuses more than one million new keys. Rollback expands inline storage and needs merge headroom. No fresh independent review was run by this implementation sub-agent.
