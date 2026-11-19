# plana.10 migration qualification

This release replaces the global maximum-body refusal with an inclusive per-row limit of `query-memory-bytes / 8`. The default remains 128 MiB per query and 16 MiB per inline body. Oversized inline rows remain untouched and appear in skip counts, byte totals, and request IDs on every invocation. Logs contain only IDs and sizes.

The preflight projects IDs and lengths with `max_threads=1,max_block_size=1`. Snapshot and dry-run queries exclude the reported IDs in PREWHERE before reading the body column, then apply the size bound. ClickHouse 25.8 evaluates `length(String)` by reading String data. The preflight therefore still reads oversized strings in one-row blocks. It does not return them to Go. This release makes no claim that all body-column IO disappears.

## Isolated regression

`TestMigrationSkipsOversized` runs against ClickHouse 25.8.33.6, image digest `0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77`, on `ssh cluster`. The server container has a 2 GiB limit. No production endpoint or container is used.

The fixture has 44 rows. Boundary bodies contain exactly 16,777,216, 16,777,217, and 34,092,805 bytes. One other row contains 1,024 bytes, and 40 normal rows use the existing chat fixture. Setup uses a 512 MiB query cap to seed and derive the oversized rows. All migration operations use 128 MiB.

The test checks dry-run accounting and unchanged source rows, SIGKILL after skips and a saved progress cursor, exclusion of oversized IDs from the snapshot, resume with oversized candidates deliberately added to simulate an unfinished plana.9 snapshot, reconstruction of eligible bodies, SHA-256 identity of skipped inline bodies, `done=true`, and a second run that migrates zero rows. The two skips total 50,870,022 bytes.

`TestMigrateNewSkeleton` also passes with two exactly 16 MiB rows, including a 2 MiB residual, dry-run projected-byte equality, reconstruction and a second no-op run.

The released plana.9 binary reproduced the original failure on a separate isolated 34,092,805-byte row:

```text
migration requires query-memory-bytes >= 8 * max(length(request_body)); increase the cap and recheck server headroom
```

## Evidence and limits

Raw logs and query-log measurements are retained in `/Users/minoo/.p3stack/runs/cpa-session-archive/plana10-implementation/`. Initial fixture seeding at 256 MiB failed with code 241. An early audit using the inline reader at 128 MiB also failed on the skipped 34 MB row. The final audit hashes skipped rows inside ClickHouse with one-row blocks and compares them with the original client SHA-256 values. Eligible rows are reconstructed in Go and compared with their original bodies.

This is a focused migration regression, not a repeat of the plana.9 live-load soak. The known blob-lookup URL limit above approximately 3,100 distinct protocol items remains. The runbook requires at most 3,000 items in eligible rows. The supplied production measurement of 843 satisfies that precondition. The reader-view limits documented in the independent plana.9 review also remain. Production installation and migration are outside this release.

## Final focused result

The mixed-corpus test passed in 65.80 seconds. It accounted for 42 eligible rows and 16,795,640 inline bytes in the dry run, with 16,788,023 projected uncompressed bytes. All 44 original SHA-256 values matched after SIGKILL/resume. Exactly two rows and 50,870,022 bytes stayed inline. The durable cursor reached `done=true`, and the second run converted zero rows.

The isolated server's query log recorded a maximum of 120,302,001 bytes across 736 completed fixture queries explicitly capped at 134,217,728 bytes. This includes earlier successful test attempts and audits, not only the final run. The two retained code-241 exceptions were fixture seeding and the superseded inline-reader audit described above. The final snapshot used 19,222,867 bytes of tracked memory and selected 16,797,762 bytes, comprising eligible bodies and metadata. It excluded the 50,870,022 oversized body bytes. The final no-op snapshot selected 5,948 metadata bytes. These are query allocation and selected-byte counters, not server RSS or disk-device IO measurements.

A repeat of the mixed-corpus test passed in 35.07 seconds after updating the obsolete global-refusal unit test to assert skip reporting. Vet and unit tests passed in both Go modules. CI runs the mixed-corpus regression alongside the existing memory-sensitive migration tests on its fresh ClickHouse 25.8 server.
