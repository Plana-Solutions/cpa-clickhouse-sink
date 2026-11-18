# v0.1.0-plana.3 acceptance

The implementation uses the accepted item-span design. `body-dedup` defaults off. Production has not been modified. The migration has only run against disposable databases.

## Local checks

Both modules pass `go test -race ./...` and `go vet ./...`. The deriver integration suite also passes against the shared local ClickHouse binary. A 60-second fuzz run completed 2,576,267 inputs without a failure. The earlier 30-second run completed another 2,678,559 inputs.

The live-shaped acceptance includes eight checked-in protocol fixtures, 36 live redacted rows fetched read-only, and an invalid UTF-8 body. The original 44 JSON-transportable rows contain 37,674,759 body bytes. Every body reconstructed byte-exact through `bodystore.Reader`, deriver fetching, and the SQL parameterized view. The 44 legacy and dedup fact rows matched after preserving ingestion order. Fact extraction depends on conversation order, so comparisons use equal original ingestion timestamps.

Compressed fixture storage after `OPTIMIZE FINAL` was 14,650,521 bytes for legacy requests and 1,055,597 bytes for dedup requests plus blobs. This is a single acceptance dataset, about 13.9 times smaller. Responses remain inline. The dedup fixture set additionally contains a five-byte invalid UTF-8 body, which legacy JSONEachRow cannot preserve.

The mixed-format migration converted 45 inline rows, including a row with no previous skeleton. Its rerun converted zero rows. A separate regression migrates a new skeleton with a 2 MiB residual, verifies its original bytes, and reruns idempotently. Missing or corrupt blobs return incomplete without returning partial content.

## Memory rehearsal

The server cap was 1,503,238,553 bytes, or 1.4 GiB. HTTP queries additionally used a 1 GiB limit, and the deriver retained its default 256 MiB query cap and existing batch/pass limits. The loop caught up 3,200 dedup backlog rows with ingestion timestamps fifteen minutes before startup, alongside the live-shaped fixtures. All 3,246 facts landed with unique request IDs. Peak logged query memory across the entire acceptance run was 232,527,487 bytes, about 222 MiB. This includes legacy baseline and migration queries, rather than only dedup lookups. During backlog catch-up alone, the peak was 19,775,627 bytes, about 18.9 MiB. These final-commit measurements use only query-log entries after the run and backlog start times.

The run finished in 152.89 seconds, including both fact rebuilds, migration, and backlog catch-up. This is acceptance evidence, not a latency or throughput comparison. Other local agents were active. The test directory stayed below 0.4 GiB, and Data-volume free space stayed above 9 GiB.

## Isolated host check

The verifier used `cpa-local/image-results:v8.0.77-a5e8f69`, ClickHouse 25.8, a fake upstream, and an internal Docker network. Its output was:

```text
request status=200 stream=False
request status=200 stream=True
request status=200 stream=False
Dedup production-image byte-exact SQL reconstruction verified. rows=3
Production-image load verified. requests=3 usage=3 request_facts=3
Released deriver verified in stock distroless image with mounted password file.
plugin_name=clickhouse-sink version=0.1.0-plana.3
body blobs written=1 skipped=2
```

The verifier removed every container and its network. The build used disposable host source and output directories. No production service, config, stack, or network policy changed.

## Failures retained as regression coverage

Real ClickHouse testing caught alias substitution in the hash lookup and migration filter, the 25.8 requirement to put comments before codecs, and the HTTP limit on large form values. The final implementation uses distinct projection aliases, a prefiltered migration source, an explicit skeleton existence check before deletion, compatible DDL ordering, and RowBinary external files for skeleton uploads. The large-residual regression and new-skeleton migration tests cover the delete-safety failure.

Rollout instructions and config are in [BODY_STORAGE.md](../BODY_STORAGE.md). Deploy dual-format deriver and UI/export readers first, then enable the plugin flag, then run migration. Blob partition retention cannot follow request partition retention directly. External independent review is owned by the parent workflow; this implementation agent did not spawn reviewers.

The initial native ARM64 workflow passed vet and race tests but failed its ABI assertion because the release verifier still expected the old plugin version. The assertion and registry now name v0.1.0-plana.3.

Automated PR review found two retry gaps after the first publication. The final release pins each retry unit to its original destination, scopes the acknowledged-hash cache to the destination, and invalidates schema state after body-write failures. Regression tests fail a request insert, change the destination, and verify old retries stay on the old server while the new server receives its own blobs. The v0.1.0-plana.3 tag and assets were refreshed before handoff.
