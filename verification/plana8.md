# Plana.8 body lookup and migration qualification

The measurements use ClickHouse 25.8.33.6, pinned image digest `0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77`, on the cluster. No production data was fetched and no production settings or rows were changed. Production's configuration was inspected with read-only Portainer GET requests.

## Lookup bounds

The fixture contains 5,760 synthetic legacy rows, 3,020,146,560 inline body bytes and about 2.27 GB on disk. Each request has a SHA-256 request ID, a 512 KiB JSON body, and a distinct conversation. The bodies are incompressible random base64 inside a valid Claude message. The primary key and granularity are the shipping schema's defaults. This avoids the misleading locality of an earlier sequential-ID fixture.

Twenty alternating old/new lookups used the same random IDs and a 512 MiB reader cap. All returned complete bodies. The original production code-241 failure did **not** reproduce in this fixture. These results prove bounded reads on this corpus, not reproduction of the intermittent production failure.

| Counter per lookup | Old median and range | New median and range |
|---|---|---|
| Peak query memory | 29,736,113 B, 7,714,703 to 29,870,233 | 6,556,626 B, 6,554,022 to 6,559,309 |
| `query_log.read_bytes` | 20,754,626 B, 1,878,134 to 20,754,626 | 829,840 B, 829,552 to 829,840 |
| File-buffer read bytes | 15,823,920 B, 2,579,951 to 15,948,310 | 5,591,638 B, 1,659,416 to 8,344,211 |

File-buffer bytes are `ProfileEvents['ReadBufferFromFileDescriptorReadBytes']`. They include compressed payload reads served by the kernel page cache. `OSReadBytes` was zero for these warm reads. `read_bytes` alone understates PREWHERE payload work, so it must not be presented as physical disk I/O or a throughput speedup. Latency is omitted because the shared two-core host had other work in flight.

A second set of twenty alternating lookups disabled automatic PREWHERE movement. The new view still passed every lookup, with a 6,505,109-byte memory peak and a 7,737,065-byte file-buffer read peak. Its explicit PREWHERE does not depend on the optimizer choosing to move the ID filter.

Five rounds exercised `bodystore.Reader.Read`, 16-ID `ReadBatch`, and the deriver's 16-ID `fetch`. SHA-256 matched for every returned body. Peak query memory was 29,095,151 B for single-row reader queries, 44,276,694 B for batch reads, and 44,534,964 B for deriver fetches. Peak `read_bytes` was 9,964,365 B, 19,928,730 B, and 19,931,642 B respectively. The Go reader already filtered request IDs in PREWHERE; blob payload reads now do so explicitly too.

## Concurrent migration

The qualification server has a 2 GiB container limit, one CPU, a 0.75 server memory ratio, 64 MiB mark cache, no uncompressed cache, four background merge workers, merge_max_block_size=256 and a 2 GiB maximum merge size. Warning-level logging and the disabled system logs match production. No ClickHouse restart occurs during qualification.

Earlier trials left default system logs enabled. They eventually stalled as jemalloc allocations grew beyond tracked memory, despite allocator purges. Those trials are not production-parity qualification. Production's disabled text, trace, metric, asynchronous metric, query-thread and processor-profile logs were confirmed by inspecting its mounted configuration. A fresh disposable server was then seeded with the full corpus again.

The live test uses the real sink queue with dedup enabled, a shared large context and one unique new item per request. The writer has only database grants. The real deriver runs with its production flags. Historical facts for the first 4,096 rows are seeded as metadata to admit migration candidates; the normal deriver processes the remaining historical backlog and live requests. The migration user has only database grants plus the documented metric reads and jemalloc purge privilege. Migration uses the 64 MiB default query cap, 16 rows and 8 MiB per batch.

The final window ran for 607.23 seconds. It captured 597 live requests and completed 3,225 verified historical conversions, about 5.31 migrated rows/s overall. Approximate net rates in consecutive two-minute windows were 3.2, 6.1, 6.1, 5.9 and 5.3 rows/s. Progress continued in every window. This is one sustained capacity qualification, not a claim of an optimal throughput ceiling.

The five-second sampled RSS peak was 1,317,179,392 B, about 1.23 GiB. Peak sampled tracked memory was 466,602,810 B, about 445 MiB. The server's code-241 counter remained 12 throughout the window. Those twelve preparation errors preceded live load and included an accidentally unbounded fixture-summary query and seeding merges. There were zero live insert or transport errors, zero buffered rows at completion, zero drops, zero duplicate live IDs, zero missing blobs and zero incomplete facts. The first and last live bodies matched byte-exact; twenty further view samples matched their stored hashes. RestartCount stayed zero and OOMKilled stayed false.

The raw fixture has one large unique item per historical request. Migration therefore does not reduce its storage materially. That is a deliberate request-read and merge-memory stress case. It does not estimate production's dedup ratio. The shared host had two CPUs; ClickHouse, the deriver and migration each had a one-CPU cap. Work consists of serial bounded SQL batches, synchronous deletion and a 250 ms pause per batch. Host scheduling and storage also affect the extrapolated duration; this run does not establish which component sets maximum throughput.

The load test checks sink failures, code 241, buffer drain, drops, and byte-exact reconstruction of the first and last live requests. Migration verifies every converted request before deletion. The recorded migration count excludes newly captured dedup rows.

## Repeat the checks

The committed tests require an isolated, seeded server and must never target production.

```sh
LARGE_BODY_URL=http://ISOLATED:8123 go test -run TestLargeBodyLiveLoad -v .
(cd deriver && LARGE_BODY_URL=http://ISOLATED:8123 go test -run TestLargeBodyReadPaths -v .)
```

`TestLargeBodyReadPaths` runs before migration while the selected seed IDs still contain inline bodies. The fixture users are `database_only`, `migration` and `measure`, all passwordless inside an isolated internal Docker network. Never create these users on production.

Raw synthetic measurements, setup scripts, invalid-trial notes and the decision trail are retained outside the repository at `/Users/minoo/.p3stack/runs/cpa-session-archive/plana8-implementation/`. The setup scripts show the fixture generation and Docker settings. The cluster containers, anonymous volumes, network, build image and temporary files are removed after verification.

## Safety and remaining limits

The SQL view uses request-ID PREWHERE, digest-based copy preference, and one-row payload blocks. It no longer sorts on the body column. Both dedup and legacy outage queues are cut at 8 MiB, with one oversized row allowed alone. A 40-row outage self-check covers both formats.

Synchronous deletes use a five-minute statement limit and retry codes 394 and 159. ClickHouse 25.8 was checked to honor the statement timeout over the HTTP client's shorter default. A cancelled delete retry is covered by a unit test.

The production XML grants were tested on the isolated server. A database-only migration dry run returned ACCESS_DENIED and refused safely. Production has no system.part_log; the runbook uses system.errors and server logs instead.

The fixture is synthetic and request-heavy. It does not qualify every production body size or response distribution. The pre-existing deriver pass deadline can still time out on a large backlog; later passes continue. Independent review of these changes remains separate from implementation and CI.
