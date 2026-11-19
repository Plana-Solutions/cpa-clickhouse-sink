# Plana.9 qualification

The implementation is `dd5d9e26da1b8bd3df2f63cdb607862f34354718`. All data is synthetic. No production rows, secrets, configuration, or deployments were changed. A read-only Portainer stack-file request supplied the production network, deriver environment, resource limits, and password-file mount for the runbook.

## View memory

Both SQL copies use reference rows with their ordinal, a join bounded by the request's lineage and keys, per-blob SHA-256 checks, ordered `groupArray`, and `arrayStringConcat`. One SHA-256 verifies the reconstructed candidate. The residual is a scalar subquery so the reference join does not replicate it. Complete digest-bearing copies retain priority. An incomplete skeleton can fall back to a present inline copy.

Five alternating old/new runs per shape used ClickHouse 25.8.33.6, pinned digest `0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77`. These are memory measurements, not a throughput comparison. The test server had query logging enabled for peak-memory evidence. The separate capacity server kept query logging off.

| Shape | Before | After median and range | Reader cap |
|---|---|---|---|
| 385 references, 3,273,545 bytes | 5/5 code 241, attempted 1.19 GiB | 27,103,481 B, 27,103,209 to 30,812,569 B, 5/5 byte-exact | 64 MiB |
| 72 references, 15,724,934 bytes | 5/5 code 241, attempted 1.11 GiB | 111,330,180 B, 111,330,020 to 115,082,292 B, 5/5 byte-exact | 128 MiB |

The failed queries' recorded allocation peaks exclude the rejected giant allocation. The before column reports ClickHouse's attempted allocation from its exception, rather than presenting the smaller recorded peak as a successful read. The 15.7 MB shape remains larger than a 64 MiB query budget. The new bound is linear in body size.

`TestViewManyReferences` also verifies valid JSON bodies, exact digests, reference counts, and incomplete-skeleton inline recovery through the SQL view, `Reader.Read`, and `Reader.ReadBatch`. Its additional case combines a 1 MiB residual with 385 references under 64 MiB. The test passed against the final source.

The deriver fetch, bodystore reader, migration verification, and restore paths reconstruct in Go with `bodystore.Body.Restore` and `Join`. Their SQL only projects bodies and reference metadata or loads blobs by bounded lineage/key sets. Their remaining lambdas transform their own elements and capture no large arrays. The deriver propagates transport and stream failures from inline recovery. Migration verification uses `ReadSkeletonBatch`, which excludes legacy rows and clears inline payloads before reconstruction. A missing blob therefore cannot pass verification through the recovery copy. The regression first failed with `migration accepted a missing blob while an inline copy was present: <nil>`, then passed with strict verification.

## Migration defaults and tests

Migration and restore default to 128 MiB per query. Snapshot and dry-run candidate selection use `length(request_body)>0`. Migration preflight runs `max(length(request_body))` and refuses a body above one eighth of the configured query cap. This documents and enforces the shipped 16 MiB inline-body limit before the candidate snapshot or payload reads.

`TestMigrateNewSkeleton` passed with two exactly 16 MiB bodies, a 2 MiB residual outside protocol items, dry-run accounting, real migration, reconstruction, deletion checks, and a second no-op migration. It explicitly uses the shipped 128 MiB cap. `TestFetchIncompleteSkeletonFallsBackInline` passed. `TestPreloadGetsFreshDeadlineAfterSlowUnit` confirms a new lineage still preloads after a preceding insert takes more than three seconds.

Both Tests workflows passed on the final implementation commit. They run both modules with the race detector and real ClickHouse 25.8 integration tests. Local vet and module tests also passed. The long local body integration test passed separately; its mixed-source schema comparison failed when the source SQL was updated after its binary had compiled. A fresh focused run then passed the schema comparison and final view tests.

## Capacity corpus and limits

The cluster corpus has 8,000 historical rows and 2,660,580,640 inline request bytes. The median is 333,035 B, p99 is 7,342,091 B, and maximum is 15,730,955 B. There are 123 rows above 5 MiB. Every historical row has 72 protocol items and response chunks. The response chunks are small synthetic SSE chunks, not a reproduction of production's response-size distribution. Random base64 inside valid JSON prevents compression from hiding request-body IO. Historical fact metadata admits the migration snapshot; the real deriver processes live captures.

The capacity server has a 2 GiB container, one CPU, a 0.75 server memory ratio, 64 MiB mark cache, no uncompressed cache, four background workers, `merge_max_block_size=256`, and a 2 GiB merge cap. Warning-level logging and disabled system logs, including query_log, match production. The normal deriver uses 384 MiB and 0.5 CPU with production batch, query-memory, and interval flags. Migration uses 768 MiB and 0.5 CPU, its shipped 128 MiB query limit, 16 rows, and an 8 MiB batch budget. The Go processes use `GOMAXPROCS=1`. A hard-link backup of the entire historical partition is created before migration, as required by the runbook.

The real CPA image is `cpa-local/image-results:v8.0.77-a5e8f69`. Requests stream at about 0.4 per second with a shared 320 KiB context and a unique final item. `body-dedup` stays enabled. Host logs identify plana.8 as retired and plana.9 as active. The final built plugin passes native ABI registration as version `0.1.0-plana.9`.

## Recovery evidence and retained failures

The logs retain every setup and measurement failure. A dynamically linked test deriver could not start in the distroless image and was replaced by a static build. The first integration attempt shared the fault-injection endpoint and failed during the outage. Tests then moved to their own disposable server and internal network. The test server needed query logging for its existing measurement queries; the capacity server retained production's disabled logs throughout.

The initial outage used network disconnect. Its explicit-IP reconnect command failed, and automatic reconnect restored the original IP. That capture window and the subsequent resume are retained as separate segments. A literal `SHOW CREATE` assertion also failed on ClickHouse's escaped newlines, although the view was correct. The assertion now normalizes whitespace. The final built artifacts repeat the three fault injections with a stop/start outage that preserves the endpoint. After adding strict verification, a fresh server and a newly seeded 8,000-row corpus replace the candidate trial. Only that clean final run supplies the release qualification numbers. The earlier trials remain in the evidence directory.

The XML permissions test uses a single read-only bind mount. Missing grants return code 497. A same-inode write reloads within three seconds. All documented metric, settings, errors, mutations, and purge grants pass as the restricted user.

Raw logs, corpus setup, decision trail, and rerunnable scripts live under `/Users/minoo/.p3stack/runs/cpa-session-archive/plana9-implementation/`. The full audit test is `deriver/TestQualificationAudit`. It compares every historical body's original SHA-256, every successful live request's client-side SHA-256, and all historical response chunks. It rejects duplicate IDs, missing bodies, and body_incomplete facts.


## Final capacity result

The final clean fixture migrated all 8,000 historical rows. The last resumed invocation exited 0 at `2026-11-19T18:01:33.758846963Z`, reporting 7,961 rows, 2,374,239,091 inline bytes, and 46,202,855 projected uncompressed bytes. Earlier fault segments account for the other 39 rows. The final table has 8,507 rows and 8,507 distinct request IDs, zero inline bodies, and zero requests without facts. All 507 live clients completed successfully.

Elapsed time from the saved monotonic start to Docker's migration completion was 1,345.16 seconds, including upgrade and fault injection. This is 5.95 historical rows/s or 6.63 GiB/hour of original inline bytes. The harness was interrupted before writing `qualification-end.json`; completion is independently anchored by Docker's exit timestamp and the durable `done=true` cursor. The live manifest covers 1,340.84 seconds. The timing conversion uses the cluster's realtime-minus-monotonic offset measured at pickup.

Five-second sampling recorded peak ClickHouse RSS of 1,567,821,824 B (1.460 GiB) and tracked memory of 665,506,869 B (635 MiB). The post-restart process high-water RSS at pickup was 1,575,472 KiB (1.503 GiB), which also includes the beginning of the independent audit. The container cgroup peaked at its 2 GiB limit, including page cache, with zero OOM events and zero OOM kills. These are separate RSS and cgroup measurements, not interchangeable limits. All retained Docker states report `OOMKilled=false`.

The final migration log contains 11 memory-headroom waits, including waits around the image-heavy and late merge phases. The longest interval without an observed changed row count was 165.1 seconds across the injected 150-second outage; the sampler could not query the stopped server during that interval. Outside that outage, the largest observed progress gap was 40.1 seconds during startup and 15.1 seconds after migration was progressing. These samples do not measure individual mutation latency. There was no ten-minute stall.

The complete CPA log has zero live-insert code-241 failures and zero capture drops. Four retained retry messages occurred during the injected ClickHouse faults and outage. All live bodies are accounted for after recovery. The clean run retained the migration SIGKILL, ClickHouse SIGKILL, and 150-second stop/start outage evidence. Production was not accessed during pickup.

The independent `TestQualificationAudit` passed in 48.55 seconds against the retained cluster fixture. It verified 8,000/8,000 original historical body SHA-256 values, 507/507 successful live client body SHA-256 values, and all historical response chunks. It reported zero response mismatches, duplicate request IDs, missing bodies, and body_incomplete facts. A separate anti-join over every distinct `(lineage, body_ref)` found zero missing blob references. Pickup evidence is preserved in `pickup-audit.log`, `pickup-summary.json`, `pickup-timing.json`, `pickup-blob-audit.json`, and `pickup-final/` in the evidence directory named above.

Publication uses the existing tag-triggered ARM64 workflow and registry/store identity `0.1.0-plana.9`. The workflow verifies both modules, native ABI registration, ZIP layout, and checksums before uploading assets. Publication does not install the release. Production installation, production migration, and independent review of the new implementation remain outside this qualification.
