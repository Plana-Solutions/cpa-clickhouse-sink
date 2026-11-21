# Sink redaction speed

Determine whether large captured requests make the live plana.9 sink unsafe, and prepare a fix without deployment.

## Verdict

The default regex build spends tens to hundreds of seconds redacting a new multi-megabyte image segment. Building with `gore2regex` reduces this cost while preserving the tested outputs. Keep image detection enabled. A synthetic AWS credential consisting entirely of valid base64 characters is detected inside an image data URL by both engines.

## Measurements

| Body | Stdlib seconds, median [range] | gore2regex seconds, median [range] | Samples per engine |
|---|---:|---:|---:|
| image_5MiB | 24.599852 [24.112338, 25.892012] | 0.724274 [0.683256, 0.877361] | 5 |
| image_20MiB | 109.096999 | 2.354911 | 1 |
| image_34MiB | 185.130276 | 3.830517 | 1 |
| cached_text_1MiB | 0.001244 [0.001068, 0.001311] | 0.003365 [0.003136, 0.003458] | 5 |
| cached_text_5MiB | 0.005368 [0.005238, 0.005613] | 0.006870 [0.006735, 0.007155] | 5 |

Measured on an Apple M4, darwin/arm64, Go 1.27.1. The host had 10 logical CPUs and an initial load average around 3.6. These are full `redactRequest` timings on the exact plana.9 commit `f33482b`, compiled twice with identical source and dependencies. Only the `gore2regex` build tag differs. The redactor implementation is also unchanged on the PR's main base.

The image cases contain one new message with a deterministic random base64 payload of the stated encoded size and a synthetic pattern secret in its text. These are scanner workloads, not decodable PNG fixtures. The history cases prime the cache with unique messages until the body exceeds the stated size, then append one new message with a synthetic secret. Each measured invocation has a fresh redactor and, for text, a primed history cache. Redactor construction, fixture generation, warmup, assertions, and output hashing are outside the timed region. No configured known-secret files are loaded for these timing runs. Real secret inventories, different text, CPU capacity, and concurrent traffic can change cost.

All completed measured calls retained the body and redacted the injected secret. Full serialized row hashes match between the engines for every workload. The cached cases still parse and hash history and apply the replacer across the body. Cache hits therefore do not make the entire operation constant-time.

The 5 MiB image and cached-text cases have five samples per engine, with alternating engines. The 20 MiB and 34 MiB cases each have one sample per engine. Their numbers are illustrative single-run measurements, with no estimated variance. No fine-grained performance claim is made for the millisecond cached cases.

An initial overlapping run was discarded after two processes were found writing the same output file. The reported runs are sequential. No production bodies or secrets were read.

Reproduce from plana.9 with `redact_large_bench_test.go` from this PR copied into that checkout. Run each command separately, alternating engines. Use `-count=5` for repeated samples.

```sh
go test -run '^$' -bench BenchmarkRedactRequestSegments -benchtime=1x -count=1 -timeout=60m .
go test -tags gore2regex -run '^$' -bench BenchmarkRedactRequestSegments -benchtime=1x -count=1 -timeout=60m .
```

A separate CPU-profiled 5 MiB run, excluded from the timing table, attributed 96.1% of CPU samples cumulatively to `regexp.(*Regexp).find`. The hottest flat functions were `regexp.(*machine).add` at 46.9% and `regexp/syntax.(*Inst).MatchRunePos` at 24.3%. A clean unprofiled process sample used about one CPU. The limiter is regex execution, rather than fixture construction, I/O, or row buffering.

## What waits

The host was read from the exact upstream `v8.0.77` tag, commit `a5e8f69e56309e1ef6d6872e667da81fdfbce2ce`.

- [Lifecycle tracker, lines 168-190](https://github.com/router-for-me/CLIProxyAPI/blob/a5e8f69e56309e1ef6d6872e667da81fdfbce2ce/sdk/api/handlers/handlers_interceptors.go#L168-L190) calls the host completion notifier.
- [CompleteRequestExcept, lines 295-325](https://github.com/router-for-me/CLIProxyAPI/blob/a5e8f69e56309e1ef6d6872e667da81fdfbce2ce/internal/pluginhost/adapters_interceptors.go#L295-L325) detaches cancellation and starts a separate goroutine for each plugin notification. It does not wait for the callback. The host also has [a blocking-plugin regression test](https://github.com/router-for-me/CLIProxyAPI/blob/a5e8f69e56309e1ef6d6872e667da81fdfbce2ce/internal/pluginhost/request_lifecycle_test.go#L94-L131). That host test was inspected, not executed in this investigation.
- [The RPC adapter](https://github.com/router-for-me/CLIProxyAPI/blob/a5e8f69e56309e1ef6d6872e667da81fdfbce2ce/internal/pluginhost/rpc_client.go#L719-L726) and [envelope dispatcher](https://github.com/router-for-me/CLIProxyAPI/blob/a5e8f69e56309e1ef6d6872e667da81fdfbce2ce/internal/pluginhost/rpc_client.go#L270-L300) call the C client directly. [The Unix C-ABI client](https://github.com/router-for-me/CLIProxyAPI/blob/a5e8f69e56309e1ef6d6872e667da81fdfbce2ce/internal/pluginhost/loader_unix.go#L248-L288) has no dispatch mutex or shared worker. Cancellation is checked before entering C, not during a running call.
- [The sink C entry point](https://github.com/Plana-Solutions/cpa-clickhouse-sink/blob/f33482b/main.go#L86-L123) dispatches directly. [Completion](https://github.com/Plana-Solutions/cpa-clickhouse-sink/blob/f33482b/plugin.go#L239-L251) redacts before buffering. [Capture completion](https://github.com/Plana-Solutions/cpa-clickhouse-sink/blob/f33482b/capture.go#L251-L281) releases its mutex before assembling and returning the row. The plugin getter locks and redaction cache locks do not surround regex scanning. Concurrent cold calls can scan the same uncached segment independently.
- [Management routing](https://github.com/router-for-me/CLIProxyAPI/blob/a5e8f69e56309e1ef6d6872e667da81fdfbce2ce/internal/pluginhost/management.go#L233-L270) releases the host route-map mutex before calling the selected handler. [The handler dispatch](https://github.com/router-for-me/CLIProxyAPI/blob/a5e8f69e56309e1ef6d6872e667da81fdfbce2ce/internal/pluginhost/management.go#L340-L352) and [management RPC adapter](https://github.com/router-for-me/CLIProxyAPI/blob/a5e8f69e56309e1ef6d6872e667da81fdfbce2ce/internal/pluginhost/rpc_client.go#L822-L832) do not share a serialization queue with sink completion.

Thus slow regex detection blocks its own completion goroutine, the row's insertion into the sink buffer, and archive availability of that row. It is not synchronously on the client response path and does not directly serialize other completion calls, hook calls, or the archive plugin's management calls. This is a source-derived concurrency conclusion. No live traffic or end-to-end client load test was run.

CPU, memory, scheduling, and garbage collection remain shared within the proxy process. Overlapping slow scans can indirectly delay other users and management requests, including requests with the supplied 5-second proxy deadline. This investigation does not establish that sink redaction caused any observed management timeout. The deployed proxy timeout configuration was not inspected.

## Risk at the supplied traffic rate

2,300 requests/day is 0.0266 requests/second, or one every 37.6 seconds on average. A 34 MiB cold scan lasting `T` seconds represents roughly `0.0266 * fraction_of_requests_with_new_large_images * T` simultaneous scans at a steady arrival rate. This is a capacity illustration, not a queue model or a measurement of production CPU. At the measured 185 seconds, that estimate is 4.93 times the fraction. A 1% image fraction averages 0.049 active scans. A 10% fraction averages 0.49. If every request contains a new image of this size, the estimate is 4.93 active scans. Bursts can exceed these daily averages, and production hardware can differ from this Mac.

The credible production risk is delayed archive rows and unbounded concurrent CPU and memory work during image-heavy bursts. A single image scan does not establish global head-of-line blocking. Current image frequency, host headroom, actual concurrency, secret inventory, and timeout incidence were not measured, so current saturation cannot be asserted. The measured cost warrants the build fix before the next release. The faster engine still has nonzero scan cost and is not a concurrency limit.

## Fix and verification

The release workflow builds the sink with `-tags gore2regex`, vets that configuration, and runs both default-engine tests and tagged race tests. There are no runtime redaction changes or new dependencies. The benchmark exercises new images and cached history. Regression tests pin a synthetic corpus output hash and prove that skipping image data URLs would lose detection.

Passed on darwin/arm64 with Go 1.27.1: `go test -count=1 ./...`, `go test -tags gore2regex -count=1 -race ./...`, and `go vet -tags gore2regex ./...`. This includes the existing `redact/redact_test.go` corpus, the new pinned-output corpus, and the image-data counterexample. Native linux/arm64 with Go 1.26.11 built the CGO c-shared artifact and passed `scripts/verify_arm64_release.py`, which loads the library, initializes its API, calls registration, frees the buffer, and shuts down.

The deriver build remains unchanged. No live deployment, release tag, merge, production configuration access, or interaction with existing running containers was performed. Cluster checks used a disposable read-only Docker container from the already present Go image, capped at one CPU and 4 GiB memory, with source, caches, and build artifacts in tmpfs. Disk was checked before each run and remained above 10 GB free. The first build's tmpfs mount was non-executable, so loading failed after compilation. Repeating with an executable tmpfs passed the ABI check. Its subsequent vet command failed because GOPATH was inside the module root and `./...` traversed downloaded dependency tests. The final isolated run places the source in a separate subdirectory. These setup failures are retained in the local evidence.

This PR targets main as requested. Main is behind the deployed plana.9 feature branch. Its redaction functions are identical, but the rest of its release machinery differs. This PR alone is not a claim that main can replace the deployed release without reconciling that branch history.
