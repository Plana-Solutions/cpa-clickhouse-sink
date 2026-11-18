# Deriver memory acceptance

This local test uses ClickHouse 26.11.1.2737 on an ARM64 Mac. The server's
tracked-memory cap is 1,503,238,553 bytes, about 1.4 GiB. The default writer
query cap is 1 GiB. The fixed deriver lowers its own query cap to 256 MiB.
These are ClickHouse limits, not an operating-system container RSS limit.
No production systems or credentials are used.

## Reproduce

These scripts truncate tables on the disposable loopback server at port 28123.
They require at least 3 GiB free on the Mac's Data volume.

1. Use the shared `/Users/minoo/.cache/clickhouse/clickhouse` binary.
2. Run `clickhouse server --config-file=benchmarks/clickhouse-memory.xml`.
3. Run `python3 benchmarks/load.py`.
4. Build the deriver from tag `v0.1.0-plana.1` as `/tmp/cpa-deriver-bench/before`.
   Build this checkout's `deriver` module as `/tmp/cpa-deriver-bench/after`.
5. Run `python3 benchmarks/measure.py /tmp/cpa-deriver-bench/before /tmp/cpa-deriver-bench/after`.
   Repeat five times, alternating the binaries, without reloading the raw table.
6. Run `python3 benchmarks/live.py /tmp/cpa-deriver-bench/after`.
7. Stop the disposable server and delete its `/tmp/cpa-deriver-bench` directory.
   Preserve the shared ClickHouse binary.

## Workload and controls

The workload contains 2,400 requests across 20 ingestion hours. Request bodies
total 607,710,026 bytes. Each contains valid chat JSON with about 100 to 406 KB
of deterministic synthetic text. Stream chunks contain an expected answer.
Repeated text compresses unusually well. This tests decoded-body memory pressure
and extraction, and does not establish real-data disk throughput.

Both binaries run under the same frozen server configuration. Lazy
materialization is disabled and preferred_block_size_bytes is zero to expose
large read allocations. These are stress settings. Production settings were
not read. Modern ClickHouse lazy materialization can make the old query pass
on this small fixture, which would hide the original failure mode.
The fixed body reader explicitly uses one-row blocks, a 1 MiB preferred block
size, and one thread. Candidate sorting contains no body columns.

`results.jsonl` records end-to-end rebuild wall time including setup, extraction,
facts inserts and checkpoint inserts. Each successful run asserts 2,400 unique
facts with the expected reply. Query errors and peak tracked query memory come
from `system.query_log`. This is a memory acceptance result, not a speedup ratio
against a baseline that completed the work.

The Mac had 10 logical CPUs and load averages 7.36, 7.36, 7.60 during the run.
Other workstation activity continued. The first fixed run overlapped the test
suite. A one-second native sample in the second fixed run showed JSON and
`gjson` parsing work. That run includes the sampling overhead. The deriver used
roughly 80% of one CPU in a process snapshot. Shared-machine timings vary.

## Root cause

The released fetch selects bodies before `ORDER BY ingested_at, request_id
LIMIT 50`. Neither the raw table nor facts is sorted on ingestion time. The
server can decode the full pending body's read granules before the limit,
and the anti-join also grows with the pending range.

The controlled baseline fails before committing any rows. `baseline-exception.json`
records a rejected 1 GiB allocation while reading `request_body`. ClickHouse
reports 1.01 GiB required against the 1 GiB query cap. The logged peak of about
8 MiB excludes the rejected allocation and must not be interpreted as a fit.

## Fixed scheduling

Each pass reserves half of its 512-row allowance for the newest five minutes,
then serves oldest backlog rows. Candidate pages select only IDs and ingestion
timestamps within a one-hour window beginning at the next raw timestamp.
The exact facts lookup contains only the current page IDs. Payload sizes select
batches of at most 16 rows and 8 MiB. `fetch` is the single body-returning function.
It streams rows to extraction. Facts and copied backlog checkpoints commit per batch.

Fresh facts cannot move the backlog checkpoint. A restart continues from the
copied backlog ingestion time minus one hour. Failed inserts stop the pass.
A lost acknowledgement cannot duplicate a facts row on the next pass.
Rebuild truncates facts and checkpoints and runs bounded oldest-first passes.

Each query has a 256 MiB cap and a 30-second execution limit. A normal pass has
a two-minute deadline, followed by the configured interval. Arrival rates that
exceed the fresh lane's service capacity can still build latency. One oversized
row runs alone and can fail the query cap. This is not qualification for
arbitrary body sizes or arbitrary live arrival rates.

## Results

Five alternating baseline runs all failed with zero derived rows. Five fixed
runs all completed with 2,400 unique facts, all expected replies and zero query
errors. Peak tracked query memory was 28,147,251 bytes, 26.84 MiB, in every
fixed run. Median rebuild time was 122.243 seconds, range 85.470 to 134.807.
Median throughput was 19.63 rows/s, range 17.80 to 28.08 rows/s. Raw request
body throughput at the median was about 4.97 MB/s. This does not approach the
Mac's disk bandwidth and is consistent with extraction and query overhead.

See `results.jsonl` for all alternating runs, `summary.json` for the calculated
summary, and `live-results.json` for live-priority and simulated-pause recovery
assertions.

The live-priority run injected 60 rows representing arrivals during a simulated
15-minute pause, followed by 20 concurrent live rows while the 2,400-row backlog
remained. All 20 live rows were derived within 47.857 seconds with default
30-second pass spacing. At that point 1,942 older rows were still pending.
A new process using `--interval=1s` drained those remaining rows in 81.700
seconds. Final checks found 2,480 facts, 2,480 unique IDs, 2,480 expected replies,
all 60 paused rows and zero missing requests. This simulates the pause through
ingestion timestamps rather than waiting 15 wall-clock minutes.

The live and recovery stages logged zero query errors. Peak query memory across
both stages, including the verification queries, was 54,536,199 bytes, 52.01 MiB.
Recovery used shorter pass spacing to measure draining; the live latency result
used the recommended default interval.

## Production starting values

Use `--batch-rows=16 --batch-bytes=8388608 --max-rows-per-pass=512
--query-memory-bytes=268435456 --interval=30s` for an initial 1.5 GB container
trial. Keep the writer's existing cap. Deployment acceptance must check real
live latency, the container's RSS and background merge pressure. This release
was not deployed by this task.
