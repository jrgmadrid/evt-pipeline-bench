# Benchmark Results

**Headline [Benchmarked]:** 234,340 events across four scenarios, **zero errors**, **p99 publish-to-queryable latency under 500ms**. The 1000-rps scenario rerun against managed ClickHouse Cloud (same bench harness, same flush policy, only the backend swapped) measured within 2-4ms of the DuckDB local stand-in at every quantile — validating that the architectural floor is the 500ms flush policy, not the storage engine.

## Latency Measurements

Latency = wall-clock interval from event creation in the generator (`publish_ts`) to row visible at the ingester (`ingest_ts`). Includes HTTP POST overhead, ingester buffering under 500ms / 1000-row flush policy, batched insert into the configured backend, and an HTTP GET lookup per sampled event.

| target_rps | engine | achieved_rps | sent | errors | measured_samples | p50_ms | p95_ms | p99_ms | max_ms | timestamp |
|---|---|---|---|---|---|---|---|---|---|---|
| 600 | DuckDB (local) | 572 | 34320 | 0 | 1049 | 253 | 478 | 499 | 502 | 2026-05-13T17:52:51Z |
| 1000 | DuckDB (local) | 952 | 57140 | 0 | 1048 | 254 | 480 | 500 | 502 | 2026-05-13T17:53:52Z |
| 3000 | DuckDB (local) | 2860 | 85800 | 0 | 716 | 128 | 320 | 340 | 342 | 2026-05-13T17:54:22Z |
| 1000 | ClickHouse Cloud | 951 | 57080 | 0 | 637 | 252 | 476 | 497 | 502 | 2026-05-13T19:54:48Z |

## Interpretation

**The flush policy dictates the latency floor.** With a 500ms timer and a 1000-row batch trigger, any event spends an expected ~250ms in the producer's batching window before being inserted. p50 ≈ 253ms across the two lower-RPS scenarios matches this prediction within noise.

**Higher load triggers the batch-size cutoff first**, shrinking the average wait. At 3000 rps, the 1000-row threshold trips ~3x per second, so events flush in ~333ms wall-time, dragging p50 to 128ms and p99 to 340ms. *This means the system gets faster under spike load, not slower* — a useful counter-intuitive property to surface in the writeup.

**Zero errors** across 234,340 events at sustained 2,860 rps single-process. The brief's 50M events/day is 580 rps average; this run sustained ~5x that on a single Node process.

**Cross-engine validation.** The 1000-rps scenario was rerun against the actual production target — ClickHouse Cloud Scale tier — using the same Fastify ingester, same flush policy, same generator, same measurement loop. Only the backend swapped. The two backends measured within 2-4ms at every quantile (DuckDB p99 500ms vs ClickHouse Cloud p99 497ms). Both are dominated by the 500ms producer-side flush, which is an architectural choice; neither engine is the bottleneck at this load. This removes the "DuckDB ≠ ClickHouse" objection: the on-laptop benchmark's latency shape is a measured proxy for the production target, not just an asserted one.

**Mapping to AWS production.** This bench measures the data-plane bottleneck (HTTP edge → batched insert → query visibility). The streaming queue (Kinesis Data Streams) adds ~70ms for Enhanced Fan-Out propagation per AWS docs. End-to-end production envelope: ~500ms p99 + ~70ms queue = ~570ms p99, well inside the <5s SLA. The architecture has ~9x headroom against the brief's SLA.

## What This Does NOT Prove [Assumed]

- ClickHouse Cloud behavior at sustained 3000+ rps over multi-hour windows (the cross-engine run was 60s at 1000 rps; sufficient to validate latency shape, insufficient for endurance claims)
- Network behavior across AWS Availability Zones (~1-2ms intra-region, billable inter-AZ)
- Fan-out under 500 concurrent multi-tenant customers competing for resources
- GDPR right-to-erasure latency at scale (crypto-shred + ClickHouse lightweight delete throughput)
- Failure-injection: broker outage, ClickHouse mutation backlog, KMS rate limits, Kinesis hot-shard rebalance under spike

## Reproducing

**Local (DuckDB) scenarios:**

```sh
cd evt-pipeline-bench
npm install
npm run ingest &
sleep 2
npm run bench:600     # ~60s
npm run bench:1000    # ~60s
npm run bench:3000    # ~30s
kill %1
```

**ClickHouse Cloud scenario:** sign up at clickhouse.com/cloud (free trial, no card), then:

```sh
export CLICKHOUSE_URL=https://your-service.region.clickhouse.cloud:8443
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=...
export CLICKHOUSE_DATABASE=default
npm run ingest:clickhouse &
sleep 2
ENGINE='ClickHouse Cloud' npm run bench:1000
kill %1
```

Raw per-sample latency data in `results/raw/run-<engine>-<rps>.csv`.
