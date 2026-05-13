# Appendix: Stateful Segment Design in ClickHouse Materialized Views

The brief names one segment example explicitly: *"viewed pricing 3x"*. This appendix walks through the ClickHouse MV design for that class of rolling-window stateful segment, since it is the most common "would I need Flink for this?" question for the proposed architecture.

## Segment definition

Plain-English rule from the brief:

> A user is in the **pricing-curious** segment if they have viewed any URL matching `/pricing*` 3+ times in the last 7 days.

This is a **count-based rolling-window** segment. It is not an *ordered* sequence (which would call for `windowFunnel`), and it is not a *cross-stream join* (which would call for Flink). It is a per-user count over a trailing time window — exactly what a columnar OLAP store with materialized views handles natively.

## Why a true sliding window is the wrong target

A literal sliding window evaluates segment membership at the moment of every event arrival: as soon as the user's 7-day count crosses 3, they enter; as soon as an old event ages out and brings the count below 3, they exit. This requires per-event state mutation and is the kind of work Flink + RocksDB is designed for.

In practice, no real-time MarTech system needs sub-second segment-state flips. Personalization decisions are made at *page-load time* by a single point query against current state, not by reacting to every backend state transition. The right granularity is therefore:

- **Maintain** per-user-per-day aggregates continuously (cheap, columnar, batched insert friendly).
- **Evaluate** segment membership on demand at query time, or refresh a cached segment table every N minutes.

This separates the *streaming* concern (keeping aggregates fresh) from the *segment-state* concern (which user is in which segment right now), and lets ClickHouse do both without Flink.

## DDL

### Per-user-per-day pricing-view rollup (MV)

```sql
CREATE MATERIALIZED VIEW segment_pricing_views_per_day
ENGINE = SummingMergeTree
PARTITION BY (customer_id, toYYYYMM(day))
ORDER BY (customer_id, user_id, day)
AS SELECT
    customer_id,
    user_id,
    toDate(publish_ts) AS day,
    countIf(event_type = 'page_view' AND url LIKE '/pricing%') AS pricing_views
FROM events
GROUP BY customer_id, user_id, day;
```

Notes:

- `SummingMergeTree` collapses rows with the same `(customer_id, user_id, day)` key on background merge — cheap aggregation maintenance.
- `countIf` filters the segment criterion at insert time, so the MV stores only relevant counts.
- Partitioning by `(customer_id, toYYYYMM(day))` keeps tenant data isolated and lets month-scale partition drops handle retention.

### Segment-evaluation query

```sql
-- "Who is in pricing-curious right now?"
SELECT user_id
FROM segment_pricing_views_per_day
WHERE customer_id = {tid:UInt32}
  AND day >= today() - 7
GROUP BY user_id
HAVING sum(pricing_views) >= 3
SETTINGS optimize_aggregation_in_order = 1;
```

### Single-user membership check (personalization hot path)

```sql
SELECT sum(pricing_views) >= 3 AS in_segment
FROM segment_pricing_views_per_day
WHERE customer_id = {tid:UInt32}
  AND user_id = {uid:String}
  AND day >= today() - 7;
```

For personalization triggers the membership check must be sub-millisecond, so the production path is: evaluate the segment-eval query every N minutes, write `(customer_id, segment_id, user_id) → bool` into Redis, and read from Redis on the hot path. ClickHouse is the *source of truth*; Redis is the *cache for personalization latency*.

## Scaling math at the brief's volume

- 50M events/day, ~580 events/sec sustained.
- Assume ~30% are `page_view`, of which ~5% match `/pricing*` → ~875K pricing-views/day.
- Assume those distribute across ~100K distinct users/day → ~10 pricing-views per active user per day.
- Per-user-per-day MV rows: **~100K/day**, **~700K/week** retained for the 7-day window evaluation.
- 7-day evaluation query is a ranged scan over ~700K rows, partitioned by tenant — well under 100ms on a single ClickHouse Cloud Scale-tier replica per public benchmarks.

## When would Flink actually be necessary?

The MV pattern above handles count-based, time-windowed, per-entity segments. Flink starts to earn its keep when you need:

1. **Cross-stream stateful joins.** "Show me users who clicked an email *and* visited the site within 5 minutes" — joining two event streams with bounded delay is Flink's home turf. ClickHouse can do this with sort-merge over short retention, but at the cost of dashboard query latency.
2. **Sub-second state-transition reactions.** "The instant a user enters the segment, fire a webhook." MVs can be polled fast, but for true event-time reactive triggers, Flink + Kafka Streams is purpose-built.
3. **Complex sequence detection.** "Saw the pricing page, then the checkout page, then abandoned within 10 minutes" — `windowFunnel` covers many of these, but stateful CEP libraries in Flink go further.

None of these appear in this brief. The recommendation to drop Flink is contingent on the segment vocabulary staying in the count-based / aggregation-based family.

## Approach comparison

| Approach | When it wins | When it loses |
|---|---|---|
| **ClickHouse MV + trailing-window query** (proposed) | Count/sum/avg segments over time windows; the 95% case in MarTech | Sub-second segment-entry triggers; cross-stream joins |
| **ClickHouse `windowFunnel`** | Ordered-sequence segments ("A then B then C") | Free-form count segments (would still need the rollup MV underneath) |
| **Periodic batch re-evaluation in Spark** | One-off segments that don't need real-time freshness | Real-time personalization triggers |
| **Materialize / RisingWave (streaming SQL)** | True sub-second state propagation with SQL-only operator team | Adds a service the 2-engineer team has to learn |
| **Flink with RocksDB state** | Cross-stream stateful joins, complex CEP, sub-second triggers | Operational burden for a 2-engineer team |

The architecture in [`submission.md`](../submission.md) lands on ClickHouse MVs because the brief's named segment example is in the 95% case, and the operational-capacity argument trumps the marginal capability gain.
