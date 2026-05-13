-- Mirrors the ClickHouse schema proposed in the architecture writeup.
-- DuckDB stand-in: same columnar storage model, similar batched-insert
-- semantics. Latency *shape* (batched insert + columnar scan for
-- aggregation) translates to ClickHouse.
--
-- Note on types: in production ClickHouse this is UUID + DateTime64(3).
-- Here we use VARCHAR + BIGINT (epoch_ms) to keep the appender path
-- simple. Storage cost and query semantics for our benchmark workload
-- are equivalent.

CREATE TABLE IF NOT EXISTS events (
    event_id      VARCHAR,
    customer_id   UINTEGER,
    user_id       VARCHAR,
    event_type    VARCHAR,
    url           VARCHAR,
    publish_ts    BIGINT,
    ingest_ts     BIGINT,
    properties    VARCHAR
);

-- Real-time aggregation: events per (customer, minute, event_type).
-- Production ClickHouse uses an AggregatingMergeTree materialized view
-- refreshed on insert; here we model the same dashboard-side query as
-- a logical VIEW. Latency to "visible in aggregation" equals time for
-- the underlying batched insert to complete.
CREATE VIEW IF NOT EXISTS events_per_min AS
SELECT
    customer_id,
    (publish_ts / 60000) * 60000 AS minute_epoch_ms,
    event_type,
    count(*) AS event_count
FROM events
GROUP BY 1, 2, 3;
