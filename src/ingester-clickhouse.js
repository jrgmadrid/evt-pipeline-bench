import Fastify from 'fastify';
import { createClient } from '@clickhouse/client';

const CLICKHOUSE_URL = process.env.CLICKHOUSE_URL;
const CLICKHOUSE_USER = process.env.CLICKHOUSE_USER ?? 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD;
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE ?? 'default';
const PORT = Number(process.env.PORT ?? 3030);
const FLUSH_INTERVAL_MS = Number(process.env.FLUSH_INTERVAL_MS ?? 500);
const FLUSH_BATCH_SIZE = Number(process.env.FLUSH_BATCH_SIZE ?? 1000);

if (!CLICKHOUSE_URL || !CLICKHOUSE_PASSWORD) {
  console.error('ERROR: set CLICKHOUSE_URL and CLICKHOUSE_PASSWORD');
  console.error('  CLICKHOUSE_URL=https://your-cluster.clickhouse.cloud:8443');
  console.error('  CLICKHOUSE_USER=default (or your service user)');
  console.error('  CLICKHOUSE_PASSWORD=...');
  console.error('  CLICKHOUSE_DATABASE=default');
  process.exit(1);
}

const client = createClient({
  url: CLICKHOUSE_URL,
  username: CLICKHOUSE_USER,
  password: CLICKHOUSE_PASSWORD,
  database: CLICKHOUSE_DATABASE,
  request_timeout: 30000,
});

await client.exec({
  query: `
    CREATE TABLE IF NOT EXISTS events (
      event_id      String,
      customer_id   UInt32,
      user_id       String,
      event_type    LowCardinality(String),
      url           String,
      publish_ts    Int64,
      ingest_ts     Int64,
      properties    String
    ) ENGINE = MergeTree
    ORDER BY (customer_id, publish_ts, event_id)
  `,
});
await client.exec({ query: 'TRUNCATE TABLE events' });

let buffer = [];
let totalInserted = 0;
let lastFlushDurationMs = 0;
let inFlightFlush = null;

async function flush() {
  if (buffer.length === 0) return;
  const batch = buffer;
  buffer = [];
  const flushStart = Date.now();
  const values = batch.map(e => ({
    event_id: e.event_id,
    customer_id: e.customer_id,
    user_id: e.user_id,
    event_type: e.event_type,
    url: e.url,
    publish_ts: e.publish_ts,
    ingest_ts: flushStart,
    properties: e.properties ?? '{}',
  }));
  await client.insert({
    table: 'events',
    values,
    format: 'JSONEachRow',
  });
  totalInserted += batch.length;
  lastFlushDurationMs = Date.now() - flushStart;
}

const flushTimer = setInterval(() => {
  if (inFlightFlush) return;
  inFlightFlush = flush()
    .catch(err => app.log.error({ err }, 'flush failed'))
    .finally(() => { inFlightFlush = null; });
}, FLUSH_INTERVAL_MS);

const app = Fastify({ logger: { level: 'warn' } });

app.post('/events', async (req) => {
  const payload = req.body;
  const events = Array.isArray(payload) ? payload : [payload];
  for (const e of events) buffer.push(e);
  if (buffer.length >= FLUSH_BATCH_SIZE && !inFlightFlush) {
    inFlightFlush = flush()
      .catch(err => app.log.error({ err }, 'size-trigger flush failed'))
      .finally(() => { inFlightFlush = null; });
  }
  return { accepted: events.length, buffered: buffer.length };
});

app.get('/lookup/:event_id', async (req, reply) => {
  const { event_id } = req.params;
  const rs = await client.query({
    query: 'SELECT ingest_ts FROM events WHERE event_id = {id:String} LIMIT 1',
    query_params: { id: event_id },
    format: 'JSONEachRow',
  });
  const rows = await rs.json();
  if (rows.length === 0) {
    reply.code(404);
    return { found: false };
  }
  return { found: true, ingest_ms: Number(rows[0].ingest_ts) };
});

app.get('/stats', async () => {
  const rs = await client.query({
    query: 'SELECT count() AS c FROM events',
    format: 'JSONEachRow',
  });
  const rows = await rs.json();
  return {
    engine: 'clickhouse-cloud',
    total_inserted: totalInserted,
    buffer_depth: buffer.length,
    table_rows: Number(rows[0].c),
    last_flush_ms: lastFlushDurationMs,
  };
});

app.get('/health', async () => ({ ok: true, engine: 'clickhouse-cloud' }));

const shutdown = async () => {
  clearInterval(flushTimer);
  if (inFlightFlush) await inFlightFlush;
  await flush();
  await app.close();
  await client.close();
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

await app.listen({ port: PORT, host: '127.0.0.1' });
console.log(`ingester (ClickHouse Cloud) ready on http://127.0.0.1:${PORT}`);
console.log(`  url=${CLICKHOUSE_URL}  database=${CLICKHOUSE_DATABASE}`);
