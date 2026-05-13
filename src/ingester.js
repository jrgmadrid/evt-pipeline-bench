import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import Fastify from 'fastify';
import { DuckDBInstance } from '@duckdb/node-api';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, '..');
const DB_PATH = process.env.DB_PATH ?? resolve(PROJECT_ROOT, 'data.duckdb');
const PORT = Number(process.env.PORT ?? 3030);
const FLUSH_INTERVAL_MS = Number(process.env.FLUSH_INTERVAL_MS ?? 500);
const FLUSH_BATCH_SIZE = Number(process.env.FLUSH_BATCH_SIZE ?? 1000);

const instance = await DuckDBInstance.create(DB_PATH);
const conn = await instance.connect();

const schema = await readFile(resolve(PROJECT_ROOT, 'schema/init.sql'), 'utf8');
for (const stmt of schema.split(/;\s*$/m).map(s => s.trim()).filter(Boolean)) {
  await conn.run(stmt);
}
await conn.run('DELETE FROM events');

let buffer = [];
let totalInserted = 0;
let lastFlushDurationMs = 0;

async function flush() {
  if (buffer.length === 0) return;
  const batch = buffer;
  buffer = [];
  const flushStart = Date.now();
  const appender = await conn.createAppender('events');
  for (const e of batch) {
    appender.appendVarchar(e.event_id);
    appender.appendUInteger(e.customer_id);
    appender.appendVarchar(e.user_id);
    appender.appendVarchar(e.event_type);
    appender.appendVarchar(e.url);
    appender.appendBigInt(BigInt(e.publish_ts));
    appender.appendBigInt(BigInt(flushStart));
    appender.appendVarchar(e.properties ?? '{}');
    appender.endRow();
  }
  appender.closeSync();
  totalInserted += batch.length;
  lastFlushDurationMs = Date.now() - flushStart;
}

const flushTimer = setInterval(() => {
  flush().catch(err => app.log.error({ err }, 'flush failed'));
}, FLUSH_INTERVAL_MS);

const app = Fastify({ logger: { level: 'warn' } });

app.post('/events', async (req, reply) => {
  const payload = req.body;
  const events = Array.isArray(payload) ? payload : [payload];
  for (const e of events) buffer.push(e);
  if (buffer.length >= FLUSH_BATCH_SIZE) {
    await flush();
  }
  return { accepted: events.length, buffered: buffer.length };
});

app.get('/lookup/:event_id', async (req, reply) => {
  const { event_id } = req.params;
  const r = await conn.runAndReadAll(
    'SELECT ingest_ts FROM events WHERE event_id = ?',
    [event_id],
  );
  const rows = r.getRows();
  if (rows.length === 0) {
    reply.code(404);
    return { found: false };
  }
  return { found: true, ingest_ms: Number(rows[0][0]) };
});

app.get('/stats', async () => {
  const r = await conn.runAndReadAll('SELECT count(*) FROM events');
  return {
    total_inserted: totalInserted,
    buffer_depth: buffer.length,
    table_rows: Number(r.getRows()[0][0]),
    last_flush_ms: lastFlushDurationMs,
  };
});

app.get('/health', async () => ({ ok: true }));

const shutdown = async () => {
  clearInterval(flushTimer);
  await flush();
  await app.close();
  await conn.disconnectSync?.();
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

await app.listen({ port: PORT, host: '127.0.0.1' });
console.log(`ingester ready on http://127.0.0.1:${PORT}  (db=${DB_PATH})`);
