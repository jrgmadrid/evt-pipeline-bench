import { randomUUID } from 'node:crypto';
import { writeFile, mkdir, appendFile, access } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, '..');
const RESULTS_DIR = resolve(PROJECT_ROOT, 'results');
const RAW_DIR = resolve(RESULTS_DIR, 'raw');

const INGESTER = process.env.INGESTER ?? 'http://127.0.0.1:3030';
const TARGET_RPS = Number(process.env.TARGET_RPS ?? 1000);
const DURATION_S = Number(process.env.DURATION_S ?? 30);
const SAMPLE_EVERY = Number(process.env.SAMPLE_EVERY ?? 50);
const WARMUP_S = Number(process.env.WARMUP_S ?? 3);
const URL_POOL_SIZE = 1000;
const ZIPF_S = 1.2;

const EVENT_TYPES = ['page_view', 'click', 'form_submit'];
const EVENT_WEIGHTS = [0.80, 0.95, 1.00];

function pickEventType() {
  const r = Math.random();
  for (let i = 0; i < EVENT_WEIGHTS.length; i++) {
    if (r < EVENT_WEIGHTS[i]) return EVENT_TYPES[i];
  }
  return EVENT_TYPES[0];
}

const urlPool = Array.from({ length: URL_POOL_SIZE }, (_, i) => `/p/${i}`);
const zipfWeights = (() => {
  const w = new Array(URL_POOL_SIZE);
  let sum = 0;
  for (let i = 0; i < URL_POOL_SIZE; i++) {
    w[i] = 1 / Math.pow(i + 1, ZIPF_S);
    sum += w[i];
  }
  for (let i = 0; i < URL_POOL_SIZE; i++) w[i] /= sum;
  for (let i = 1; i < URL_POOL_SIZE; i++) w[i] += w[i - 1];
  return w;
})();

function pickUrl() {
  const r = Math.random();
  let lo = 0, hi = URL_POOL_SIZE - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (zipfWeights[mid] < r) lo = mid + 1;
    else hi = mid;
  }
  return urlPool[lo];
}

function makeEvent() {
  return {
    event_id: randomUUID(),
    customer_id: 1 + Math.floor(Math.random() * 500),
    user_id: `u-${Math.floor(Math.random() * 100_000)}`,
    event_type: pickEventType(),
    url: pickUrl(),
    publish_ts: Date.now(),
    properties: '{}',
  };
}

async function postEvent(event) {
  const r = await fetch(`${INGESTER}/events`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(event),
    keepalive: false,
  });
  if (!r.ok) throw new Error(`ingester ${r.status}`);
  return r.json();
}

async function lookup(eventId) {
  const r = await fetch(`${INGESTER}/lookup/${eventId}`);
  if (r.status === 404) return null;
  if (!r.ok) throw new Error(`lookup ${r.status}`);
  const body = await r.json();
  return body.ingest_ms;
}

async function waitForIngester() {
  for (let i = 0; i < 30; i++) {
    try {
      const r = await fetch(`${INGESTER}/health`);
      if (r.ok) return;
    } catch {}
    await new Promise(r => setTimeout(r, 200));
  }
  throw new Error(`ingester not reachable at ${INGESTER}`);
}

function quantile(sorted, q) {
  if (sorted.length === 0) return null;
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * q));
  return sorted[idx];
}

async function run() {
  await mkdir(RAW_DIR, { recursive: true });
  await waitForIngester();

  console.log(`[bench] target=${TARGET_RPS} rps  duration=${DURATION_S}s  sample_every=${SAMPLE_EVERY}  warmup=${WARMUP_S}s`);

  const samples = new Map();
  const startMs = Date.now();
  const endMs = startMs + DURATION_S * 1000;
  const warmupEnd = startMs + WARMUP_S * 1000;
  const burstIntervalMs = 20;
  const eventsPerBurst = Math.max(1, Math.round(TARGET_RPS * burstIntervalMs / 1000));
  let sent = 0;
  let sampleCounter = 0;
  let errors = 0;

  const inflight = new Set();
  const send = async (e) => {
    sent++;
    try {
      await postEvent(e);
    } catch {
      errors++;
    }
  };

  const ticker = setInterval(() => {
    if (Date.now() >= endMs) return;
    for (let i = 0; i < eventsPerBurst; i++) {
      const e = makeEvent();
      sampleCounter++;
      if (Date.now() >= warmupEnd && sampleCounter % SAMPLE_EVERY === 0) {
        samples.set(e.event_id, e.publish_ts);
      }
      const p = send(e);
      inflight.add(p);
      p.finally(() => inflight.delete(p));
    }
  }, burstIntervalMs);

  const statusTimer = setInterval(() => {
    const elapsed = (Date.now() - startMs) / 1000;
    const rate = sent / elapsed;
    process.stdout.write(`\r  sent=${sent} (${rate.toFixed(0)} rps) inflight=${inflight.size} samples=${samples.size} errors=${errors}    `);
  }, 1000);

  await new Promise(r => setTimeout(r, DURATION_S * 1000));
  clearInterval(ticker);
  process.stdout.write('\n[bench] load complete, draining inflight…\n');
  await Promise.allSettled(inflight);
  clearInterval(statusTimer);

  console.log(`[bench] polling ${samples.size} samples for ingest_ts…`);
  const latencies = [];
  const csvLines = ['event_id,publish_ms,ingest_ms,latency_ms'];
  const pollDeadline = Date.now() + 30_000;
  for (const [eventId, publishMs] of samples) {
    let ingestMs = null;
    while (Date.now() < pollDeadline) {
      ingestMs = await lookup(eventId);
      if (ingestMs !== null) break;
      await new Promise(r => setTimeout(r, 100));
    }
    if (ingestMs === null) {
      csvLines.push(`${eventId},${publishMs},,TIMEOUT`);
      continue;
    }
    const latency = ingestMs - publishMs;
    latencies.push(latency);
    csvLines.push(`${eventId},${publishMs},${ingestMs},${latency}`);
  }

  latencies.sort((a, b) => a - b);
  const elapsed = (Date.now() - startMs) / 1000;
  const achievedRps = sent / DURATION_S;
  const stats = {
    target_rps: TARGET_RPS,
    achieved_rps: Math.round(achievedRps),
    duration_s: DURATION_S,
    sent,
    errors,
    samples: samples.size,
    measured: latencies.length,
    p50_ms: quantile(latencies, 0.5),
    p95_ms: quantile(latencies, 0.95),
    p99_ms: quantile(latencies, 0.99),
    max_ms: latencies.at(-1) ?? null,
    timestamp: new Date().toISOString(),
  };

  const csvPath = resolve(RAW_DIR, `run-${TARGET_RPS}.csv`);
  await writeFile(csvPath, csvLines.join('\n') + '\n');

  const mdPath = resolve(RESULTS_DIR, 'RESULTS.md');
  const headerExists = await access(mdPath).then(() => true).catch(() => false);
  const row = `| ${TARGET_RPS} | ${stats.achieved_rps} | ${stats.sent} | ${stats.errors} | ${stats.measured} | ${stats.p50_ms} | ${stats.p95_ms} | ${stats.p99_ms} | ${stats.max_ms} | ${stats.timestamp} |\n`;
  if (!headerExists) {
    const header = [
      '# Benchmark Results',
      '',
      'Latency = publish (event creation in generator) → row visible in DuckDB query at ingester. Includes HTTP POST, ingester buffering (500ms / 1000-row flush policy), DuckDB appender batch insert, HTTP GET lookup. Single-laptop, single-process ingester.',
      '',
      '| target_rps | achieved_rps | sent | errors | measured_samples | p50_ms | p95_ms | p99_ms | max_ms | timestamp |',
      '|---|---|---|---|---|---|---|---|---|---|',
      row.trimEnd(),
      '',
    ].join('\n');
    await writeFile(mdPath, header);
  } else {
    await appendFile(mdPath, row);
  }

  console.log('\n[bench] done');
  console.log(stats);
  console.log(`csv -> ${csvPath}`);
  console.log(`md  -> ${mdPath}`);
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
