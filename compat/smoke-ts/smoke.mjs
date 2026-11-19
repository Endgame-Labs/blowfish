import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import Turbuffer from '@turbopuffer/turbopuffer';

const baseURL = process.argv[2];
if (!baseURL) throw new Error('Usage: run.sh <base-url>');
const client = new Turbuffer({ baseURL, apiKey: process.env.COMPAT_API_KEY || 'dummy', maxRetries: 0 });
const name = `blowfish-oracle-ts-${randomUUID()}`;
const ns = client.namespace(name);
const explicit = client.namespace(`${name}-explicit`);
const preferred = client.namespace(`${name}-preferred`);
try {
  await ns.write({
    upsert_rows: [
      { id: 1, vector: [1, 0], text: 'quick brown fox' },
      { id: 2, vector: [0, 1], text: 'slow dog' },
    ],
    distance_metric: 'cosine_distance',
    schema: { text: { type: 'string', full_text_search: true } },
  });
  const ann = await ns.query({ rank_by: ['vector', 'ANN', [1, 0]], top_k: 2 });
  assert.equal(ann.rows[0].id, 1);
  const bm25 = await ns.query({ rank_by: ['text', 'BM25', 'fox'], top_k: 2 });
  assert.equal(bm25.rows[0].id, 1);
  const metadata = await ns.metadata();
  assert.equal(typeof metadata, 'object');
  const started = await explicit.startCopyFrom({ source_namespace: name });
  const deadline = Date.now() + 15000;
  let operation;
  do {
    operation = await explicit.pollCopyFrom(started.token);
    if (operation.status === 'finished') break;
    if (Date.now() > deadline) throw new Error('async copy timeout');
    await new Promise(resolve => setTimeout(resolve, 50));
  } while (true);
  assert.equal(operation.result.success.status, 'OK');
  assert.equal((await explicit.query({ rank_by: ['id', 'asc'], top_k: 10 })).rows.length, 2);
  const copied = await preferred.write({ copy_from_namespace: name });
  assert.equal(copied.status, 'OK');
  const patched = await ns.updateMetadata({ read_only: true, pinning: { replicas: 2 } });
  assert.equal(patched.read_only, true);
  assert.equal(patched.pinning.replicas, 2);
  await assert.rejects(ns.write({ deletes: [1] }), error => error.status === 400);
  await ns.updateMetadata({ read_only: false, pinning: false });
  console.log('PASS TypeScript write, ANN, BM25, metadata, async start/poll, and transparent polling');
} finally {
  for (const target of [ns, explicit, preferred]) {
    await target.deleteAll().catch(error => { if (error.status !== 404) throw error; });
  }
}

// Backpressure: a server with a tiny unindexed limit answers the second write 429 until its
// indexer publishes. The official SDK must retry after the advertised delay and then succeed.
const bpURL = process.env.COMPAT_BACKPRESSURE_URL;
if (bpURL) {
  const events = [];
  const recording = async (url, init) => {
    const sent = Date.now();
    const response = await fetch(url, init);
    events.push({ sent, status: response.status, ms: response.headers.get('retry-after-ms'), at: Date.now() });
    return response;
  };
  const bp = new Turbuffer({ baseURL: bpURL, apiKey: process.env.COMPAT_API_KEY || 'dummy', maxRetries: 6, fetch: recording });
  const bpNs = bp.namespace(`blowfish-oracle-ts-retry-${randomUUID()}`);
  try {
    await bpNs.write({ upsert_rows: [{ id: 1, text: 'x' }] });
    const result = await bpNs.write({ upsert_rows: [{ id: 2, text: 'x' }] });
    assert.equal(result.rows_affected, 1);
    const statuses = events.map(e => e.status);
    assert.equal(statuses[1], 429, `statuses ${statuses}`);
    assert.equal(statuses[statuses.length - 1], 200);
    events.forEach((e, i) => {
      if (e.status !== 429) return;
      const ms = Number(e.ms);
      const waited = events[i + 1].sent - e.at;
      assert.ok(ms >= 500 && ms <= 30000, `retry-after-ms ${ms}`);
      assert.ok(waited >= 0.85 * ms && waited <= ms + 1500, `waited ${waited} ms, header said ${ms} ms`);
    });
    console.log('PASS TypeScript retries a 429 after the advertised retry-after-ms');
  } finally {
    await bpNs.deleteAll().catch(error => { if (error.status !== 404) throw error; });
  }
}
