import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import Turbuffer from '@turbopuffer/turbopuffer';

const baseURL = process.argv[2];
if (!baseURL) throw new Error('Usage: run.sh <base-url>');
const client = new Turbuffer({ baseURL, apiKey: process.env.COMPAT_API_KEY || 'dummy', maxRetries: 0 });
const ns = client.namespace(`blowfish-oracle-ts-${randomUUID()}`);
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
  console.log('PASS TypeScript write, ANN, BM25, and metadata');
} finally {
  await ns.deleteAll();
}
