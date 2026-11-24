"""Copy a local-filesystem NQ base tier into a disposable, standalone IVF fixture.

Run with uv. Arguments: LOCAL_STORE OUTPUT_DIR. The source is opened read-only.
Only one complete base tier is accepted. Experiment outputs belong under .scratch/.
"""
import ctypes
import json
from pathlib import Path
import shutil
import struct
import sys

store, output = map(Path, sys.argv[1:])
if '.scratch' not in output.resolve().parts:
    raise SystemExit('output must be under .scratch/')
output.mkdir(parents=True, exist_ok=False)
raw = (store / 'ns/nq/manifest-latest').read_bytes()
if raw.startswith(b'BFPUT001'):
    raw = raw[24:]
raw = raw[8:]  # The compact reader copy prefixes the manifest generation.
if raw.startswith(b'{'):
    manifest = json.loads(raw)
else:
    zstd = ctypes.CDLL('libzstd.so.1')
    zstd.ZSTD_decompress.restype = ctypes.c_size_t
    out = ctypes.create_string_buffer(100_000_000)
    size = zstd.ZSTD_decompress(out, len(out), raw, len(raw))
    if size > len(out):
        raise SystemExit('manifest decompression failed')
    manifest = json.loads(out.raw[:size])
tiers = manifest['ann_tiers']['vector']
if len(tiers) != 1:
    raise SystemExit('expected one complete base tier')
tier = tiers[0]
count = max(c['base'] + c['rows'] for c in tier['covered'])
ids = [None] * count
for cover in tier['covered']:
    raw = (store / f"ns/nq/segments/{cover['segment']}/ids.bfi").read_bytes()
    if raw[:8] != b'BFPUT001':
        raise SystemExit('unexpected local object envelope')
    raw = raw[24:]
    offset, length = struct.unpack_from('<QI', raw, len(raw) - 20)
    directory = json.loads(raw[offset:offset + length])
    block = directory['columns']['value']['blocks'][0]
    values = json.loads(raw[block['offset']:block['offset'] + block['length']])['ids']
    if len(values) != cover['rows'] or not all(isinstance(i, int) for i in values):
        raise SystemExit('expected integer NQ IDs')
    ids[cover['base']:cover['base'] + cover['rows']] = values
if any(i is None for i in ids):
    raise SystemExit('base tier is incomplete')
with (output / 'rowmap.u32').open('wb') as f:
    for i in ids:
        f.write(struct.pack('<I', i))
for index, slab in enumerate(tier['slabs']):
    key = slab['files']['annv2:vector']['key']
    with (store / key).open('rb') as source, (output / f'{index:03}.bfa').open('wb') as dest:
        header = source.read(24)
        if header[:8] != b'BFPUT001':
            raise SystemExit('unexpected local object envelope')
        shutil.copyfileobj(source, dest)
print(f"exported slabs={len(tier['slabs'])}, rows={len(ids)}")
