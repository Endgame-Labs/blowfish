# /// script
# requires-python = ">=3.12"
# ///
"""Round C45: size every section of format-7 text segments (fts.1.bft) from local copies.

usage: c45_sections.py FILE...   (prints one JSON object per file, then totals)

Layout (bundle.rs): long-term postings [0, positions_start), phrase positions up to the first bundle,
then dictionary bundles (8-byte header, FST plus padding, then per term a compact record and, for terms of
at most 32 blocks, its postings), then packed document lengths, then the chunk index, then a 72-byte footer.
"""
import json, struct, sys


# Offsets in the footer and chunk index are 24 bytes short of the stored object (a leading header, found by
# checking against the objects), so every absolute offset gets this added.
HEADER = 24


def varint(b, i):
    v = s = 0
    while True:
        x = b[i]; i += 1
        v |= (x & 127) << s; s += 7
        if x < 128:
            return v, i


def index_at(b, do):
    """The chunk index runs to the footer; find where it starts (the footer's offset can sit a few bytes before it)."""
    end = len(b) - 72
    for k in range(do, do + 64):
        try:
            n, = struct.unpack_from("<I", b, k); i = k + 4; chunks = []
            for _ in range(n):
                l, = struct.unpack_from("<I", b, i); i += 4 + l
                base, blen, _crc = struct.unpack_from("<QII", b, i); i += 16
                chunks.append((base + HEADER, blen))
            if i == end:
                return chunks
        except struct.error:
            pass
    raise ValueError("no chunk index")


def sections(path):
    b = open(path, "rb").read()
    size = len(b)
    ver, flags, docs, _tok = struct.unpack_from("<IIII", b, size - 72 + 8)
    lo, mo, do = struct.unpack_from("<QQQ", b, size - 72 + 32)
    lo, mo, do = lo + HEADER, mo + HEADER, do + HEADER
    chunks = index_at(b, do)
    first_bundle = chunks[0][0]
    out = dict(path=path.split("/")[-1][-10:], size=size, version=ver, positions_flag=flags & 1, docs=docs,
               long_postings=mo, positions=first_bundle - mo, bundles=lo - first_bundle, lengths=do - lo,
               chunk_index=size - 72 - do, footer=72, n_bundles=len(chunks))
    fst = pad = rec = pay = nterms = nshort = nblocks = posvar = 0
    short_blocks = []
    for base, blen in chunks:
        d, r = struct.unpack_from("<II", b, base)
        fst += d; pad += r - d
        i = base + 8 + r; end = base + blen
        while i < end:
            rs, = struct.unpack_from("<I", b, i)
            j = i + 4 + 16; stop = i + rs - 4; payload = 0; blocks = 0
            while j < stop:
                for f in range(6):
                    j0 = j
                    v, j = varint(b, j)
                    if f == 4: sz = v
                    if f == 5: posvar += j - j0
                j += 8; payload += sz; blocks += 1
            nblocks += blocks
            assert j == stop, (path, i)
            # payload follows only for short terms
            if blocks <= 32:
                pay += payload; nshort += 1; short_blocks.append(blocks)
                i += rs + payload
            else:
                i += rs
            rec += rs; nterms += 1
        assert i == end, (path, i, end)
    # A scoring record keeps the offset, the block bounds (first, last, max_tf, min_length, size), and one checksum:
    # it drops the position offset (8 bytes per term), the position size varint, and both CRCs (8 bytes per block).
    out.update(blocks=nblocks, record_dropped=8 * nterms + posvar + 8 * nblocks, fst=fst, fst_padding=pad, records=rec, short_payload=pay, terms=nterms, short_terms=nshort)
    return out


if __name__ == "__main__":
    tot = {}
    for p in sys.argv[1:]:
        o = sections(p); print(json.dumps(o))
        for k, v in o.items():
            if isinstance(v, int): tot[k] = tot.get(k, 0) + v
    print(json.dumps({"total": tot}))
