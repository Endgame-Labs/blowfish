# Second-stage PCA bound for the cold rerank (round C12: not built)

## Idea

After the 1-bit RaBitQ scan, rerank reads an exact vector for every candidate whose lower bound can still beat the
k-th smallest upper bound. A second per-row code (top-m PCA coordinates plus a stored error norm and tail norm,
about 32 to 48 B) would tighten each interval so fewer candidates need an exact read.

## Provable bound

With the residual `x - c` split into a top-m PCA part `p` (stored as the code `p'` with error norm `eps`) and a
tail `t` (stored norm `|t|`), and the query residual split the same way into `qp` and `qt`:

- `|q-x|^2 = |qp - p|^2 + |qt - t|^2`
- lower: `max(|qp - p'| - eps, 0)^2 + (|qt| - |t|)^2`
- upper: `(|qp - p'| + eps)^2 + (|qt| + |t|)^2`

Both follow from the triangle inequality, so a candidate dropped by "lower > k-th smallest upper" can never be in
the exact top k of the pool, and results stay identical.

## Outcome

Measured offline (`docs/benchmarks.md`, Round C12): the tail term alone makes the interval about as wide as the
candidate distance range, and no variant prunes any candidate, not even unquantized PCA coordinates at 768 B per
row. So the format below was never written.

## Format fence and upgrade (design only, for the record)

- A per-tier code file (`ann.pca.bfa`) written at recluster, with its own version byte and a checksum over the
  PCA basis, listed in the tier manifest. A reader that does not know the file ignores it.
- An old tier without the file keeps today's path until it is rebuilt; SPEC 25's automatic upgrade would write it
  the next time the tier is reclustered. Nothing would change for readers on the old format.
