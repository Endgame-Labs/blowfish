"""C12 offline: how many of today's rerank candidates would a provable PCA second-stage bound prune?

Simulates today's pool (leaf k-means, 1-bit RaBitQ residual codes, 1.9-sigma statistical bounds,
k-th-upper-bound retain) on a 1M-row sample, then applies provable bounds from a second-stage code
(top-m PCA components, quantized with error norm eps, plus stored tail norm) on top of that pool.
"""
import sys, time
import numpy as np

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
NQ = 60
K = 10
NPROBE = 48
LEAF = 640
D = 1024
rng = np.random.default_rng(7)

x = np.fromfile(sys.argv[2] if len(sys.argv) > 2 else 'c1m.f16', dtype=np.float16, count=N * D).reshape(N, D).astype(np.float32)
x = x[np.isfinite(x).all(1)]
N = len(x)
qs = np.load(sys.argv[3] if len(sys.argv) > 3 else 'queries.npy')[:NQ].astype(np.float32)
print('rows', N, 'queries', len(qs), flush=True)

# leaves: spherical-ish k-means on a sample
nl = N // LEAF
t = time.time()
cent = x[rng.choice(N, nl, replace=False)].copy()
samp = x[rng.choice(N, min(N, 200_000), replace=False)]
for it in range(6):
    a = np.argmax(samp @ cent.T - 0.5 * (cent * cent).sum(1), axis=1)
    for c in range(nl):
        pass
    sums = np.zeros_like(cent)
    np.add.at(sums, a, samp)
    cnt = np.bincount(a, minlength=nl)
    m = cnt > 0
    cent[m] = sums[m] / cnt[m, None]
assign = np.empty(N, np.int32)
for s in range(0, N, 50_000):
    blk = x[s:s + 50_000]
    assign[s:s + 50_000] = np.argmax(blk @ cent.T - 0.5 * (cent * cent).sum(1), axis=1)
print('kmeans', time.time() - t, flush=True)
order = np.argsort(assign, kind='stable')
bounds = np.searchsorted(assign[order], np.arange(nl + 1))

# RaBitQ 1-bit on residuals (random orthogonal rotation)
P, _ = np.linalg.qr(rng.standard_normal((D, D)).astype(np.float32))
P = P.astype(np.float32)
res = x - cent[assign]
rn = np.linalg.norm(res, axis=1)
u = res / np.maximum(rn, 1e-12)[:, None]
ru = u @ P.T
alpha = np.abs(ru).sum(1) / np.sqrt(D)
alpha = np.clip(alpha, 1e-6, 1)
signs = np.sign(ru)
signs[signs == 0] = 1
bits = signs / np.sqrt(D)  # o-bar
spread = np.sqrt(np.maximum(1 - alpha ** 2, 0)) / (alpha * np.sqrt(D - 1))
del ru, u

# PCA of residual (and of raw) on a sample
def pca(mat, sample=200_000):
    s = mat[rng.choice(len(mat), min(sample, len(mat)), replace=False)]
    mu = s.mean(0)
    cov = np.cov((s - mu).T)
    w, v = np.linalg.eigh(cov)
    return mu, v[:, ::-1].astype(np.float32), w[::-1]

mu_r, V_r, w_r = pca(res)
cum = np.cumsum(w_r) / w_r.sum()
print('residual PCA variance in top 64/128/192/256:', [round(float(cum[i - 1]), 3) for i in (64, 128, 192, 256)])
mu_x, V_x, w_x = pca(x)
cumx = np.cumsum(w_x) / w_x.sum()
print('raw PCA variance in top 64/128/192/256:', [round(float(cumx[i - 1]), 3) for i in (64, 128, 192, 256)])

# second-stage codes, variants: (name, basis, m, quantizer)
def make_code(m, V, mode):
    """Return per-row approx of the first-m PCA coords (N,m), per-row eps (N,), tail norm (N,)"""
    Vm = V[:, :m]
    proj = res @ Vm
    tail2 = np.maximum((res * res).sum(1) - (proj * proj).sum(1), 0)
    if mode == 'exact':
        approx = proj
    elif mode.startswith('sq'):  # scalar quantize each coord to b bits, uniform per-dim range
        b = int(mode[2:])
        lo = np.percentile(proj, 0.1, axis=0)
        hi = np.percentile(proj, 99.9, axis=0)
        levels = (1 << b) - 1
        step = (hi - lo) / levels
        qv = np.clip(np.round((proj - lo) / step), 0, levels)
        approx = qv * step + lo
    eps = np.linalg.norm(proj - approx, axis=1)
    return approx.astype(np.float32), eps.astype(np.float32) * 1.0001, np.sqrt(tail2).astype(np.float32), Vm

configs = [('exact', 128), ('exact', 192), ('exact', 256), ('exact', 384), ('sq4', 128), ('sq4', 192), ('sq8', 128), ('sq2', 256)]
codes = {}
for mode, m in configs:
    codes[(mode, m)] = make_code(m, V_r, mode)
    print('built', mode, m, 'mean eps', float(codes[(mode, m)][1].mean()), 'mean tail', float(codes[(mode, m)][2].mean()), flush=True)

stats = {c: [] for c in configs}
diag = {}
pools = []
exact_k_in_pool = []
for qi, q in enumerate(qs):
    # probe nearest leaves
    sc = cent @ q - 0.5 * (cent * cent).sum(1)
    probe = np.argpartition(-sc, NPROBE)[:NPROBE]
    rows = np.concatenate([order[bounds[l]:bounds[l + 1]] for l in probe])
    qr = q[None, :] - cent[assign[rows]]            # per-row query residual
    qrn = np.linalg.norm(qr, axis=1)
    # estimate <qr_rot, obar>: rotate the query residual
    dotv = np.einsum('ij,ij->i', qr @ P.T, bits[rows])
    est = rn[rows] ** 2 + qrn ** 2 - 2 * rn[rows] * dotv / alpha[rows]
    err = 1.9 * 2 * rn[rows] * qrn * spread[rows]
    ex = ((q[None, :] - x[rows]) ** 2).sum(1)
    thr = np.partition(est + err, K - 1)[K - 1]
    keep = (est - err) <= thr
    pool = np.nonzero(keep)[0]
    pools.append(len(pool))
    # sanity: true top-k within the pool?
    topk_all = np.argsort(ex)[:K]
    exact_k_in_pool.append(np.isin(topk_all, pool).mean())
    for (mode, m), (approx, eps, tail, Vm) in codes.items():
        r = rows[pool]
        qrp = qr[pool]
        qproj = qrp @ Vm
        qt = np.sqrt(np.maximum((qrp * qrp).sum(1) - (qproj * qproj).sum(1), 0))
        dp = np.linalg.norm(qproj - approx[r], axis=1)
        lo_p = np.maximum(dp - eps[r], 0)
        hi_p = dp + eps[r]
        lo_t = np.abs(qt - tail[r])
        hi_t = qt + tail[r]
        lo = lo_p ** 2 + lo_t ** 2
        hi = hi_p ** 2 + hi_t ** 2
        # provable: must hold against exact
        exp = ex[pool]
        assert (lo <= exp * (1 + 1e-4) + 1e-5).all() and (hi >= exp * (1 - 1e-4) - 1e-5).all(), 'bound violated'
        kth = np.partition(hi, K - 1)[K - 1]
        survivors = int((lo <= kth).sum())
        stats[(mode, m)].append(survivors)
        diag.setdefault((mode, m), []).append((float(np.partition(exp, K - 1)[K - 1]), float(kth), float(np.median(hi - lo))))

print('pool size: median', np.median(pools), 'p90', np.percentile(pools, 90), 'mean', np.mean(pools))
print('true top-10 inside pool:', np.mean(exact_k_in_pool))
for c, v in stats.items():
    v = np.array(v)
    print(c, 'survivors median', np.median(v), 'p90', np.percentile(v, 90), 'reduction x(median)', round(float(np.median(pools) / max(np.median(v), 1)), 2))

for c, v in diag.items():
    v = np.array(v)
    print(c, 'median exact kth d2', round(float(np.median(v[:,0])),3), 'median provable kth upper d2', round(float(np.median(v[:,1])),3), 'median interval width', round(float(np.median(v[:,2])),3))
