//! Blocked one-bit codes scanned with table lookups ("fast scan"), 32 rows at a time.
//!
//! The one-bit codes of a cluster's rows are stored transposed in blocks of [`BLOCK`] rows: byte
//! `t` of every row's code (dimensions `8t..8t+8`) sits next to byte `t` of the 31 other rows. A
//! query builds, once per index, a 16-entry table per group of four dimensions holding the sum
//! of the query's coordinates selected by each nibble, quantized to seven bits. One byte shuffle
//! then looks up four dimensions of 16 (SSE, NEON), 32 (AVX2), or 64 (AVX-512) rows at once,
//! instead of a popcount per bit plane per row. The sums are exact integers, so every path gives
//! the same result.
//!
//! The table is built from the query's offset from the index's mean center `g`, not from each
//! cluster's center, so one table serves every cluster of an index: each row stores
//! `<b, c - g> / sqrt(D)` (its code `b`, as +-1, against its cluster center `c`), and the
//! residual inner product is `<b, q - c> = <b, q - g> - <b, c - g>`.
//!
//! A cluster's code block is four `f32` arrays of `n` per-row factors, then `ceil(n / 32)`
//! blocks of `32 * padded / 8` bytes (rows past `n` are zero). The factors are what the estimate
//! needs beside the lookup sum, so a scan does no division or square root per row: the squared
//! residual norm `|r|^2`, `|r| / alpha` (negative for an all-zero input vector), the error scale
//! `|r| sqrt(1 - alpha^2) / alpha`, and `<b, c - g> / sqrt(D)`.

use crate::rabitq::ERROR_CONFIDENCE;

/// Rows per transposed block.
pub(crate) const BLOCK: usize = 32;
/// Table entries are at most this, so two lookups add up within a byte.
const LEVELS: f32 = 127.;

/// Bytes of a cluster's code block with `rows` rows of `padded` dimensions.
pub(crate) fn block_len(rows: usize, padded: usize) -> usize {
    16 * rows + padded / 8 * BLOCK * rows.div_ceil(BLOCK)
}

/// One row's code: its sign bits and the factors its estimate needs.
pub(crate) struct RowCode {
    /// `padded / 8` bytes, least significant bit first.
    pub bits: Vec<u8>,
    /// Residual norm.
    pub norm: f32,
    /// RaBitQ's inner-product correction, in `(0, 1]`.
    pub alpha: f32,
    /// `<b, c - g> / sqrt(D)`.
    pub cb: f32,
    /// The input vector is all zero.
    pub zero: bool,
}

/// Lay out a cluster's codes.
pub(crate) fn encode(rows: &[RowCode], padded: usize) -> Vec<u8> {
    let width = padded / 8;
    let mut out = Vec::with_capacity(block_len(rows.len(), padded));
    let factors: [&dyn Fn(&RowCode) -> f32; 4] = [
        &|r| r.norm * r.norm,
        &|r| {
            let k = r.norm / r.alpha;
            if r.zero { -k } else { k }
        },
        &|r| r.norm * (1. - r.alpha * r.alpha).max(0.).sqrt() / r.alpha,
        &|r| r.cb,
    ];
    for f in factors {
        for r in rows {
            out.extend(f(r).to_le_bytes());
        }
    }
    for block in rows.chunks(BLOCK) {
        let start = out.len();
        out.resize(start + width * BLOCK, 0);
        for (j, r) in block.iter().enumerate() {
            for (t, byte) in r.bits.iter().enumerate() {
                out[start + t * BLOCK + j] = *byte;
            }
        }
    }
    out
}

/// One set of lookup tables: `padded / 4` tables of 16 entries, and (on x86-64) the same tables
/// laid out for the AVX-512 kernel: per 16 dimensions, 64 bytes for low nibbles and 64 for high
/// nibbles (tables repeated per 128-bit lane).
struct Tables {
    lut: Vec<u8>,
    #[cfg(target_arch = "x86_64")]
    wide: Vec<u8>,
}

/// A query prepared for fast scans of one index: the offset from the index's mean center,
/// rotated and padded, quantized into one 16-entry table per four dimensions.
///
/// The seven-bit tables are coarse: a row's lookup sum is off by up to half a step per group, an
/// error proportional to the query's distance from the mean center rather than from the row's
/// own cluster center. A row close enough to the query to matter (a block with any row whose
/// lower bound is within the scan's cut) is scored again with fine tables holding each entry's
/// rounding residual in a further seven bits.
pub(crate) struct FastQuery {
    coarse: Tables,
    fine: std::cell::OnceCell<Tables>,
    /// The offset `q - g`, kept to build the fine tables on first use.
    x: Vec<f32>,
    /// Each group's smallest entry.
    lows: Vec<f32>,
    delta: f32,
    /// Sum of every table's smallest entry (the value of all-zero nibbles).
    base: f32,
    /// Sum of the offset's coordinates.
    sum: f32,
    padded: usize,
}

impl FastQuery {
    /// `q` is the rotated query and `g` the index's mean center, both of the padded length (a
    /// multiple of 8).
    pub(crate) fn new(q: &[f64], g: &[f32]) -> Self {
        let padded = q.len();
        debug_assert!(padded.is_multiple_of(8) && g.len() == padded);
        let x: Vec<f32> = q
            .iter()
            .zip(g)
            .map(|(q, g)| (*q - *g as f64) as f32)
            .collect();
        let groups = padded / 4;
        let mut lows = Vec::with_capacity(groups);
        let mut span = 0f32;
        for group in x.as_chunks::<4>().0 {
            let (mut low, mut high) = (0f32, 0f32);
            for v in group {
                if *v < 0. {
                    low += v;
                } else {
                    high += v;
                }
            }
            lows.push(low);
            span = span.max(high - low);
        }
        let delta = if span > 0. { span / LEVELS } else { 1. };
        let coarse = Tables::build(&x, |group, e| {
            ((e - lows[group]) / delta + 0.5).clamp(0., LEVELS).floor()
        });
        Self {
            coarse,
            fine: std::cell::OnceCell::new(),
            base: lows.iter().sum(),
            sum: x.iter().sum(),
            x,
            lows,
            delta,
            padded,
        }
    }

    /// The fine tables: entry `e`'s residual after the coarse level `L`, `e - low - delta * L`
    /// (within half a step), as `round((residual + delta / 2) / fine_step)`.
    fn fine(&self) -> &Tables {
        self.fine.get_or_init(|| {
            let (delta, step) = (self.delta, self.fine_step());
            Tables::build(&self.x, |group, e| {
                let level = ((e - self.lows[group]) / delta + 0.5)
                    .clamp(0., LEVELS)
                    .floor();
                let residual = e - self.lows[group] - delta * level;
                ((residual + delta / 2.) / step + 0.5)
                    .clamp(0., LEVELS)
                    .floor()
            })
        })
    }

    fn fine_step(&self) -> f32 {
        self.delta / LEVELS
    }

    /// The lookup sums of one block's 32 rows (`32 * padded / 8` bytes).
    #[cfg(test)]
    fn block_sums(&self, block: &[u8], out: &mut [u32; BLOCK]) {
        self.coarse.block_sums(self.padded, block, out);
    }
}

impl Tables {
    /// Tables over `x` (a multiple of 8 long) with entry `level(group, value)` for each subset
    /// sum `value` of a group's four coordinates.
    fn build(x: &[f32], level: impl Fn(usize, f32) -> f32) -> Self {
        let groups = x.len() / 4;
        let mut lut = vec![0u8; groups * 16];
        let mut entries = [0f32; 16];
        for (g, (group, table)) in x
            .as_chunks::<4>()
            .0
            .iter()
            .zip(lut.as_chunks_mut::<16>().0)
            .enumerate()
        {
            // Each entry adds one coordinate to an entry with one bit fewer.
            for n in 1..16usize {
                entries[n] = entries[n & (n - 1)] + group[n.trailing_zeros() as usize];
            }
            for (slot, e) in table.iter_mut().zip(entries) {
                *slot = level(g, e) as u8;
            }
        }
        #[cfg(target_arch = "x86_64")]
        let wide = {
            let padded = x.len();
            let mut wide = vec![0u8; padded / 16 * 128];
            for (i, out) in wide.as_chunks_mut::<128>().0.iter_mut().enumerate() {
                // Bytes 0..32 of a 64-byte load are chunk 2i (groups 4i, 4i+1) for 32 rows, and
                // bytes 32..64 chunk 2i+1 (groups 4i+2, 4i+3); a shuffle stays in 16-byte lanes.
                let table = |g: usize| &lut[g * 16..g * 16 + 16];
                for lane in 0..4 {
                    let (lo, hi) = if lane < 2 {
                        (4 * i, 4 * i + 1)
                    } else {
                        (4 * i + 2, 4 * i + 3)
                    };
                    out[lane * 16..lane * 16 + 16].copy_from_slice(table(lo));
                    out[64 + lane * 16..64 + lane * 16 + 16].copy_from_slice(table(hi));
                }
            }
            wide
        };
        Self {
            lut,
            #[cfg(target_arch = "x86_64")]
            wide,
        }
    }

    /// The lookup sums of one block's 32 rows (`32 * padded / 8` bytes).
    fn block_sums(&self, padded: usize, block: &[u8], out: &mut [u32; BLOCK]) {
        #[cfg(target_arch = "x86_64")]
        if padded.is_multiple_of(16) && avx512bw() {
            // SAFETY: the CPU features were detected, and the block is checked by the caller.
            unsafe { self.block_sums_avx512(padded, block, out) };
            return;
        }
        #[cfg(target_arch = "aarch64")]
        {
            self.block_sums_neon(padded, block, out);
            return;
        }
        #[allow(unreachable_code)]
        self.block_sums_scalar(block, out);
    }

    fn block_sums_scalar(&self, block: &[u8], out: &mut [u32; BLOCK]) {
        *out = [0; BLOCK];
        for (t, rows) in block.as_chunks::<BLOCK>().0.iter().enumerate() {
            let lo = &self.lut[2 * t * 16..2 * t * 16 + 16];
            let hi = &self.lut[(2 * t + 1) * 16..(2 * t + 1) * 16 + 16];
            for (o, b) in out.iter_mut().zip(rows) {
                *o += u32::from(lo[(b & 15) as usize]) + u32::from(hi[(b >> 4) as usize]);
            }
        }
    }

    /// 64 bytes (two dimension bytes of 32 rows) per step. Sums of two lookups fit a byte (at
    /// most 254); they accumulate in 16-bit lanes holding an even and an odd row's byte, split
    /// apart at the end, and are flushed to 32 bits every 128 steps.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f,avx512bw")]
    unsafe fn block_sums_avx512(&self, padded: usize, block: &[u8], out: &mut [u32; BLOCK]) {
        use std::arch::x86_64::*;
        let steps = padded / 16;
        debug_assert!(block.len() >= steps * 64 && self.wide.len() >= steps * 128);
        let nibble = _mm512_set1_epi8(15);
        *out = [0; BLOCK];
        let mut step = 0;
        while step < steps {
            let end = (step + 128).min(steps);
            let mut pairs = _mm512_setzero_si512();
            let mut odds = _mm512_setzero_si512();
            for s in step..end {
                // SAFETY: `block` holds `steps * 64` bytes and `wide` `steps * 128`.
                let (v, lo_t, hi_t) = unsafe {
                    (
                        _mm512_loadu_si512(block.as_ptr().add(s * 64).cast()),
                        _mm512_loadu_si512(self.wide.as_ptr().add(s * 128).cast()),
                        _mm512_loadu_si512(self.wide.as_ptr().add(s * 128 + 64).cast()),
                    )
                };
                let lo = _mm512_and_si512(v, nibble);
                let hi = _mm512_and_si512(_mm512_srli_epi16::<4>(v), nibble);
                let sum =
                    _mm512_add_epi8(_mm512_shuffle_epi8(lo_t, lo), _mm512_shuffle_epi8(hi_t, hi));
                pairs = _mm512_add_epi16(pairs, sum);
                odds = _mm512_add_epi16(odds, _mm512_srli_epi16::<8>(sum));
            }
            // Lane k of `pairs` is (even byte + 256 * odd byte) mod 2^16; the even sum is exact.
            let evens = _mm512_sub_epi16(pairs, _mm512_slli_epi16::<8>(odds));
            let (mut e, mut o) = ([0u16; 32], [0u16; 32]);
            // SAFETY: both arrays hold 64 bytes.
            unsafe {
                _mm512_storeu_si512(e.as_mut_ptr().cast(), evens);
                _mm512_storeu_si512(o.as_mut_ptr().cast(), odds);
            }
            // Lanes 0..16 cover chunk 2i (rows 2k, 2k+1), lanes 16..32 chunk 2i+1.
            for k in 0..16 {
                out[2 * k] += u32::from(e[k]) + u32::from(e[16 + k]);
                out[2 * k + 1] += u32::from(o[k]) + u32::from(o[16 + k]);
            }
            step = end;
        }
    }

    /// 16 rows of one dimension byte per lookup (`TBL`); sums widen into 16-bit lanes and are
    /// flushed to 32 bits every 128 bytes.
    #[cfg(target_arch = "aarch64")]
    fn block_sums_neon(&self, padded: usize, block: &[u8], out: &mut [u32; BLOCK]) {
        use std::arch::aarch64::*;
        let chunks = padded / 8;
        assert!(block.len() >= chunks * BLOCK && self.lut.len() >= chunks * 32);
        *out = [0; BLOCK];
        // SAFETY: NEON is mandatory on AArch64; every load is within `block` or `lut`, as
        // asserted above.
        unsafe {
            let nibble = vdupq_n_u8(15);
            let mut t = 0;
            while t < chunks {
                let end = (t + 128).min(chunks);
                let mut acc = [vdupq_n_u16(0); 4];
                for c in t..end {
                    let lo_t = vld1q_u8(self.lut.as_ptr().add(2 * c * 16));
                    let hi_t = vld1q_u8(self.lut.as_ptr().add((2 * c + 1) * 16));
                    for half in 0..2 {
                        let v = vld1q_u8(block.as_ptr().add(c * BLOCK + half * 16));
                        let sum = vaddq_u8(
                            vqtbl1q_u8(lo_t, vandq_u8(v, nibble)),
                            vqtbl1q_u8(hi_t, vshrq_n_u8::<4>(v)),
                        );
                        acc[2 * half] = vaddw_u8(acc[2 * half], vget_low_u8(sum));
                        acc[2 * half + 1] = vaddw_high_u8(acc[2 * half + 1], sum);
                    }
                }
                let mut lanes = [0u16; 32];
                for (i, a) in acc.iter().enumerate() {
                    vst1q_u16(lanes.as_mut_ptr().add(8 * i), *a);
                }
                for (o, l) in out.iter_mut().zip(lanes) {
                    *o += u32::from(l);
                }
                t = end;
            }
        }
    }
}

impl FastQuery {
    /// Score `rows` codes in one cluster's block layout. `qc2` is the squared distance from the
    /// rotated query to the cluster's center. Distances and error bounds come out as the metric
    /// reports them (halved and clamped to `[0, 2]` for cosine, at least zero for Euclidean).
    /// Calls `emit(first ordinal, rows, distances, bounds, zero mask)` once per block of up to
    /// 32 rows; bit `j` of the mask marks row `first + j` as an all-zero input vector. Scores
    /// come from the coarse tables, with their rounding in the bounds; [`Self::rescore`] refines
    /// the rows that survive.
    pub(crate) fn scan(
        &self,
        bytes: &[u8],
        rows: usize,
        qc2: f32,
        cosine: bool,
        emit: &mut impl FnMut(usize, usize, &[f32; BLOCK], &[f32; BLOCK], u32),
    ) -> crate::Result<()> {
        let padded = self.padded;
        if bytes.len() != block_len(rows, padded) {
            return Err(crate::format::bad("code block length mismatch"));
        }
        let (factors, codes) = bytes.split_at(16 * rows);
        let factor = |i: usize| factors[i * 4 * rows..(i + 1) * 4 * rows].as_chunks::<4>().0;
        let (squares, scales, errors, cbs) = (factor(0), factor(1), factor(2), factor(3));
        let root = (padded as f32).sqrt();
        // Statistical error bound, as in the row-major scan: about sqrt(1 - a^2) / (a sqrt(D - 1))
        // of the norms' product per unit.
        let spread =
            ERROR_CONFIDENCE as f32 * 2. * qc2.max(0.).sqrt() / ((padded - 1).max(1) as f32).sqrt();
        let coarse_error = self.rounding(self.delta);
        let (scale, offset) = (2. * self.delta / root, (2. * self.base - self.sum) / root);
        let (half, top) = if cosine {
            (0.5, 2.)
        } else {
            (1., f32::INFINITY)
        };
        let width = padded / 8 * BLOCK;
        let mut sums = [0u32; BLOCK];
        for (b, block) in codes.chunks_exact(width).enumerate() {
            self.coarse.block_sums(padded, block, &mut sums);
            let start = b * BLOCK;
            let n = (rows - start).min(BLOCK);
            // Padding rows get harmless factors (and are not emitted).
            let (mut sq, mut k1, mut k2, mut cb) =
                ([0f32; BLOCK], [0f32; BLOCK], [0f32; BLOCK], [0f32; BLOCK]);
            let mut zero = 0u32;
            for j in 0..n {
                sq[j] = f32::from_le_bytes(squares[start + j]);
                let k = f32::from_le_bytes(scales[start + j]);
                zero |= u32::from(k.is_sign_negative()) << j;
                k1[j] = k.abs();
                k2[j] = f32::from_le_bytes(errors[start + j]);
                cb[j] = f32::from_le_bytes(cbs[start + j]);
            }
            let (mut distance, mut bound) = ([0f32; BLOCK], [0f32; BLOCK]);
            let mut bad = false;
            for j in 0..BLOCK {
                bad |= !((sq[j] >= 0.) & (sq[j] < f32::INFINITY) & (k1[j] < f32::INFINITY))
                    | !((k2[j] >= 0.) & (k2[j] < f32::INFINITY) & (cb[j].abs() < f32::INFINITY));
                let dot = sums[j] as f32 * scale + offset - cb[j];
                distance[j] = ((sq[j] + qc2 - 2. * k1[j] * dot) * half).clamp(0., top);
                bound[j] = (spread * k2[j] + 2. * k1[j] * coarse_error) * half;
            }
            if bad {
                return Err(crate::format::bad("invalid quantization factors"));
            }
            emit(start, n, &distance, &bound, zero);
        }
        Ok(())
    }

    /// Table rounding at `step`: up to half a step per group, about uniform and independent,
    /// so the dot product is off by `2 step / sqrt(D) * sqrt(groups / 12)` (one standard
    /// deviation), times the confidence factor.
    fn rounding(&self, step: f32) -> f32 {
        let padded = self.padded;
        ERROR_CONFIDENCE as f32 * 2. * step / (padded as f32).sqrt()
            * ((padded / 4) as f32 / 12.).sqrt()
    }

    /// Whether the coarse tables' rounding is a material part of the error bound for a cluster
    /// whose center is `qc2` (squared) from the query: more than a tenth of the RaBitQ term at a
    /// typical `alpha` of 0.8. It is when the query is much nearer the cluster's center than the
    /// index's mean center (about ten times or more), as for a query that duplicates a row. Only
    /// then does [`Self::rescore`] change much.
    pub(crate) fn rounding_matters(&self, qc2: f32) -> bool {
        let padded = self.padded;
        let spread =
            ERROR_CONFIDENCE as f32 * 2. * qc2.max(0.).sqrt() / ((padded - 1).max(1) as f32).sqrt();
        2. * self.rounding(self.delta) > 0.1 * 0.6 * spread
    }

    /// Score row `ordinal` of a block layout (as [`Self::scan`] takes it, already checked by a
    /// scan) with the fine tables too: `(distance, bound)` in the metric's units, about a
    /// hundred times less table rounding than the scan's.
    pub(crate) fn rescore(
        &self,
        bytes: &[u8],
        rows: usize,
        qc2: f32,
        cosine: bool,
        ordinal: usize,
    ) -> (f32, f32) {
        let padded = self.padded;
        let factor = |i: usize| {
            let at = (i * rows + ordinal) * 4;
            f32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
        };
        let (sq, k1, k2, cb) = (factor(0), factor(1).abs(), factor(2), factor(3));
        let block = &bytes[16 * rows + ordinal / BLOCK * (padded / 8 * BLOCK)..];
        let j = ordinal % BLOCK;
        let fine = self.fine();
        let (mut coarse_sum, mut fine_sum) = (0u32, 0u32);
        for t in 0..padded / 8 {
            let byte = block[t * BLOCK + j];
            let (lo, hi) = (
                (2 * t) * 16 + (byte & 15) as usize,
                (2 * t + 1) * 16 + (byte >> 4) as usize,
            );
            coarse_sum += u32::from(self.coarse.lut[lo]) + u32::from(self.coarse.lut[hi]);
            fine_sum += u32::from(fine.lut[lo]) + u32::from(fine.lut[hi]);
        }
        let root = (padded as f32).sqrt();
        let dot = coarse_sum as f32 * 2. * self.delta / root
            + (2. * self.base - self.sum) / root
            + fine_sum as f32 * 2. * self.fine_step() / root
            - (padded / 4) as f32 * self.delta / root
            - cb;
        let spread =
            ERROR_CONFIDENCE as f32 * 2. * qc2.max(0.).sqrt() / ((padded - 1).max(1) as f32).sqrt();
        let (half, top) = if cosine {
            (0.5, 2.)
        } else {
            (1., f32::INFINITY)
        };
        (
            ((sq + qc2 - 2. * k1 * dot) * half).clamp(0., top),
            (spread * k2 + 2. * k1 * self.rounding(self.fine_step())) * half,
        )
    }

    /// `<b, q - g> / sqrt(D)` for one row-major code from the coarse tables, or with the fine
    /// tables too (tests).
    #[cfg(test)]
    fn dot_with(&self, bits: &[u8], fine: bool) -> f32 {
        let entry = |t: &Tables, i: usize, byte: u8| {
            u32::from(t.lut[2 * i * 16 + (byte & 15) as usize])
                + u32::from(t.lut[(2 * i + 1) * 16 + (byte >> 4) as usize])
        };
        let (mut acc, mut acc_fine) = (0u32, 0u32);
        for (i, byte) in bits.iter().enumerate() {
            acc += entry(&self.coarse, i, *byte);
            if fine {
                acc_fine += entry(self.fine(), i, *byte);
            }
        }
        let root = (self.padded as f32).sqrt();
        let coarse = acc as f32 * 2. * self.delta / root + (2. * self.base - self.sum) / root;
        if !fine {
            return coarse;
        }
        coarse + acc_fine as f32 * 2. * self.fine_step() / root
            - (self.padded / 4) as f32 * self.delta / root
    }

    #[cfg(test)]
    fn dot(&self, bits: &[u8]) -> f32 {
        self.dot_with(bits, false)
    }
}

/// Whether this CPU has AVX-512BW (checked once; `BF_NO_AVX512` turns it off).
#[cfg(target_arch = "x86_64")]
fn avx512bw() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var_os("BF_NO_AVX512").is_none()
            && std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rabitq::{RaBitQ, Rng};

    fn row_codes(codes: &[Vec<u8>]) -> Vec<RowCode> {
        codes
            .iter()
            .map(|bits| RowCode {
                bits: bits.clone(),
                norm: 1.,
                alpha: 0.8,
                cb: 0.1,
                zero: false,
            })
            .collect()
    }

    fn sample(rng: &mut Rng, n: usize) -> Vec<f32> {
        (0..n)
            .map(|_| (rng.next() % 2000) as f32 / 1000. - 1.)
            .collect()
    }

    #[test]
    fn kernels_match_scalar_and_estimates_track_exact_dots() {
        let mut rng = Rng(3);
        for dims in [8usize, 24, 100, 1024, 4096] {
            let rq = RaBitQ::new(dims, 9).unwrap();
            let padded = rq.padded;
            let q = rq.rotate(&sample(&mut rng, dims));
            let g: Vec<f32> = rq
                .rotate(&sample(&mut rng, dims))
                .iter()
                .map(|x| *x as f32 * 0.5)
                .collect();
            let fq = FastQuery::new(&q, &g);
            for rows in [1usize, 31, 32, 33, 70] {
                let codes: Vec<Vec<u8>> = (0..rows)
                    .map(|_| (0..padded / 8).map(|_| rng.next() as u8).collect())
                    .collect();
                let block = encode(&row_codes(&codes), padded);
                let width = padded / 8 * BLOCK;
                for (b, chunk) in block[16 * rows..].chunks_exact(width).enumerate() {
                    let (mut fast, mut slow) = ([0u32; BLOCK], [0u32; BLOCK]);
                    fq.block_sums(chunk, &mut fast);
                    fq.coarse.block_sums_scalar(chunk, &mut slow);
                    assert_eq!(fast, slow, "dims {dims} rows {rows} block {b}");
                }
                // The quantized dot product is close to the exact one.
                for code in &codes {
                    let exact: f64 = (0..padded)
                        .map(|i| {
                            let x = q[i] - g[i] as f64;
                            if code[i / 8] & (1 << (i % 8)) != 0 {
                                x
                            } else {
                                -x
                            }
                        })
                        .sum::<f64>()
                        / (padded as f64).sqrt();
                    let norm: f64 = q
                        .iter()
                        .zip(&g)
                        .map(|(q, g)| (q - *g as f64).powi(2))
                        .sum::<f64>()
                        .sqrt();
                    let got = fq.dot(code) as f64;
                    assert!(
                        (got - exact).abs() <= 0.02 * norm + 1e-6,
                        "dims {dims}: {got} vs {exact} (norm {norm})"
                    );
                }
            }
        }
    }

    fn fq_sums(q: &[f64], g: &[f32], block: &[u8], out: &mut [u32; BLOCK]) {
        thread_local!(static FQ: std::cell::OnceCell<FastQuery> = const { std::cell::OnceCell::new() });
        FQ.with(|f| {
            f.get_or_init(|| FastQuery::new(q, g))
                .block_sums(block, out)
        });
    }

    #[test]
    #[ignore]
    fn quantization_error_against_planes() {
        let mut rng = Rng(11);
        for dims in [128usize, 1024] {
            let rq = RaBitQ::new(dims, 9).unwrap();
            let padded = rq.padded;
            let rot = |rng: &mut Rng, s: f64| -> Vec<f64> {
                rq.rotate(&sample(rng, dims))
                    .iter()
                    .map(|x| x * s)
                    .collect()
            };
            let (mut ef, mut efine, mut ep, mut n) = (0f64, 0f64, 0f64, 0usize);
            for _ in 0..20 {
                let g = rot(&mut rng, 1.0);
                let c: Vec<f64> = g
                    .iter()
                    .zip(rot(&mut rng, 0.6))
                    .map(|(a, b)| a + b)
                    .collect();
                let q: Vec<f64> = c
                    .iter()
                    .zip(rot(&mut rng, 0.6))
                    .map(|(a, b)| a + b)
                    .collect();
                let g32: Vec<f32> = g.iter().map(|x| *x as f32).collect();
                let c32: Vec<f32> = c.iter().map(|x| *x as f32).collect();
                let fq = FastQuery::new(&q, &g32);
                let pq = crate::rabitq::PlaneQuery::new(&q, &c32);
                for _ in 0..200 {
                    let code: Vec<u8> = (0..padded / 8).map(|_| rng.next() as u8).collect();
                    let sign = |i: usize| {
                        if code[i / 8] & (1 << (i % 8)) != 0 {
                            1.
                        } else {
                            -1.
                        }
                    };
                    let root = (padded as f64).sqrt();
                    let exact: f64 =
                        (0..padded).map(|i| sign(i) * (q[i] - c[i])).sum::<f64>() / root;
                    let cb: f64 = (0..padded).map(|i| sign(i) * (c[i] - g[i])).sum::<f64>() / root;
                    let fast = fq.dot(&code) as f64 - cb;
                    let fine = fq.dot_with(&code, true) as f64 - cb;
                    efine += (fine - exact).powi(2);
                    let plane = pq.signed_dot_for_tests(&code, padded);
                    ef += (fast - exact).powi(2);
                    ep += (plane - exact).powi(2);
                    n += 1;
                }
            }
            let qc: f64 = 0.6 * (padded as f64).sqrt();
            eprintln!(
                "dims {dims}: rms error fast {:.5} fine {:.6} planes {:.5} (|q - c| about {qc:.2})",
                (ef / n as f64).sqrt(),
                (efine / n as f64).sqrt(),
                (ep / n as f64).sqrt()
            );
        }
    }

    /// Throughput against the row-major popcount scan (`cargo test --release -- --ignored`).
    #[test]
    #[ignore]
    fn scan_speed() {
        let mut rng = Rng(5);
        let dims = 1024;
        let rq = RaBitQ::new(dims, 9).unwrap();
        let padded = rq.padded;
        let rows = 200;
        let clusters = 500;
        let q = rq.rotate(&sample(&mut rng, dims));
        let g: Vec<f32> = vec![0.; padded];
        let c: Vec<f32> = rq
            .rotate(&sample(&mut rng, dims))
            .iter()
            .map(|x| *x as f32)
            .collect();
        let codes: Vec<Vec<u8>> = (0..rows)
            .map(|_| (0..padded / 8).map(|_| rng.next() as u8).collect())
            .collect();
        let fast_block = encode(&row_codes(&codes), padded);
        let mut compact = Vec::new();
        for b in &codes {
            compact.extend(1f32.to_le_bytes());
            compact.extend(0.8f32.to_le_bytes());
            compact.extend(b);
        }
        let blocks_fast: Vec<Vec<u8>> = (0..clusters).map(|_| fast_block.clone()).collect();
        let blocks_old: Vec<Vec<u8>> = (0..clusters).map(|_| compact.clone()).collect();
        for _ in 0..8 {
            let t = std::time::Instant::now();
            let fq = FastQuery::new(&q, &g);
            let mut acc = 0f32;
            for b in &blocks_fast {
                fq.scan(b, rows, 1.0, true, &mut |_, n, d, e, _| {
                    for j in 0..n {
                        if d[j] - e[j] < 0.5 {
                            acc += d[j];
                        }
                    }
                })
                .unwrap();
            }
            let fast = t.elapsed();
            let t = std::time::Instant::now();
            let mut sums = [0u32; BLOCK];
            let mut total = 0u32;
            let width = padded / 8 * BLOCK;
            for b in &blocks_fast {
                for block in b[16 * rows..].chunks_exact(width) {
                    fq_sums(&q, &g, block, &mut sums);
                    total = total.wrapping_add(sums[0]);
                }
            }
            eprintln!(
                "kernel only {:.1} ns/row ({total})",
                t.elapsed().as_nanos() as f64 / (rows * clusters) as f64
            );
            let t = std::time::Instant::now();
            let mut acc2 = 0f64;
            for b in &blocks_old {
                let plane = crate::rabitq::PlaneQuery::new(&q, &c);
                plane
                    .scan(b, rows, true, padded, &mut |_, d, e, _| acc2 += d + e)
                    .unwrap();
            }
            let old = t.elapsed();
            let n = (rows * clusters) as f64;
            eprintln!(
                "fast {:.1} ns/row, old {:.1} ns/row ({acc} {acc2})",
                fast.as_nanos() as f64 / n,
                old.as_nanos() as f64 / n
            );
        }
    }
}
