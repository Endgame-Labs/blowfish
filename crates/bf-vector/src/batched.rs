//! Packed BF16 dot products and explicit L2 quantization errors for certified search.
use std::{ffi::c_void, sync::OnceLock};

type Size = unsafe extern "C" fn(usize, usize) -> usize;
type Pack = unsafe extern "C" fn(*const numkong::bf16, usize, usize, usize, *mut c_void);
type Dots = unsafe extern "C" fn(
    *const numkong::bf16,
    *const c_void,
    *mut f32,
    usize,
    usize,
    usize,
    usize,
    usize,
);
type Punned = unsafe extern "C" fn();
unsafe extern "C" {
    fn nk_find_kernel_punned(
        kind: i32,
        dtype: i32,
        viable: u64,
        kernel: *mut Option<Punned>,
        capability: *mut u64,
    );
}

/// Select a matched packing/compute family without changing NumKong's global dispatch.
/// In 7.8.7, SME's three-tile fast path stores a full final tile even for a partial
/// output width, overwriting the next row and the allocation's end. AMX and future
/// backends are also excluded until their arithmetic and buffer contracts are reviewed.
struct Kernels {
    size: Size,
    pack: Pack,
    dots: Dots,
    capability: u64,
}
impl Kernels {
    fn new(available: u64) -> Self {
        use numkong::cap;
        let viable =
            available & (cap::SERIAL | cap::NEONBFDOT | cap::HASWELL | cap::SKYLAKE | cap::GENOA);
        let find = |kind: u8| {
            let (mut function, mut capability) = (None, 0);
            // SAFETY: the pinned NumKong C API accepts these enum values ('P', 'Q',
            // and 'G', with BF16 dtype 1<<13), and writes one function and capability.
            unsafe {
                nk_find_kernel_punned(
                    i32::from(kind),
                    1 << 13,
                    viable,
                    &mut function,
                    &mut capability,
                );
            }
            (
                function.expect("NumKong BF16 kernel unavailable"),
                capability,
            )
        };
        let (size, capability) = find(b'P');
        let (pack, pack_cap) = find(b'Q');
        let (dots, dots_cap) = find(b'G');
        assert_eq!(capability, pack_cap, "BF16 packing family differs");
        assert_eq!(capability, dots_cap, "BF16 scoring family differs");
        // SAFETY: NumKong's pinned dots.h declares these three signatures for the
        // corresponding kernel kinds. All three use the same packed representation.
        unsafe {
            Self {
                size: std::mem::transmute::<Punned, Size>(size),
                pack: std::mem::transmute::<Punned, Pack>(pack),
                dots: std::mem::transmute::<Punned, Dots>(dots),
                capability,
            }
        }
    }

    fn get() -> &'static Self {
        static KERNELS: OnceLock<Kernels> = OnceLock::new();
        KERNELS.get_or_init(|| Self::new(numkong::available()))
    }
}

/// Packed buffers require the same 64-byte alignment as NumKong's PackedMatrix.
#[derive(Clone)]
#[repr(C, align(64))]
struct CacheLine([u8; 64]);

fn configure_thread() {
    thread_local! { static READY: bool = numkong::configure_thread(); }
    READY.with(|_| {});
}

/// Whether the CPU has native ARM BF16 matrix instructions.
/// Callers can retain an ordinary f32 path on other processors.
pub fn bf16_matrix_accelerated() -> bool {
    Kernels::get().capability == numkong::cap::NEONBFDOT
}

/// Rows rounded to BF16 once, with conservative norms of each row and its rounding error.
/// These bounds describe conversion only; callers must also account for dot-product roundoff.
pub struct Bf16DotRows {
    data: Vec<numkong::bf16>,
    dims: usize,
    norms: Vec<f64>,
    errors: Vec<f64>,
}
impl Bf16DotRows {
    /// Round equally sized f32 rows to nearest BF16, flushing BF16 subnormals to signed zero.
    /// The error includes flushing, so kernels that flush inputs have the same certificate.
    /// Nonfinite bounds become infinity.
    pub fn new(rows: &[&[f32]]) -> Self {
        let dims = rows.first().map_or(0, |r| r.len());
        let mut data = Vec::with_capacity(
            rows.len()
                .checked_mul(dims)
                .expect("BF16 row matrix too large"),
        );
        let mut norms = Vec::with_capacity(rows.len());
        let mut errors = Vec::with_capacity(rows.len());
        let inflation = 1. + 4. * (dims as f64 + 1.) * f64::EPSILON;
        let upper = |sum: f64| {
            if sum.is_finite() {
                sum.sqrt() * inflation + f64::MIN_POSITIVE
            } else {
                f64::INFINITY
            }
        };
        for row in rows {
            assert_eq!(row.len(), dims);
            let (mut norm, mut error) = (0., 0.);
            for x in *row {
                let mut rounded = half::bf16::from_f32(*x);
                if rounded.to_bits() & 0x7f80 == 0 {
                    rounded = half::bf16::from_bits(rounded.to_bits() & 0x8000);
                }
                data.push(numkong::bf16(rounded.to_bits()));
                let x = f64::from(*x);
                let difference = x - f64::from(rounded.to_f32());
                norm += x * x;
                error += difference * difference;
            }
            norms.push(upper(norm));
            errors.push(upper(error));
        }
        Self {
            data,
            dims,
            norms,
            errors,
        }
    }

    /// Upper bound on the original row's Euclidean norm.
    pub fn norm_upper(&self, row: usize) -> f64 {
        self.norms[row]
    }

    /// Upper bound on the Euclidean norm of the BF16 rounding error.
    pub fn error_upper(&self, row: usize) -> f64 {
        self.errors[row]
    }
}

/// BF16 centers packed once for repeated batches, with f32 accumulation.
/// Packing and scoring use a matched, reviewed NumKong backend and the caller's thread pool.
/// SME and AMX are excluded; this does not change NumKong dispatch for other callers.
pub struct Bf16DotCenters {
    packed: Vec<CacheLine>,
    kernels: &'static Kernels,
    dims: usize,
    count: usize,
    max_norm: f64,
    max_error: f64,
}
impl Bf16DotCenters {
    /// Round and pack equally sized centers, retaining conservative conversion bounds.
    pub fn new(centers: &[&[f32]]) -> Self {
        Self::with_kernels(centers, Kernels::get())
    }

    fn with_kernels(centers: &[&[f32]], kernels: &'static Kernels) -> Self {
        configure_thread();
        let rows = Bf16DotRows::new(centers);
        // SAFETY: size takes matrix dimensions only. The checked allocation is
        // 64-byte aligned, zeroed, and at least as large as the selected packer needs.
        let size = unsafe { (kernels.size)(centers.len(), rows.dims) };
        let mut packed = vec![CacheLine([0; 64]); size.div_ceil(64)];
        if size != 0 {
            // SAFETY: rows is a contiguous count x dims BF16 matrix; output is
            // distinct, aligned, and sized by this same family's size function.
            unsafe {
                (kernels.pack)(
                    rows.data.as_ptr(),
                    centers.len(),
                    rows.dims,
                    rows.dims * 2,
                    packed.as_mut_ptr().cast(),
                );
            }
        }
        Self {
            packed,
            kernels,
            dims: rows.dims,
            count: centers.len(),
            max_norm: rows.norms.iter().copied().fold(0., f64::max),
            max_error: rows.errors.iter().copied().fold(0., f64::max),
        }
    }

    /// Greatest original-center norm bound in this group.
    pub fn max_norm_upper(&self) -> f64 {
        self.max_norm
    }

    /// Greatest BF16 conversion-error norm bound in this group.
    pub fn max_error_upper(&self) -> f64 {
        self.max_error
    }

    /// Dot products of the selected rows with every center, in row-major order.
    /// The output uses f32 accumulation and may overflow; certification must reject nonfinite scores.
    pub fn dots(&self, rows: &Bf16DotRows, selected: &[usize]) -> Vec<f32> {
        assert_eq!(rows.dims, self.dims);
        let mut out = vec![
            0.;
            selected
                .len()
                .checked_mul(self.count)
                .expect("BF16 output matrix too large")
        ];
        if out.is_empty() || self.dims == 0 {
            return out;
        }
        configure_thread();
        let mut data = Vec::with_capacity(
            selected
                .len()
                .checked_mul(self.dims)
                .expect("BF16 input matrix too large"),
        );
        for row in selected {
            assert!(*row < rows.norms.len());
            data.extend_from_slice(&rows.data[row * self.dims..(row + 1) * self.dims]);
        }
        // SAFETY: A contains selected.len() contiguous rows of dims BF16 values; packed B was
        // created by NumKong for count x dims, and C is a distinct selected.len() x count buffer.
        // Unlike matrixmultiply, NumKong's strides are in bytes.
        unsafe {
            (self.kernels.dots)(
                data.as_ptr(),
                self.packed.as_ptr().cast(),
                out.as_mut_ptr(),
                selected.len(),
                self.count,
                self.dims,
                self.dims * 2,
                self.count * 4,
            );
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subnormal_inputs_are_flushed_and_charged_to_conversion_error() {
        // BF16 instructions can flush input operands before multiplication. An additive
        // f32 underflow allowance cannot cover a tiny operand times a large operand.
        let tiny = [1e-39, -1e-39, f32::from_bits(1), -f32::from_bits(1)];
        let rows = Bf16DotRows::new(&[&tiny]);
        let centers = Bf16DotCenters::new(&[&[1e18, -1e18, 1e18, -1e18]]);
        let dot = f64::from(centers.dots(&rows, &[0])[0]);
        let exact: f64 = tiny.iter().map(|x| f64::from(x.abs()) * 1e18).sum();
        let (a, b, ea, eb) = (
            rows.norm_upper(0),
            centers.max_norm_upper(),
            rows.error_upper(0),
            centers.max_error_upper(),
        );
        let ne = 36. * f64::from(f32::EPSILON);
        let bound = ea * b
            + eb * a
            + ea * eb
            + ne / (1. - ne) * (a + ea) * (b + eb)
            + 36. * f64::from(f32::MIN_POSITIVE);
        assert!(
            (dot - exact).abs() <= bound,
            "error={} bound={bound}",
            (dot - exact).abs()
        );
        assert!(rows.data.iter().all(|x| x.0 & 0x7fff == 0));
        assert!(rows.error_upper(0) >= rows.norm_upper(0));
    }

    #[test]
    fn certified_dispatch_excludes_unreviewed_matrix_families() {
        // Injecting capabilities is safe here: selection removes them before lookup.
        // This exercises the SME guard even on x86 CI, without executing Arm code.
        let kernels =
            Kernels::new(numkong::available() | numkong::cap::SME | numkong::cap::SAPPHIREAMX);
        assert_eq!(
            kernels.capability & (numkong::cap::SME | numkong::cap::SAPPHIREAMX),
            0
        );
        println!("certified BF16 capability: {:#x}", kernels.capability);
    }

    #[test]
    fn partial_matrix_tiles_preserve_every_row_and_dot_certificate() {
        // SME 7.8.7 overwrites the next row when the last of three tiles is partial
        // (81..95 columns on M4). Include both row and column tile boundaries.
        for count in [47, 48, 49, 80, 81, 90, 95, 96, 97] {
            let dims = 96;
            let data: Vec<Vec<f32>> = (0..count + 65)
                .map(|i| {
                    (0..dims)
                        .map(|d| ((i * 7919 + d * 104729) as f64).sin() as f32 * 0.1)
                        .collect()
                })
                .collect();
            let refs: Vec<_> = data.iter().map(Vec::as_slice).collect();
            let rows = Bf16DotRows::new(&refs[count..]);
            let centers = Bf16DotCenters::new(&refs[..count]);
            let selected: Vec<_> = (0..65).collect();
            let dots = centers.dots(&rows, &selected);
            for r in 0..65 {
                let (a, b, ea, eb) = (
                    rows.norm_upper(r),
                    centers.max_norm_upper(),
                    rows.error_upper(r),
                    centers.max_error_upper(),
                );
                let ne = (dims as f64 + 32.) * f64::from(f32::EPSILON);
                let bound = ea * b + eb * a + ea * eb + ne / (1. - ne) * (a + ea) * (b + eb);
                for c in 0..count {
                    let exact: f64 = data[count + r]
                        .iter()
                        .zip(&data[c])
                        .map(|(a, b)| f64::from(*a) * f64::from(*b))
                        .sum();
                    assert!(
                        (f64::from(dots[r * count + c]) - exact).abs() <= bound,
                        "count={count} row={r} center={c}"
                    );
                }
            }
        }
    }

    #[test]
    fn packed_bf16_errors_bound_original_dots_for_odd_shapes_and_reordered_rows() {
        for dims in [1, 7, 31, 128, 1024] {
            for scale in [0., 1e-20, 1., 1e18] {
                let data: Vec<Vec<f32>> = (0..17)
                    .map(|i| {
                        (0..dims)
                            .map(|d| ((i * 7919 + d * 104729) as f64).sin() as f32 * scale)
                            .collect()
                    })
                    .collect();
                let refs: Vec<_> = data.iter().map(Vec::as_slice).collect();
                let rows = Bf16DotRows::new(&refs);
                let centers = Bf16DotCenters::new(&refs[..9]);
                assert!(centers.dots(&rows, &[]).is_empty());
                let selected = [16, 0, 11, 3, 16];
                let got = centers.dots(&rows, &selected);
                for (r, i) in selected.iter().enumerate() {
                    for c in 0..9 {
                        let expected: f64 = data[*i]
                            .iter()
                            .zip(&data[c])
                            .map(|(a, b)| f64::from(*a) * f64::from(*b))
                            .sum();
                        let (a, b, ea, eb) = (
                            rows.norm_upper(*i),
                            rows.norm_upper(c),
                            rows.error_upper(*i),
                            rows.error_upper(c),
                        );
                        let ne = (dims as f64 + 32.) * f64::from(f32::EPSILON);
                        let bound = ea * b
                            + eb * a
                            + ea * eb
                            + ne / (1. - ne) * (a + ea) * (b + eb)
                            + (dims as f64 + 32.) * f64::from(f32::MIN_POSITIVE);
                        if !got[r * 9 + c].is_finite() {
                            assert!(a * b > f64::from(f32::MAX) / 4., "unexpected nonfinite dot");
                            continue;
                        }
                        assert!(
                            (f64::from(got[r * 9 + c]) - expected).abs() <= bound,
                            "dims={dims} scale={scale} row={i} center={c}"
                        );
                    }
                }
            }
        }
    }
}
