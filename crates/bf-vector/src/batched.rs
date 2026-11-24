//! Packed BF16 dot products and explicit L2 quantization errors for certified search.
use numkong::{Dots, PackedMatrix, Tensor};

fn configure_thread() {
    thread_local! { static READY: bool = numkong::configure_thread(); }
    READY.with(|_| {});
}

/// Whether the CPU has native ARM BF16 matrix instructions.
/// Callers can retain an ordinary f32 path on other processors.
pub fn bf16_matrix_accelerated() -> bool {
    numkong::available() & numkong::cap::NEONBFDOT != 0
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
    /// Round equally sized f32 rows to nearest BF16. Nonfinite bounds become infinity.
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
                let rounded = half::bf16::from_f32(*x);
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
/// Packing and scoring use NumKong's runtime-selected backend and the caller's thread pool.
pub struct Bf16DotCenters {
    packed: PackedMatrix<numkong::bf16>,
    dims: usize,
    count: usize,
    max_norm: f64,
    max_error: f64,
}
impl Bf16DotCenters {
    /// Round and pack equally sized centers, retaining conservative conversion bounds.
    pub fn new(centers: &[&[f32]]) -> Self {
        configure_thread();
        let rows = Bf16DotRows::new(centers);
        let tensor = Tensor::<numkong::bf16>::from_slice(&rows.data, &[centers.len(), rows.dims]);
        Self {
            packed: PackedMatrix::pack(&tensor),
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
            numkong::bf16::dots_packed(
                data.as_ptr(),
                self.packed.as_ptr(),
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
