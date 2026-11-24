pub fn normalized(q: &[f32]) -> Vec<f32> {
    let norm = q.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm.is_finite() && norm > 0. {
        return q.iter().map(|x| x / norm).collect();
    }
    // Finite f32 inputs can overflow or underflow the fast norm. The wider fallback
    // preserves cosine scale invariance without sending infinities into quantization.
    let norm = q.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    q.iter().map(|x| (*x as f64 / norm) as f32).collect()
}
