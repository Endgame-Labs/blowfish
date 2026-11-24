//! Owned scratch can cross an async suspension without retaining a thread-local borrow.
use std::cell::RefCell;
use std::ops::Deref;

#[derive(Default)]
struct Pool {
    rows: Option<RowStorage>,
    windows: Vec<Vec<f64>>,
    window_bytes: usize,
}

thread_local! {
    static POOL: RefCell<Pool> = RefCell::default();
}

#[derive(Default)]
struct RowStorage {
    values: Vec<f32>,
    touched: Vec<u32>,
}

/// One dense accumulator per worker, cleared only at rows that contributed.
pub(crate) struct Rows(RowStorage);

impl Rows {
    pub(crate) fn take(len: usize) -> Self {
        let mut rows = POOL.with(|pool| pool.borrow_mut().rows.take().unwrap_or_default());
        rows.values.resize(len, 0.0);
        Self(rows)
    }

    pub(crate) fn add(&mut self, row: u32, score: f32) {
        if score == 0.0 {
            return;
        }
        let value = &mut self.0.values[row as usize];
        if *value == 0.0 {
            self.0.touched.push(row);
        }
        *value += score;
    }

    pub(crate) fn touched(&self) -> &[u32] {
        &self.0.touched
    }
}

impl Deref for Rows {
    type Target = [f32];
    fn deref(&self) -> &Self::Target {
        &self.0.values
    }
}

impl Drop for Rows {
    fn drop(&mut self) {
        for row in self.0.touched.drain(..) {
            self.0.values[row as usize] = 0.0;
        }
        // Futures may resume on a different worker. Return storage to that worker,
        // keeping at most one accumulator, including during cancellation or error.
        let _ = POOL.try_with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool
                .rows
                .as_ref()
                .is_none_or(|rows| rows.values.capacity() < self.0.values.capacity())
            {
                pool.rows = Some(std::mem::take(&mut self.0));
            }
        });
    }
}

/// Reusable term/window bounds. The retained pool matches the search planner's
/// existing 65,536-window limit, independently of query or prefix width.
#[derive(Default)]
pub(crate) struct Windows(Vec<f64>);

impl Windows {
    const MAX_BYTES: usize = 65_536 * std::mem::size_of::<f64>();

    pub(crate) fn take(len: usize) -> Self {
        let mut values = POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            let index = pool.windows.iter().position(|v| v.capacity() >= len);
            let values = index
                .map(|i| pool.windows.swap_remove(i))
                .or_else(|| pool.windows.pop())
                .unwrap_or_default();
            pool.window_bytes -= values.capacity() * std::mem::size_of::<f64>();
            values
        });
        values.resize(len, 0.0);
        values.fill(0.0);
        Self(values)
    }
}

impl Deref for Windows {
    type Target = [f64];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Windows {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for Windows {
    fn drop(&mut self) {
        let bytes = self.0.capacity() * std::mem::size_of::<f64>();
        if bytes == 0 || bytes > Self::MAX_BYTES {
            return;
        }
        let _ = POOL.try_with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.window_bytes + bytes <= Self::MAX_BYTES {
                pool.window_bytes += bytes;
                pool.windows.push(std::mem::take(&mut self.0));
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_clear_after_cancellation_and_across_segment_sizes() {
        let mut future = Box::pin(async {
            let mut rows = Rows::take(1_000_000);
            rows.add(999_999, 3.0);
            rows.add(7, 1.0);
            rows.add(7, 2.0);
            rows.add(8, 0.0);
            assert_eq!(rows.touched(), [999_999, 7]);
            std::future::pending::<()>().await;
        });
        let mut context = std::task::Context::from_waker(futures::task::noop_waker_ref());
        assert!(std::future::Future::poll(future.as_mut(), &mut context).is_pending());
        drop(future);
        let pointer = {
            let rows = Rows::take(16);
            assert!(rows.iter().all(|v| v.to_bits() == 0));
            rows.as_ptr()
        };
        let rows = Rows::take(1_000_000);
        assert_eq!(
            rows.as_ptr(),
            pointer,
            "reuse the larger segment's allocation"
        );
        assert!(rows.iter().all(|v| v.to_bits() == 0));
    }

    #[test]
    fn overlapping_and_migrating_queries_keep_independent_storage() {
        let mut outer = Rows::take(1024);
        outer.add(3, 4.0);
        let mut inner = Rows::take(128);
        inner.add(3, 9.0);
        assert_ne!(inner.as_ptr(), outer.as_ptr());
        std::thread::spawn(move || drop(inner)).join().unwrap();
        assert_eq!(outer[3], 4.0);
        drop(outer);
        let rows = Rows::take(1024);
        assert_eq!(rows[3].to_bits(), 0);
    }

    #[test]
    fn window_scratch_clears_reused_bounds_and_limits_retained_bytes() {
        let buffers: Vec<_> = (0..20)
            .map(|_| {
                let mut windows = Windows::take(8192);
                windows[7] = 42.0;
                windows
            })
            .collect();
        drop(buffers);
        POOL.with(|pool| assert!(pool.borrow().window_bytes <= Windows::MAX_BYTES));
        let windows = Windows::take(8192);
        assert!(windows.iter().all(|v| v.to_bits() == 0));
        drop(windows);
        let before = POOL.with(|pool| pool.borrow().window_bytes);
        drop(Windows(vec![0.0; Windows::MAX_BYTES]));
        POOL.with(|pool| assert_eq!(pool.borrow().window_bytes, before));
    }
}
