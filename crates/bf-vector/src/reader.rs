use std::{future::Future, io, ops::Range};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};

/// Immutable object range reader. Implementations must return exactly the
/// requested bytes, or an error. An index assumes its object never changes.
/// The default batch implementation reads up to 32 ranges concurrently.
pub trait RangeRead: Send + Sync {
    /// Object length, already known from segment metadata or an object HEAD.
    fn len(&self) -> u64;
    /// Whether the source contains no bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Fetch an exclusive byte range.
    fn read(&self, range: Range<u64>) -> impl Future<Output = io::Result<Bytes>> + Send;
    /// Fetch ranges in input order. Override to coalesce or share cache misses.
    fn read_batch(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> impl Future<Output = io::Result<Vec<Bytes>>> + Send {
        async move {
            stream::iter(ranges.into_iter().map(|range| self.read(range)))
                .buffered(32)
                .try_collect()
                .await
        }
    }
}

impl RangeRead for Bytes {
    fn len(&self) -> u64 {
        Bytes::len(self) as u64
    }
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        let range = checked_range(range, Bytes::len(self))?;
        Ok(self.slice(range))
    }
}
impl RangeRead for Vec<u8> {
    fn len(&self) -> u64 {
        Vec::len(self) as u64
    }
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        Ok(Bytes::copy_from_slice(
            &self[checked_range(range, Vec::len(self))?],
        ))
    }
}
impl<R: RangeRead + ?Sized> RangeRead for std::sync::Arc<R> {
    fn len(&self) -> u64 {
        (**self).len()
    }
    fn read(&self, range: Range<u64>) -> impl Future<Output = io::Result<Bytes>> + Send {
        (**self).read(range)
    }
    fn read_batch(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> impl Future<Output = io::Result<Vec<Bytes>>> + Send {
        (**self).read_batch(ranges)
    }
}
fn checked_range(range: Range<u64>, len: usize) -> io::Result<Range<usize>> {
    if range.start > range.end || range.end > len as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "range exceeds object",
        ));
    }
    Ok(range.start as usize..range.end as usize)
}
