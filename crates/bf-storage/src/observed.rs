//! Low-cardinality metrics at the backend boundary, including body consumption.
use async_trait::async_trait;
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

/// In-flight object reads and the highest count seen in the current window. The connection warmer
/// sizes its pool from the peak: a wave of N parallel reads needs N connections.
#[derive(Debug, Default)]
pub(crate) struct Activity {
    inflight: AtomicUsize,
    peak: AtomicUsize,
}
impl Activity {
    /// Highest number of concurrent reads since the last call.
    pub(crate) fn take_peak(&self) -> usize {
        self.peak
            .swap(self.inflight.load(Ordering::Relaxed), Ordering::Relaxed)
    }
    fn enter(self: &Arc<Self>) -> ReadGuard {
        let now = self.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(now, Ordering::Relaxed);
        ReadGuard(self.clone())
    }
}
struct ReadGuard(Arc<Activity>);
impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) struct Observed(pub(crate) Arc<dyn ObjectStore>, pub(crate) Arc<Activity>);
impl fmt::Display for Observed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("observed object store")
    }
}
fn record<T>(op: &'static str, start: Instant, result: &Result<T>, bytes: u64) {
    crate::diagnostics::record(op, if result.is_ok() { bytes } else { 0 }, start.elapsed());
    let status = match result {
        Ok(_) => "ok",
        Err(object_store::Error::NotFound { .. }) => "not_found",
        Err(
            object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. },
        ) => "conflict",
        Err(_) => "error",
    };
    metrics::counter!("blowfish_object_store_requests_total", "op" => op, "status" => status)
        .increment(1);
    metrics::histogram!("blowfish_object_store_request_seconds", "op" => op)
        .record(start.elapsed().as_secs_f64());
    if result.is_ok() {
        metrics::counter!("blowfish_object_store_bytes_total", "op" => op).increment(bytes);
    }
}
#[async_trait]
impl ObjectStore for Observed {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let start = Instant::now();
        let bytes = payload.content_length() as u64;
        let result = self.0.put_opts(path, payload, opts).await;
        record("put", start, &result, bytes);
        result
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> Result<GetResult> {
        let op = if opts.head {
            "head"
        } else if opts.range.is_some() {
            "get_range"
        } else {
            "get"
        };
        let head = opts.head;
        let range_dbg = match &opts.range {
            Some(object_store::GetRange::Bounded(r)) => Some((r.start, r.end)),
            _ => None,
        };
        let start = Instant::now();
        let _guard = (!head).then(|| self.1.enter());
        let result = async {
            let result = self.0.get_opts(path, opts).await?;
            if head {
                return Ok((result, 0));
            }
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let extensions = result.extensions.clone();
            let bytes = result.bytes().await?;
            let len = bytes.len() as u64;
            Ok((
                GetResult {
                    payload: GetResultPayload::Stream(
                        stream::once(async move { Ok(bytes) }).boxed(),
                    ),
                    meta,
                    range,
                    attributes,
                    extensions,
                },
                len,
            ))
        }
        .await;
        if op != "head" && trace_reads() {
            let (a, b) = range_dbg.unwrap_or((0, 0));
            eprintln!(
                "TRACE_READ {} {} {} {} {} {}",
                path,
                a,
                b,
                result.as_ref().map_or(0, |(_, len)| *len),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis()),
                start.elapsed().as_millis()
            );
        }
        record(
            op,
            start,
            &result,
            result.as_ref().map_or(0, |(_, len)| *len),
        );
        result.map(|(result, _)| result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // Storage deletes one key at a time; one stream result is one logical operation.
        let start = Instant::now();
        self.0
            .delete_stream(paths)
            .map(move |result| {
                record("delete", start, &result, 0);
                result
            })
            .boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        Self::observed_list(self.0.list(prefix))
    }
    // Forward the offset, so a backend with start-after lists only the keys after it instead of
    // the default's full listing filtered here.
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        Self::observed_list(self.0.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let start = Instant::now();
        let result = self.0.list_with_delimiter(prefix).await;
        record("list", start, &result, 0);
        result
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        let start = Instant::now();
        let result = self.0.copy_opts(from, to, opts).await;
        record("copy", start, &result, 0);
        result
    }
}

/// `BF_TRACE_READS=1` prints one line per backend read (diagnostics only).
fn trace_reads() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("BF_TRACE_READS").is_some())
}

impl Observed {
    fn observed_list(
        source: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let start = Instant::now();
        stream::unfold((source, false), move |(mut source, done)| async move {
            if done {
                return None;
            }
            match source.next().await {
                Some(item) => {
                    let failed = item.is_err();
                    if failed {
                        record("list", start, &item, 0);
                    }
                    Some((item, (source, failed)))
                }
                None => {
                    record("list", start, &Ok::<_, object_store::Error>(()), 0);
                    None
                }
            }
        })
        .boxed()
    }
}
