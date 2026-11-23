//! Deployment-wide, append-only format generations, shared by every scoped store.
use crate::{Error, Storage};
use futures::TryStreamExt;
use object_store::{ObjectStore, path::Path};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

/// Highest bucket generation this binary can safely write or garbage-collect.
pub const BUCKET_FORMAT_VERSION: u64 = 2;
const PREFIX: &str = "_system/format/";

pub(crate) async fn check(backend: &dyn ObjectStore, root: &Path) -> Result<u64, Error> {
    let prefix = Path::from(format!("{root}/{PREFIX}"));
    let mut objects = backend.list(Some(&prefix));
    let mut highest = 1; // Unmarked buckets were written by format-1 binaries.
    while let Some(object) = objects.try_next().await? {
        let version = object
            .location
            .filename()
            .and_then(|name| name.strip_suffix(".json"))
            .and_then(|name| name.parse::<u64>().ok())
            .filter(|version| *version > 0)
            .ok_or(Error::Config("invalid bucket format marker"))?;
        highest = highest.max(version);
    }
    if highest > BUCKET_FORMAT_VERSION {
        return Err(Error::BucketFormat {
            required: highest,
            supported: BUCKET_FORMAT_VERSION,
        });
    }
    Ok(highest)
}

impl Storage {
    /// Read the deployment's cached generation without any object-store request. Startup seeds
    /// it, the background task refreshes it every 60 seconds, and callers refresh on manifest
    /// conflicts or decode failures. Once a newer generation is observed, this process stays fenced.
    pub fn cached_bucket_format(&self) -> Result<u64, Error> {
        let required = self.0.required_format.load(Ordering::Acquire);
        if required > BUCKET_FORMAT_VERSION {
            return Err(Error::BucketFormat {
                required,
                supported: BUCKET_FORMAT_VERSION,
            });
        }
        Ok(required)
    }

    /// Check the deployment root, even through a project-scoped view. Always lists the
    /// marker generations afresh; a cached marker must never authorize garbage collection.
    pub async fn check_bucket_format(&self) -> Result<u64, Error> {
        let result = check(&*self.0.backend, &self.0.prefix).await;
        let required = match &result {
            Ok(version) => *version,
            Err(Error::BucketFormat { required, .. }) => *required,
            Err(_) => return result,
        };
        // Share the monotonic latch with every mutation and scoped handle. A successful later
        // listing cannot revive a process that has already observed an unsupported generation.
        self.0.required_format.fetch_max(required, Ordering::AcqRel);
        self.cached_bucket_format()
    }

    pub(crate) fn spawn_bucket_format_refresher(&self) {
        let weak = Arc::downgrade(&self.0);
        tokio::spawn(async move {
            loop {
                // Sleep after each completed refresh, avoiding bursts after a slow LIST. The
                // weak reference lets idle stores and their caches be dropped immediately.
                tokio::time::sleep(Duration::from_secs(60)).await;
                let Some(inner) = weak.upgrade() else { break };
                let store = Storage(inner, Arc::from(""));
                if store.cached_bucket_format().is_err() {
                    break;
                }
                // Transient errors leave the last successful result cached until the next tick.
                // Unsupported generations latch every mutation off inside the fresh check.
                let _ = store.check_bucket_format().await;
            }
        });
    }

    /// Raise the deployment's format generation with a create-only object. Generations are
    /// never overwritten or removed, so concurrent upgraders cannot lower the fence.
    /// Read-only stores check compatibility without creating a marker.
    pub async fn ensure_bucket_format(&self) -> Result<(), Error> {
        let highest = self.check_bucket_format().await?;
        if !self.is_read_only() && highest < BUCKET_FORMAT_VERSION {
            let root = Self(self.0.clone(), std::sync::Arc::from(""));
            root.create(
                &format!("{PREFIX}{BUCKET_FORMAT_VERSION}.json"),
                format!("{{\"format_version\":{BUCKET_FORMAT_VERSION}}}"),
            )
            .await?;
            // Detect a competing newer upgrader before authorizing subsequent work.
            self.check_bucket_format().await?;
        }
        Ok(())
    }
}
