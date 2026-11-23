//! Deployment-wide, append-only format generations, shared by every scoped store.
use crate::{Error, Storage};
use futures::TryStreamExt;
use object_store::{ObjectStore, path::Path};

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
    /// Check the deployment root, even through a project-scoped view. Always lists the
    /// marker generations afresh; a cached marker must never authorize garbage collection.
    pub async fn check_bucket_format(&self) -> Result<u64, Error> {
        let result = check(&*self.0.backend, &self.0.prefix).await;
        if let Err(Error::BucketFormat { required, .. }) = &result {
            // Once observed, fence every mutation through every scoped handle, including
            // non-WAL system writes. Removing a marker cannot revive this process.
            self.0
                .required_format
                .fetch_max(*required, std::sync::atomic::Ordering::AcqRel);
        }
        result
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
