//! Object-storage-friendly vector distances, immutable IVF indexes, and exact search.
#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod cluster;
mod directory;
mod distance;
mod fastscan;
mod format;
mod index;
mod reader;
mod resident;
pub use resident::{ResidentIndex, ResidentQueryScratch};
mod relayout;
pub use bytes::Bytes;
pub use cluster::{
    BuildParams, IndexBuilder, VectorType, dots_x4, kmeans, kmeans_two_level, nearest_center,
    prepare_vector, squared_distance,
};
pub use directory::upgrade;
pub use directory::{
    DirEntry, DirSegment, Directory, Fragment, Membership, Page, PagedDirectory, PartialIndex,
};
pub use format::{ANN_FORMAT_VERSION, FORMAT_VERSION, metadata_len};
pub use index::{
    Candidate, DecodedCluster, Index, RotatedQuery, ScanScratch, SearchParams, read_gap,
    read_merged,
};
pub use reader::RangeRead;
#[cfg(feature = "legacy-fixtures")]
pub use relayout::downgrade_layout;
pub use relayout::{CodeLayout, code_layout, upgrade_layout};
mod rabitq;
pub use rabitq::{DistanceEstimate, RaBitQ, RaBitQCode};

pub use distance::{Metric, decode_base64_f32, encode_base64_f32, exact_search, recall_at_k};
pub use half::f16;
pub use roaring::RoaringBitmap;

/// Errors from input validation, index decoding, and range reads.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The supplied input violates the vector API contract.
    #[error("invalid vector input: {0}")]
    InvalidInput(String),
    /// An index is truncated, corrupt, or uses an unsupported format.
    #[error("invalid vector index: {0}")]
    InvalidFormat(String),
    /// The storage reader failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Result type used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
