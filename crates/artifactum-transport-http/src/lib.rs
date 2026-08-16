//! Shared HTTP byte-transfer primitives for Artifactum providers.
//!
//! Provider crates remain responsible for semantic resolution, authentication,
//! and constructing requests. This crate owns the mechanical transfer of a
//! successful HTTP response into the host-provided staging path.

use std::path::Path;

use futures_util::StreamExt;
use thiserror::Error;
use tokio::{fs, io::AsyncWriteExt};

#[derive(Debug, Error)]
pub enum Error {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Stream an HTTP response into `destination`, fsync it, and return bytes
/// written. Non-success HTTP statuses are rejected before the destination is
/// created.
pub async fn write_response(response: reqwest::Response, destination: &Path) -> Result<u64> {
    let response = response.error_for_status()?;
    let mut output = fs::File::create(destination).await?;
    let mut stream = response.bytes_stream();
    let mut bytes_written = 0_u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        output.write_all(&chunk).await?;
        bytes_written = bytes_written.saturating_add(chunk.len() as u64);
    }

    output.sync_all().await?;
    Ok(bytes_written)
}
