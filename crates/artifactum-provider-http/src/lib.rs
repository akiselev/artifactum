pub use artifactum_resolver::HttpProvider as Provider;
#[must_use]
pub fn provider() -> Provider {
    Provider
}
