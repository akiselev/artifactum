#[tokio::main]
async fn main() {
    if let Ok(path) = std::env::var("ARTIFACTUM_FIXTURE_PID_FILE") {
        let _ = std::fs::write(path, std::process::id().to_string());
    }
    if let Err(e) =
        artifactum_provider_sdk::serve_provider(artifactum_provider_fixture::Fixture).await
    {
        eprintln!("fixture provider: {e}");
        std::process::exit(1)
    }
}
