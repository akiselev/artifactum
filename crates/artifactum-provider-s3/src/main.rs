#[tokio::main]
async fn main() {
    if let Err(e) =
        artifactum_provider_sdk::serve_provider(artifactum_provider_s3::provider()).await
    {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
