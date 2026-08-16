use artifactum_core::ArtifactProvider;
use artifactum_provider_huggingface::HuggingFaceProvider;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let provider = HuggingFaceProvider::new();
    if std::env::args().any(|argument| argument == artifactum_plugin_protocol::PLUGIN_MODE_FLAG) {
        artifactum_plugin_protocol::serve(provider).await?;
    } else {
        println!("{}", serde_json::to_string_pretty(&provider.descriptor())?);
    }
    Ok(())
}
