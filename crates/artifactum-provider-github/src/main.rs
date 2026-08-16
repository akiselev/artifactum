use artifactum_core::ArtifactProvider;
use artifactum_provider_github::GitHubProvider;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let provider = GitHubProvider::new();
    if std::env::args().any(|argument| argument == artifactum_plugin_protocol::PLUGIN_MODE_FLAG) {
        artifactum_plugin_protocol::serve(provider).await?;
    } else {
        println!("{}", serde_json::to_string_pretty(&provider.descriptor())?);
    }
    Ok(())
}
