#[tokio::main]async fn main(){if let Err(e)=artifactum_provider_sdk::serve_provider(artifactum_provider_osf::OsfProvider::default()).await{eprintln!("{e}");std::process::exit(1)}}
