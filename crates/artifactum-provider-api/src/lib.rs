use artifactum_resolver::{AccessRequirement, Error, Result, access_required};
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;

#[derive(Clone)]
pub struct ApiClient {
    client: Client,
}
impl Default for ApiClient {
    fn default() -> Self {
        Self {
            client: Client::builder()
                .user_agent("artifactum/0.4")
                .build()
                .expect("client"),
        }
    }
}
impl ApiClient {
    pub async fn get_json<T: DeserializeOwned>(
        &self,
        provider: &str,
        url: &str,
        headers: &BTreeMap<String, String>,
    ) -> Result<T> {
        let mut r = self.client.get(url);
        for (k, v) in headers {
            r = r.header(k, v);
        }
        let res = r.send().await.map_err(|e| Error::Provider {
            provider: provider.into(),
            message: e.to_string(),
        })?;
        if matches!(
            res.status(),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
        ) {
            return Err(access_required(
                provider,
                AccessRequirement::Authentication,
                "provider requires authentication or gated-access approval",
            ));
        }
        let res = res.error_for_status().map_err(|e| Error::Provider {
            provider: provider.into(),
            message: e.to_string(),
        })?;
        res.json().await.map_err(|e| Error::Provider {
            provider: provider.into(),
            message: e.to_string(),
        })
    }
}
pub fn bearer_from_env(env: &str) -> BTreeMap<String, String> {
    std::env::var(env)
        .ok()
        .map(|v| BTreeMap::from([("Authorization".into(), format!("Bearer {v}"))]))
        .unwrap_or_default()
}
