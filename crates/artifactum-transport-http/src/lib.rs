//! Host-owned HTTP acquisition transport.

use std::{path::Path, time::Duration};

use artifactum_core::HttpAcquisition;
use futures_util::StreamExt;
use reqwest::{header, Client, StatusCode};
use thiserror::Error;
use tokio::{fs, io::{AsyncSeekExt, AsyncWriteExt, SeekFrom}, time::sleep};

#[derive(Debug, Error)]
pub enum Error {
    #[error("HTTP error: {0}")] Http(#[from] reqwest::Error),
    #[error("I/O error: {0}")] Io(#[from] std::io::Error),
    #[error("HTTP request failed after retries: {0}")] Exhausted(String),
}
pub type Result<T,E=Error>=std::result::Result<T,E>;

#[derive(Clone, Debug)]
pub struct HttpTransport {
    client: Client,
    retries: usize,
}
impl Default for HttpTransport { fn default()->Self{Self::new()} }
impl HttpTransport {
    #[must_use]
    pub fn new()->Self {
        let client=Client::builder().user_agent(concat!("artifactum/",env!("CARGO_PKG_VERSION"))).build().expect("valid HTTP client");
        Self{client,retries:4}
    }
    #[must_use] pub fn retries(mut self,retries:usize)->Self{self.retries=retries;self}
    pub async fn execute(&self,plan:&HttpAcquisition,destination:&Path)->Result<u64>{
        let mut last=None;
        for attempt in 0..=self.retries {
            match self.attempt(plan,destination).await {
                Ok(n)=>return Ok(n),
                Err(error)=>{
                    last=Some(error.to_string());
                    if attempt==self.retries { break; }
                    sleep(Duration::from_millis(150_u64.saturating_mul(1_u64 << attempt.min(5)))).await;
                }
            }
        }
        Err(Error::Exhausted(last.unwrap_or_else(||"unknown transfer error".into())))
    }
    async fn attempt(&self,plan:&HttpAcquisition,destination:&Path)->Result<u64>{
        let existing=if plan.resume { fs::metadata(destination).await.map(|m|m.len()).unwrap_or(0) } else {0};
        let mut request=self.client.get(&plan.url);
        for (name,value) in &plan.headers { request=request.header(name,value); }
        if existing>0 { request=request.header(header::RANGE,format!("bytes={existing}-")); }
        let response=request.send().await?;
        let status=response.status();
        let append=existing>0 && status==StatusCode::PARTIAL_CONTENT;
        let response=response.error_for_status()?;
        let mut options=fs::OpenOptions::new();
        options.create(true).write(true);
        if append { options.append(true); } else { options.truncate(true); }
        let mut output=options.open(destination).await?;
        if append { output.seek(SeekFrom::End(0)).await?; }
        let mut total=if append {existing}else{0};
        let mut stream=response.bytes_stream();
        while let Some(chunk)=stream.next().await { let chunk=chunk?; output.write_all(&chunk).await?; total=total.saturating_add(chunk.len() as u64); }
        output.sync_all().await?;
        Ok(total)
    }
}

pub async fn write_response(response:reqwest::Response,destination:&Path)->Result<u64>{
    let response=response.error_for_status()?;
    let mut output=fs::File::create(destination).await?;
    let mut stream=response.bytes_stream(); let mut bytes=0_u64;
    while let Some(chunk)=stream.next().await {let chunk=chunk?;output.write_all(&chunk).await?;bytes+=chunk.len() as u64;}
    output.sync_all().await?; Ok(bytes)
}
