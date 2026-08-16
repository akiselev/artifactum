//! Shared HTTP/JSON helpers for semantic REST-backed providers.
use std::collections::BTreeMap;
use artifactum_core::{access_required,provider_error,AccessRequirement,AcquisitionPlan,HttpAcquisition,ProviderProfile,ResolveContext};
use percent_encoding::{utf8_percent_encode,NON_ALPHANUMERIC};
use reqwest::{RequestBuilder,Response,StatusCode};
use serde_json::Value;

pub fn profile_value<'a>(context:&'a ResolveContext,key:&str)->Option<&'a str>{context.profile.as_ref().and_then(|p|p.config.get(key)).map(String::as_str)}
pub fn profile_value_acquire<'a>(profile:Option<&'a ProviderProfile>,key:&str)->Option<&'a str>{profile.and_then(|p|p.config.get(key)).map(String::as_str)}
pub fn env_name(profile:Option<&ProviderProfile>,key:&str,default:&str)->String{profile.and_then(|p|p.config.get(key)).cloned().unwrap_or_else(||default.into())}
pub fn token(profile:Option<&ProviderProfile>,config_key:&str,default_env:&str)->Option<String>{let name=env_name(profile,config_key,default_env);std::env::var(name).ok().filter(|v|!v.is_empty())}
pub fn bearer_headers(profile:Option<&ProviderProfile>,config_key:&str,default_env:&str)->BTreeMap<String,String>{token(profile,config_key,default_env).map_or_else(BTreeMap::new,|v|BTreeMap::from([("Authorization".into(),format!("Bearer {v}"))]))}
pub fn header_token(profile:Option<&ProviderProfile>,config_key:&str,default_env:&str,header:&str)->BTreeMap<String,String>{token(profile,config_key,default_env).map_or_else(BTreeMap::new,|v|BTreeMap::from([(header.into(),v)]))}
pub fn apply_headers(mut request:RequestBuilder,headers:&BTreeMap<String,String>)->RequestBuilder{for(k,v)in headers{request=request.header(k,v);}request}
pub async fn checked(provider:&str,response:Response)->artifactum_core::Result<Response>{let status=response.status();if status.is_success(){return Ok(response)}if matches!(status,StatusCode::UNAUTHORIZED|StatusCode::FORBIDDEN){return Err(access_required(provider,AccessRequirement::Authentication,format!("{provider} returned {status}; authenticate or request access"),None));}let text=response.text().await.unwrap_or_default();Err(provider_error(provider,format!("HTTP {status}: {}",text.chars().take(512).collect::<String>()))) }
pub async fn json(provider:&str,response:Response)->artifactum_core::Result<Value>{checked(provider,response).await?.json().await.map_err(|e|provider_error(provider,e))}
pub fn plan(url:impl Into<String>,headers:BTreeMap<String,String>)->AcquisitionPlan{AcquisitionPlan::Http(HttpAcquisition{url:url.into(),headers,resume:true})}
pub fn encode_segment(value:&str)->String{utf8_percent_encode(value,NON_ALPHANUMERIC).to_string()}
pub fn client()->artifactum_core::Result<reqwest::Client>{reqwest::Client::builder().user_agent(concat!("artifactum/",env!("CARGO_PKG_VERSION"))).build().map_err(|e|provider_error("http-api",e))}
