//! Generic multiplexable JSON-lines protocol shared by provider, executor and
//! verifier plugin families. No Rust dylib ABI is involved.

use serde::{Deserialize,Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncBufRead,AsyncBufReadExt,AsyncWrite,AsyncWriteExt};
use uuid::Uuid;

pub const PROTOCOL_VERSION:u32=3;
#[derive(Debug,Error)]pub enum Error{#[error("I/O error: {0}")]Io(#[from]std::io::Error),#[error("JSON error: {0}")]Json(#[from]serde_json::Error),#[error("protocol version mismatch: {0}")]Version(u32),#[error("plugin error: {message}")]Remote{message:String,data:Option<Value>},#[error("unexpected EOF")]Eof}
pub type Result<T,E=Error>=std::result::Result<T,E>;
#[derive(Clone,Debug,Serialize,Deserialize)]#[serde(rename_all="snake_case")]pub enum PluginKind{Provider,Executor,Verifier,Publisher}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct PluginDescriptor{pub protocol:u32,pub kind:PluginKind,pub name:String,pub version:String,#[serde(default)]pub capabilities:Vec<String>}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct Request{pub id:Uuid,pub method:String,#[serde(default)]pub params:Value}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct Response{pub id:Uuid,pub ok:bool,#[serde(default)]pub result:Value,#[serde(default,skip_serializing_if="Option::is_none")]pub error:Option<String>,#[serde(default,skip_serializing_if="Option::is_none")]pub error_data:Option<Value>}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct Notification{pub method:String,#[serde(default)]pub params:Value}
#[derive(Clone,Debug,Serialize,Deserialize)]#[serde(tag="type",rename_all="snake_case")]pub enum Frame{Request(Request),Response(Response),Notification(Notification)}

pub async fn write_frame<W:AsyncWrite+Unpin>(w:&mut W,frame:&Frame)->Result<()> {let mut b=serde_json::to_vec(frame)?;b.push(b'\n');w.write_all(&b).await?;w.flush().await?;Ok(())}
pub async fn read_frame<R:AsyncBufRead+Unpin>(r:&mut R)->Result<Frame>{let mut line=String::new();if r.read_line(&mut line).await?==0{return Err(Error::Eof)}Ok(serde_json::from_str(&line)?)}
pub fn request(method:impl Into<String>,params:Value)->Request{Request{id:Uuid::new_v4(),method:method.into(),params}}
pub fn success(id:Uuid,result:Value)->Response{Response{id,ok:true,result,error:None,error_data:None}}
pub fn failure(id:Uuid,error:impl Into<String>)->Response{Response{id,ok:false,result:Value::Null,error:Some(error.into()),error_data:None}}
pub fn failure_data(id:Uuid,error:impl Into<String>,data:Value)->Response{Response{id,ok:false,result:Value::Null,error:Some(error.into()),error_data:Some(data)}}

#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn structured_remote_error_roundtrips(){
        let id=Uuid::new_v4();let response=failure_data(id,"access required",serde_json::json!({"kind":"access_challenge","challenge":{"requirement":"license_acceptance"}}));let bytes=serde_json::to_vec(&Frame::Response(response)).unwrap();let frame:Frame=serde_json::from_slice(&bytes).unwrap();match frame{Frame::Response(response)=>{assert!(!response.ok);assert_eq!(response.error_data.unwrap()["challenge"]["requirement"],"license_acceptance");},_=>panic!("wrong frame")}
    }
}
