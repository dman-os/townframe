//! Client for ADR 042's native descriptor socket.
//!
//! A call owns one connection: it sends one length-prefixed request with all descriptors attached
//! to the first `sendmsg`, then reads one response until the node closes the connection.

#![cfg(unix)]

use serde::de::DeserializeOwned;
use serde_json::Value;
use service_core_rs::descriptor;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::io::Interest;
use tokio::net::UnixStream;

#[derive(Debug, Error)]
pub enum DescriptorClientError {
    #[error("descriptor socket IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("descriptor socket response was invalid: {0}")]
    Protocol(String),
    #[error("descriptor call was refused: {0}")]
    Refused(Value),
    #[error("descriptor result was invalid: {0}")]
    Result(String),
}

/// A client for one node's `<node id>.fd.sock`.
#[derive(Debug, Clone)]
pub struct DescriptorClient {
    path: PathBuf,
}

impl DescriptorClient {
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Sends a descriptor batch and decodes each result in its original descriptor order.
    ///
    /// ```no_run
    /// # #[cfg(unix)]
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = service_client_native_rs::DescriptorClient::unix("/run/day/node.fd.sock");
    /// let results: Vec<serde_json::Value> = client
    ///     .call_files("org.example.daybook.blobs.importFile", &serde_json::json!({
    ///         "capability": "capability-id",
    ///     }), &["one.txt".into(), "two.txt".into()])
    ///     .await?;
    /// # let _ = results;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call<T: DeserializeOwned>(
        &self,
        operation: &str,
        parameters: &Value,
        descriptors: Vec<OwnedFd>,
    ) -> Result<Vec<T>, DescriptorClientError> {
        if descriptors.len() > descriptor::MAX_DESCRIPTORS {
            return Err(DescriptorClientError::Protocol(
                "too many descriptors".to_owned(),
            ));
        }
        let frame = descriptor::encode_request(operation, parameters, descriptors.len())?;
        let mut stream = UnixStream::connect(&self.path).await?;
        send_request(&stream, &frame, &descriptors).await?;
        let response = read_response(&mut stream).await?;
        let (status, count, payload) = descriptor::decode_response(&response)
            .map_err(|error| DescriptorClientError::Protocol(error.to_string()))?;
        if status != 0 {
            return Err(DescriptorClientError::Refused(payload));
        }
        let values = payload
            .get("results")
            .and_then(Value::as_array)
            .or_else(|| payload.as_array())
            .ok_or_else(|| {
                DescriptorClientError::Protocol("successful response is not an array".to_owned())
            })?;

        if values.len() != count {
            return Err(DescriptorClientError::Protocol(
                "response result count does not match its array".to_owned(),
            ));
        }
        values
            .iter()
            .cloned()
            .map(|value| {
                serde_json::from_value(value)
                    .map_err(|error| DescriptorClientError::Result(error.to_string()))
            })
            .collect()
    }

    pub async fn call_files<T: DeserializeOwned>(
        &self,
        operation: &str,
        parameters: &Value,
        paths: &[PathBuf],
    ) -> Result<Vec<T>, DescriptorClientError> {
        let descriptors = paths
            .iter()
            .map(std::fs::File::open)
            .map(|file| file.map(Into::into))
            .collect::<Result<Vec<OwnedFd>, _>>()?;
        self.call(operation, parameters, descriptors).await
    }
}

async fn send_request(
    stream: &UnixStream,
    frame: &[u8],
    descriptors: &[OwnedFd],
) -> Result<(), DescriptorClientError> {
    let mut offset = 0;
    let mut attach = true;
    while offset < frame.len() {
        let sent = stream
            .async_io(Interest::WRITABLE, || {
                let sent = if attach {
                    descriptor::send_part(stream, &frame[offset..], descriptors)
                } else {
                    descriptor::send_part(stream, &frame[offset..], &[])
                }?;
                attach = false;
                Ok(sent)
            })
            .await?;
        if sent == 0 {
            return Err(DescriptorClientError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "descriptor request made no progress",
            )));
        }
        offset += sent;
    }
    Ok(())
}

async fn read_response(stream: &mut UnixStream) -> Result<Vec<u8>, DescriptorClientError> {
    let mut prefix = [0; 4];
    stream.read_exact(&mut prefix).await?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > descriptor::MAX_HEADER_OCTETS {
        return Err(DescriptorClientError::Protocol(
            "descriptor response is too long".to_owned(),
        ));
    }
    let mut response = Vec::with_capacity(length + 4);
    response.extend_from_slice(&prefix);
    response.resize(length + 4, 0);
    stream.read_exact(&mut response[4..]).await?;
    let mut extra = [0; 1];
    if stream.read(&mut extra).await? != 0 {
        return Err(DescriptorClientError::Protocol(
            "descriptor response had trailing bytes".to_owned(),
        ));
    }
    Ok(response)
}
