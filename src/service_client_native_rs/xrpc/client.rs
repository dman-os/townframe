use super::ws::WsIo;
use super::ws::WsMessage;
use futures::StreamExt;
use method_core::MethodKind;
use method_core::Subscription;
use method_core::XrpcMethod;
use reqwest::Url;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use service_client_core_rs::xrpc::XrpcClientError;
use service_client_core_rs::xrpc::decode_response;
use service_client_core_rs::xrpc::encode_request;
use service_core_rs::EventFrame;
use service_core_rs::HttpMetadata;
use service_core_rs::RequestContext;
use service_core_rs::ResponseEnvelope;
use service_core_rs::ResponseMetadata;
use service_core_rs::xrpc::AUTHORIZATION_HEADER;
use service_core_rs::xrpc::REQUEST_ID_HEADER;

#[derive(Clone, Debug)]
pub struct XrpcClient {
    client: reqwest::Client,
    base_url: Url,
    max_response_bytes: usize,
    /// Set when every connection, the subscription handshake's included, goes to this Unix
    /// socket instead of the host in `base_url`.
    #[cfg(unix)]
    unix_socket: Option<std::path::PathBuf>,
}

/// A byte stream a subscription handshake runs over: TCP or a Unix socket.
trait HandshakeIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> HandshakeIo for T {}

impl XrpcClient {
    pub fn http_client(&self) -> &reqwest::Client {
        &self.client
    }
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// A client for the node listening on the Unix socket at `path`. Requests are HTTP/1 over the
    /// socket; the URL only supplies the `Host` header and path prefix, so it is fixed.
    #[cfg(unix)]
    pub fn unix(
        path: impl Into<std::path::PathBuf>,
        max_response_bytes: usize,
    ) -> Result<Self, XrpcClientError<Value>> {
        let path = path.into();
        let client = reqwest::Client::builder()
            .unix_socket(path.as_path())
            .build()
            .map_err(|error| XrpcClientError::Transport {
                message: error.to_string(),
                outcome_unknown: false,
            })?;
        let mut this = Self::with_client(
            client,
            Url::parse("http://localhost/").expect("a constant URL parses"),
            max_response_bytes,
        )?;
        this.unix_socket = Some(path);
        Ok(this)
    }

    pub fn new(base_url: Url, max_response_bytes: usize) -> Result<Self, XrpcClientError<Value>> {
        if max_response_bytes == 0 {
            return Err(XrpcClientError::Encode(
                "response body limit must be positive".to_owned(),
            ));
        }
        let client =
            reqwest::Client::builder()
                .build()
                .map_err(|error| XrpcClientError::Transport {
                    message: error.to_string(),
                    outcome_unknown: false,
                })?;
        Self::with_client(client, base_url, max_response_bytes)
    }

    pub fn with_client(
        client: reqwest::Client,
        base_url: Url,
        max_response_bytes: usize,
    ) -> Result<Self, XrpcClientError<Value>> {
        if max_response_bytes == 0 {
            return Err(XrpcClientError::Encode(
                "response body limit must be positive".to_owned(),
            ));
        }
        Ok(Self {
            client,
            base_url,
            max_response_bytes,
            #[cfg(unix)]
            unix_socket: None,
        })
    }

    pub async fn execute<P, O, E>(
        &self,
        operation: XrpcMethod,
        context: RequestContext,
        metadata: &HttpMetadata,
        params: &P,
    ) -> Result<O, XrpcClientError<E>>
    where
        P: Serialize,
        O: DeserializeOwned,
        E: DeserializeOwned,
    {
        self.execute_with_metadata(operation, context, metadata, params)
            .await
            .map(|response| response.output)
    }

    pub async fn execute_with_metadata<P, O, E>(
        &self,
        operation: XrpcMethod,
        context: RequestContext,
        metadata: &HttpMetadata,
        params: &P,
    ) -> Result<ResponseEnvelope<O>, XrpcClientError<E>>
    where
        P: Serialize,
        O: DeserializeOwned,
        E: DeserializeOwned,
    {
        if operation.kind == MethodKind::Subscription {
            return Err(XrpcClientError::Protocol {
                message: "the unary HTTP adapter does not support subscriptions".to_owned(),
                outcome_unknown: false,
            });
        }
        let encoded = encode_request(operation, &context, metadata, params)
            .map_err(XrpcClientError::Encode)?;
        let url = self
            .base_url
            .join(&encoded.path)
            .map_err(|error| XrpcClientError::Encode(error.to_string()))?;
        // No reqwest timeout: `Transport` owns the deadline, so a spent budget surfaces as
        // `ContextError::DeadlineExceeded` and not as a racing transport error.
        let response = self
            .client
            .request(encoded.method, url)
            .headers(encoded.headers)
            .body(encoded.body)
            .send()
            .await
            .map_err(|error| XrpcClientError::Transport {
                message: error.to_string(),
                outcome_unknown: operation.kind == MethodKind::Procedure,
            })?;
        let status = response.status();
        let response_headers = response.headers().clone();
        let body = read_response_body(response, self.max_response_bytes)
            .await
            .map_err(|(message, outcome_unknown)| XrpcClientError::Protocol {
                message,
                outcome_unknown: outcome_unknown || operation.kind == MethodKind::Procedure,
            })?;
        decode_response(
            operation,
            context.request_id,
            status,
            &response_headers,
            &body,
        )
    }

    pub async fn subscribe<P>(
        &self,
        context: RequestContext,
        metadata: &HttpMetadata,
        params: &P,
        buffer: usize,
    ) -> Result<
        irpc::channel::mpsc::Receiver<Result<EventFrame<P::Event>, P::Error>>,
        XrpcClientError<P::Error>,
    >
    where
        P: Subscription,
    {
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;
        let operation = P::XRPC;
        let query = serde_urlencoded::to_string(params)
            .map_err(|error| XrpcClientError::Encode(error.to_string()))?;
        let mut url = self.base_url.clone();
        url.set_path(&format!(
            "{}/xrpc/{}",
            url.path().trim_end_matches('/'),
            operation.nsid
        ));
        url.set_query((!query.is_empty()).then_some(query.as_str()));
        let host = url
            .host_str()
            .ok_or_else(|| XrpcClientError::Encode("subscription URL has no host".to_owned()))?
            .to_owned();
        let port = url
            .port_or_known_default()
            .ok_or_else(|| XrpcClientError::Encode("subscription URL has no port".to_owned()))?;
        if url.scheme() != "http" {
            return Err(XrpcClientError::Protocol {
                message: "websocket subscriptions require an http base URL (wss is not supported)"
                    .to_owned(),
                outcome_unknown: false,
            });
        }
        let connect_failed = |error: std::io::Error| XrpcClientError::Transport {
            message: error.to_string(),
            outcome_unknown: false,
        };
        #[cfg(unix)]
        let unix_socket = self.unix_socket.as_deref();
        #[cfg(not(unix))]
        let unix_socket: Option<&std::path::Path> = None;
        let mut stream: Box<dyn HandshakeIo> = match unix_socket {
            #[cfg(unix)]
            Some(path) => Box::new(
                tokio::net::UnixStream::connect(path)
                    .await
                    .map_err(connect_failed)?,
            ),
            #[cfg(not(unix))]
            Some(_) => unreachable!("a Unix socket client exists only on Unix"),
            None => Box::new(
                tokio::net::TcpStream::connect((host.as_str(), port))
                    .await
                    .map_err(connect_failed)?,
            ),
        };
        let key = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            uuid::Uuid::now_v7().as_bytes(),
        );
        let path = match url.query() {
            Some(query) => format!("{}?{}", url.path(), query),
            None => url.path().to_owned(),
        };
        let mut request = format!(
            "GET {path} HTTP/1.1\r\nhost: {host}:{port}\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-key: {key}\r\nsec-websocket-version: 13\r\n{REQUEST_ID_HEADER}: {}\r\n",
            context.request_id
        );
        if let Some(value) = &metadata.authorization {
            request.push_str(&format!("{AUTHORIZATION_HEADER}: {value}\r\n"));
        }
        request.push_str("\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|error| XrpcClientError::Transport {
                message: error.to_string(),
                outcome_unknown: false,
            })?;
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if stream.read_exact(&mut byte).await.is_err() || head.len() > 16 * 1024 {
                return Err(XrpcClientError::Transport {
                    message: "websocket handshake failed".to_owned(),
                    outcome_unknown: false,
                });
            }
            head.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&head).into_owned();
        if !head.starts_with("HTTP/1.1 101") {
            return Err(XrpcClientError::Transport {
                message: format!(
                    "websocket handshake refused: {}",
                    head.lines().next().unwrap_or_default()
                ),
                outcome_unknown: false,
            });
        }
        let (tx, rx) = irpc::channel::mpsc::channel(buffer.max(1));
        tokio::spawn(async move {
            let mut socket = WsIo::new(stream, true);
            loop {
                match socket.read_message().await {
                    Ok(Some(WsMessage::Text(text))) => {
                        let Ok(value) = serde_json::from_str::<Value>(&text) else {
                            break;
                        };
                        if let Some(open) = value.get("open") {
                            let metadata: ResponseMetadata = open
                                .get("metadata")
                                .cloned()
                                .and_then(|metadata| serde_json::from_value(metadata).ok())
                                .unwrap_or_default();
                            if tx.send(Ok(EventFrame::Open(metadata))).await.is_err() {
                                break;
                            }
                        } else if let Some(event) = value.get("event") {
                            match serde_json::from_value::<P::Event>(event.clone()) {
                                Ok(event) => {
                                    if tx.send(Ok(EventFrame::Event(event))).await.is_err() {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        } else if let Some(error) = value.get("error") {
                            if let Ok(error) = serde_json::from_value::<P::Error>(error.clone()) {
                                drop(tx.send(Err(error)).await);
                            }
                            break;
                        }
                    }
                    Ok(Some(WsMessage::Close)) | Ok(None) | Err(_) => {
                        drop(tx.send(Ok(EventFrame::End)).await);
                        break;
                    }
                }
            }
        });
        Ok(rx)
    }
}

async fn read_response_body(
    response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, (String, bool)> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err((
            "XRPC response body exceeds configured limit".to_owned(),
            false,
        ));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| (error.to_string(), true))?;
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err((
                "XRPC response body exceeds configured limit".to_owned(),
                true,
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
