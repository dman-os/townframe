use http::HeaderMap;
use http::HeaderValue;
use http::StatusCode;
use method_core::XrpcMethod;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

pub const REQUEST_ID_HEADER: &str = "x-request-id";
pub const TRACEPARENT_HEADER: &str = "traceparent";
pub const AUTHORIZATION_HEADER: &str = "authorization";
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
pub const TIMEOUT_HEADER: &str = "x-timeout-ms";
pub const DOMAIN_ERROR_FIELD: &str = "operationError";
pub const DOMAIN_ERROR_STATUS: StatusCode = StatusCode::BAD_REQUEST;
/// The `Authorization` scheme of every XRPC hop, including its separating space.
const BEARER_SCHEME: &str = "Bearer ";

/// The `Authorization` header value presenting `token` as a bearer credential.
pub fn bearer_header_value(token: &str) -> String {
    format!("{BEARER_SCHEME}{token}")
}

/// The token of an `Authorization: Bearer <token>` header value; the scheme is matched case
/// insensitively (RFC 9110). `None` for any other scheme. This is the one parser of the scheme,
/// shared by every XRPC server and by guests.
pub fn bearer_token(value: &str) -> Option<&str> {
    value
        .get(..BEARER_SCHEME.len())
        .filter(|scheme| scheme.eq_ignore_ascii_case(BEARER_SCHEME))
        .map(|_| &value[BEARER_SCHEME.len()..])
}

/// Encodes a typed method error as the standard XRPC error envelope: `error` is the method's
/// error schema name and the serialized error travels under [`DOMAIN_ERROR_FIELD`], answered
/// with [`DOMAIN_ERROR_STATUS`]. A non-serializable error is a programming error in the error
/// type.
pub fn domain_error_envelope<E>(method: XrpcMethod, error: &E) -> XrpcErrorEnvelope
where
    E: Serialize + std::fmt::Debug,
{
    let value = serde_json::to_value(error).expect("method errors must serialize");
    XrpcErrorEnvelope::new(
        method.error_schema.to_owned(),
        Some(format!("{error:?}")),
        BTreeMap::from([(DOMAIN_ERROR_FIELD.to_owned(), value)]),
    )
    .expect("method error detail is not reserved")
}

/// Parses the headers every XRPC hop carries into the call's context and metadata; the inverse
/// of `service_client_core_rs::xrpc::write_context_headers`. The budget is the relative
/// `x-timeout-ms`, started from this hop's own clock.
pub fn request_context(
    headers: &HeaderMap,
) -> Result<(crate::RequestContext, crate::HttpMetadata), String> {
    let mut context = crate::RequestContext::root();
    if let Some(value) = header_string(headers, REQUEST_ID_HEADER)? {
        context.request_id = value
            .parse()
            .map_err(|_| "x-request-id must be a UUID".to_owned())?;
    }
    context.trace = crate::TraceContext::from_headers(
        header_string(headers, TRACEPARENT_HEADER)?.as_deref(),
        header_string(headers, "tracestate")?.as_deref(),
    );
    if let Some(value) = header_string(headers, TIMEOUT_HEADER)? {
        let millis = value
            .parse::<u64>()
            .map_err(|_| "x-timeout-ms must be an unsigned integer".to_owned())?;
        context = context.with_time_budget(Duration::from_millis(millis));
    }
    Ok((
        context,
        crate::HttpMetadata {
            authorization: header_string(headers, AUTHORIZATION_HEADER)?,
            idempotency_key: header_string(headers, IDEMPOTENCY_KEY_HEADER)?,
        },
    ))
}

fn header_string(headers: &HeaderMap, name: &str) -> Result<Option<String>, String> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| format!("{name} must be valid UTF-8 HTTP text"))
        })
        .transpose()
}

impl crate::ResponseMetadata {
    pub fn write_headers(&self, headers: &mut HeaderMap) {
        for (name, value) in [
            ("etag", &self.etag),
            ("location", &self.location),
            ("cache-control", &self.cache_control),
            ("retry-after", &self.retry_after),
        ] {
            if let Some(value) = value {
                headers.insert(
                    name,
                    HeaderValue::from_str(value).expect("response metadata must be valid HTTP"),
                );
            }
        }
        for warning in &self.warnings {
            headers.append(
                "warning",
                HeaderValue::from_str(warning).expect("warning must be valid HTTP"),
            );
        }
    }

    pub fn read_headers(headers: &HeaderMap) -> Result<Self, String> {
        let text = |name: &str| {
            headers
                .get(name)
                .map(|value| {
                    value
                        .to_str()
                        .map(str::to_owned)
                        .map_err(|error| error.to_string())
                })
                .transpose()
        };
        Ok(Self {
            etag: text("etag")?,
            location: text("location")?,
            cache_control: text("cache-control")?,
            retry_after: text("retry-after")?,
            warnings: headers
                .get_all("warning")
                .iter()
                .map(|value| {
                    value
                        .to_str()
                        .map(str::to_owned)
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XrpcErrorEnvelope {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(flatten)]
    details: BTreeMap<String, Value>,
}

impl XrpcErrorEnvelope {
    pub fn new(
        error: String,
        message: Option<String>,
        details: BTreeMap<String, Value>,
    ) -> Result<Self, XrpcDomainDetailsError> {
        for reserved in ["error", "message"] {
            if details.contains_key(reserved) {
                return Err(XrpcDomainDetailsError::ReservedField(reserved));
            }
        }
        Ok(Self {
            error,
            message,
            details,
        })
    }
    pub fn error(&self) -> &str {
        &self.error
    }
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
    pub fn details(&self) -> &BTreeMap<String, Value> {
        &self.details
    }
    pub fn into_parts(self) -> (String, Option<String>, BTreeMap<String, Value>) {
        (self.error, self.message, self.details)
    }
}

impl<'de> serde::Deserialize<'de> for XrpcErrorEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("XRPC error envelope must be an object"))?;
        let error = object.get("error").and_then(Value::as_str).ok_or_else(|| {
            serde::de::Error::custom("XRPC error envelope error must be a string")
        })?;
        let message = object
            .get("message")
            .map(|value| {
                value.as_str().map(str::to_owned).ok_or_else(|| {
                    serde::de::Error::custom("XRPC error envelope message must be a string")
                })
            })
            .transpose()?;
        let details = object
            .iter()
            .filter(|(key, _)| key.as_str() != "error" && key.as_str() != "message")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(Self {
            error: error.to_owned(),
            message,
            details,
        })
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum XrpcDomainDetailsError {
    #[error("XRPC domain details cannot redefine reserved field {0}")]
    ReservedField(&'static str),
}
