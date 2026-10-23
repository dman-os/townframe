use http::{HeaderMap, HeaderValue, StatusCode};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

pub const REQUEST_ID_HEADER: &str = "x-request-id";
pub const TRACEPARENT_HEADER: &str = "traceparent";
pub const AUTHORIZATION_HEADER: &str = "authorization";
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
pub const TIMEOUT_HEADER: &str = "x-timeout-ms";
pub const DOMAIN_ERROR_FIELD: &str = "operationError";
pub const DOMAIN_ERROR_STATUS: StatusCode = StatusCode::BAD_REQUEST;

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
