//! The shared request shape (ADR 028 §4): how a manifest names what it uses.
//!
//! Every runtime that consumes a plug (BigNode's nodule, Clay's micro) states its needs in this
//! one vocabulary, so an audit of "what can this plug reach" reads the same everywhere. A request
//! names a contract by NSID, never a provider: whichever enabled provider offers the NSID
//! satisfies it, and the concrete provider, collection or document is configuration. A manifest
//! describes needs, not granted authority; a request confers nothing by itself.

use crate::PlugError;
use big_cabinet_core::datum::Extra;
use big_cabinet_core::datum::is_nsid;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;

/// One thing a plug asks for.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    /// The plug-local name code uses to reach the grant. Unique within one manifest.
    pub binding: String,
    /// The NSID of the contract used: an operation, a datum schema, a collection contract or a
    /// runtime-defined contract.
    pub contract: String,
    /// An open per-contract vocabulary: `read`/`write` for data contracts, absent for an
    /// operation call, where the grant is "may call".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<String>,
    pub necessity: Necessity,
    pub resolution: Resolution,
    /// The least Lexicon `revision` of `contract` that serves the plug. Carried and checked for
    /// shape here; enforcing it is the resolution step's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_revision: Option<u32>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Whether a plug can run without the request being granted.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Necessity {
    Required,
    Optional,
    #[datum(other)]
    Unknown(String),
}

/// How the request gets bound.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Resolution {
    /// Configuration binds it, with defaults derived from the manifest.
    Configured,
    /// Bound later by the user's consent through a picker, issuing the same handle.
    AtRuntime,
    #[datum(other)]
    Unknown(String),
}

impl Necessity {
    /// An unknown member is required: a build that does not understand a relaxation must not
    /// assume the plug can run without the grant.
    pub fn is_required(&self) -> bool {
        !matches!(self, Self::Optional)
    }
}

impl Request {
    /// A required, configured request for `contract` bound as `binding`, with no access.
    pub fn new(binding: impl Into<String>, contract: impl Into<String>) -> Self {
        Self {
            binding: binding.into(),
            contract: contract.into(),
            access: None,
            necessity: Necessity::Required,
            resolution: Resolution::Configured,
            min_revision: None,
            extra: Default::default(),
        }
    }

    /// Whether this asks to call an operation, as opposed to read or write data: an operation
    /// request carries no access.
    pub fn is_operation_call(&self) -> bool {
        self.access.is_none()
    }

    /// Checks a manifest's request list, stated at `field`: each binding is non-empty and unique,
    /// each contract an NSID, an access (when present) and a minimum revision are meaningful.
    /// Two requests for one contract are allowed, since distinct bindings may use it differently.
    /// An unknown necessity or resolution is carried, not refused.
    pub fn validate_all(requests: &[Self], field: &str) -> Result<(), PlugError> {
        let mut bindings = BTreeSet::new();
        for (index, request) in requests.iter().enumerate() {
            let path = format!("{field}[{index}]");
            if request.binding.is_empty() {
                return Err(PlugError::invalid(
                    format!("{path}.binding"),
                    "a binding is not empty",
                ));
            }
            if !bindings.insert(request.binding.as_str()) {
                return Err(PlugError::invalid(
                    format!("{path}.binding"),
                    format!("binding `{}` is declared twice", request.binding),
                ));
            }
            if !is_nsid(&request.contract) {
                return Err(PlugError::invalid(
                    format!("{path}.contract"),
                    format!("`{}` is not an NSID", request.contract),
                ));
            }
            if request.access.as_deref() == Some("") {
                return Err(PlugError::invalid(
                    format!("{path}.access"),
                    "an access is absent or not empty",
                ));
            }
            if request.min_revision == Some(0) {
                return Err(PlugError::invalid(
                    format!("{path}.minRevision"),
                    "a Lexicon revision starts at 1",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_form_is_camel_case_and_unknown_members_are_carried() {
        let request: Request = serde_json::from_value(serde_json::json!({
            "binding": "search",
            "contract": "org.example.search.query",
            "necessity": "someday",
            "resolution": "atRuntime",
            "minRevision": 2,
            "hint": true,
        }))
        .unwrap();
        assert_eq!(request.necessity, Necessity::Unknown("someday".into()));
        assert!(request.necessity.is_required());
        assert_eq!(request.resolution, Resolution::AtRuntime);
        assert_eq!(request.min_revision, Some(2));
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value["necessity"], "someday");
        assert_eq!(value["minRevision"], 2);
        assert_eq!(value["hint"], true);
        assert!(value.get("access").is_none());
        assert!(!Necessity::Optional.is_required());
    }

    #[test]
    fn validation_refuses_with_precise_paths() {
        let refused = |requests: Vec<Request>| {
            Request::validate_all(&requests, "requires")
                .unwrap_err()
                .to_string()
        };
        let ok = Request::new("a", "org.example.a.run");
        Request::validate_all(
            &[ok.clone(), Request::new("b", "org.example.a.run")],
            "requires",
        )
        .unwrap();
        assert!(
            refused(vec![ok.clone(), Request::new("a", "org.example.b.run")])
                .contains("requires[1].binding")
        );
        assert!(
            refused(vec![Request::new("", "org.example.a.run")]).contains("requires[0].binding")
        );
        assert!(refused(vec![Request::new("a", "not an nsid")]).contains("requires[0].contract"));
        let mut empty_access = ok.clone();
        empty_access.access = Some(String::new());
        assert!(refused(vec![empty_access]).contains("requires[0].access"));
        let mut zero = ok;
        zero.min_revision = Some(0);
        assert!(refused(vec![zero]).contains("requires[0].minRevision"));
    }
}
