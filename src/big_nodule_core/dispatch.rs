//! The dispatch wire contract (ADR 027 §3, §6): the nodule runtime's XRPC reverse proxy, seen
//! from its callers.
//!
//! A caller names an operation by NSID and hands dispatch JSON; dispatch admits the caller's
//! [`Authorization`], resolves the provider for the NSID and forwards. Like
//! [`crate::protocol`], every operation carries an `Authorization`: the operator's identity
//! capability, or a handle minted through [`MintDispatchCapability`] that carries a
//! [`DispatchScope`]. Nothing here serves a provider; routing, readiness and the providers
//! themselves are the host's.

use crate::Nodule;
use crate::inventory::Readiness;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Rights;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use service_core_rs::ContextError;
use service_core_rs::Operation;
use service_core_rs::XrpcOperation;
use service_core_rs::declare_service_protocol;
use std::collections::BTreeSet;

/// The operations a minted dispatch handle may invoke: individual NSIDs, and authority
/// prefixes that cover every NSID at or under them (`org.example.photos` covers
/// `org.example.photos.list`, not `org.example.photosynthesis.list`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DispatchScope {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub operations: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub authorities: BTreeSet<String>,
}

impl DispatchScope {
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty() && self.authorities.is_empty()
    }

    /// Whether a holder of this scope may invoke `nsid`.
    pub fn permits(&self, nsid: &str) -> bool {
        self.operations.contains(nsid)
            || self
                .authorities
                .iter()
                .any(|a| Nodule::authority_owns(a, nsid))
    }

    /// Whether everything `other` permits is also permitted here: the attenuation check a mint
    /// applies, so a handle can only hand out a narrower scope than it holds.
    pub fn covers(&self, other: &Self) -> bool {
        other.operations.iter().all(|nsid| self.permits(nsid))
            && other.authorities.iter().all(|authority| {
                self.authorities
                    .iter()
                    .any(|a| Nodule::authority_owns(a, authority))
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintDispatchCapability {
    pub authorization: Authorization,
    pub scope: DispatchScope,
    pub rights: Rights,
}

/// What travels with one call beside its input; providers receive it, and the host enforces
/// `deadline_unix_ms` around the provider call.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvocationContext {
    /// Wall-clock deadline in milliseconds since the Unix epoch; it is the wire form
    /// [`service_core_rs::RequestContext`] also uses, so it survives a hop between processes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Invoke {
    pub authorization: Authorization,
    pub operation: String,
    pub input: Value,
    #[serde(default)]
    pub context: InvocationContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvokeOutput {
    pub output: Value,
}

/// Why an operation that is routed cannot be served right now. Provider readiness is the
/// nodule inventory's [`Readiness`]; `Ambiguous` is dispatch's own, for an NSID that more than
/// one provider implements, which is never resolved by installation order (ADR 027 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NotReadyReason {
    Provider(Readiness),
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum DispatchError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    #[error("the presented capability does not permit this operation")]
    Denied,
    #[error("authority failed: {0}")]
    Authority(AuthorityServiceError),
    #[error("authority transport failed: {0}")]
    AuthorityUnavailable(String),
    #[error("no provider implements the operation")]
    UnknownOperation,
    /// Retryable: the provider exists but is not serving yet.
    #[error("the operation's provider is not ready: {reason:?}")]
    NotReady { reason: NotReadyReason },
    #[error("the provider refused the input: {0}")]
    InvalidInput(String),
    #[error("the call exceeded its deadline")]
    DeadlineExceeded,
    /// The provider's own XRPC error, passed through untouched.
    #[error("the provider failed with status {status}")]
    Provider { status: u16, body: Value },
}

macro_rules! operations {
    ($($request:ident => $output:ty, $nsid:literal;)*) => {$(
        impl Operation for $request {
            type Output = $output;
            type Error = DispatchError;
            const XRPC: XrpcOperation =
                XrpcOperation::procedure(concat!("org.example.daybook.dispatch.", $nsid))
                    .with_schemas(stringify!($output), stringify!(DispatchError));
        }
    )*};
}
operations! {
    Invoke => InvokeOutput, "invoke";
    MintDispatchCapability => Capability, "mintCapability";
}

declare_service_protocol! {
    pub enum DispatchProtocol => DispatchMessage {
        unary {
            Invoke(Invoke),
            MintDispatchCapability(MintDispatchCapability)
        }
        stream {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(operations: &[&str], authorities: &[&str]) -> DispatchScope {
        DispatchScope {
            operations: operations.iter().map(|s| (*s).to_owned()).collect(),
            authorities: authorities.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    #[test]
    fn authority_prefixes_match_whole_segments_only() {
        let held = scope(&["org.example.search.query"], &["org.example.photos"]);
        assert!(held.permits("org.example.search.query"));
        assert!(held.permits("org.example.photos"));
        assert!(held.permits("org.example.photos.list"));
        assert!(!held.permits("org.example.photosynthesis.list"));
        assert!(!held.permits("org.example.search.other"));
    }

    #[test]
    fn covers_is_attenuation_only() {
        let held = scope(&["org.example.search.query"], &["org.example.photos"]);
        assert!(held.covers(&DispatchScope::default()));
        assert!(held.covers(&scope(&["org.example.photos.list"], &[])));
        assert!(held.covers(&scope(&[], &["org.example.photos.albums"])));
        assert!(held.covers(&held));
        assert!(!held.covers(&scope(&["org.example.search.other"], &[])));
        assert!(!held.covers(&scope(&[], &["org.example.search"])));
        // An operation scope never covers an authority prefix, even over the same name.
        assert!(!held.covers(&scope(&[], &["org.example.search.query"])));
        assert!(!held.covers(&scope(&[], &["org.example"])));
    }
}
