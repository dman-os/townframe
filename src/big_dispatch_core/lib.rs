//! The dispatch wire contract (ADR 027 §3, §6; ADR 033 §4): the node's XRPC reverse proxy, seen
//! from its callers.
//!
//! A caller names an operation by NSID and hands dispatch JSON; dispatch admits the caller's
//! [`Authorization`], resolves the provider for the NSID and forwards. Like every service
//! operation, the caller's `Authorization` is not in the request body: it is the request's
//! `Authorization: Bearer` credential (the envelope's HTTP metadata). It is the operator's
//! identity capability, or a handle minted through [`MintDispatchCapability`] that carries a
//! [`DispatchScope`]. Nothing here serves a provider; routing, readiness and the providers
//! themselves are the host's.

use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Rights;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use service_core_rs::declare_service_protocol;
use std::collections::BTreeSet;

/// The operations a minted dispatch handle may invoke: individual NSIDs, and authority
/// prefixes that cover every NSID at or under them (`org.example.photos` covers
/// `org.example.photos.list`, not `org.example.photosynthesis.list`).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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

    /// Whether `nsid` is `authority` itself or lives under `authority.`: the one NSID-prefix
    /// rule for authorities, whole segments only (`org.example.photos` owns
    /// `org.example.photos.list`, not `org.example.photosynthesis`).
    pub fn authority_owns(authority: &str, nsid: &str) -> bool {
        nsid.strip_prefix(authority)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
    }

    /// Whether a holder of this scope may invoke `nsid`.
    pub fn permits(&self, nsid: &str) -> bool {
        self.operations.contains(nsid)
            || self
                .authorities
                .iter()
                .any(|a| Self::authority_owns(a, nsid))
    }

    /// Whether everything `other` permits is also permitted here: the attenuation check a mint
    /// applies, so a handle can only hand out a narrower scope than it holds.
    pub fn covers(&self, other: &Self) -> bool {
        other.operations.iter().all(|nsid| self.permits(nsid))
            && other.authorities.iter().all(|authority| {
                self.authorities
                    .iter()
                    .any(|a| Self::authority_owns(a, authority))
            })
    }
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintDispatchCapability {
    pub scope: DispatchScope,
    pub rights: Rights,
}

/// Whether a provider can serve a call, and if not the first reason in priority order. For a
/// nodule's handler (the nodule inventory's `InstalledNodule`) the order is: refused configuration, then disabled, then a required operation request no grant
/// covers, then a required operation request no provider satisfies, then missing handler
/// artifact bytes, then the
/// absence of a handler executor, then initialization. [`Self::Ready`] is the executor's word
/// that the handler can be called now and the node's word that every initializer of this
/// revision and start has succeeded.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Readiness {
    Ready,
    Refused,
    Disabled,
    /// A required operation request has no covering grant. Not a fault: configuration (granting
    /// the operation) makes the nodule ready. An optional request never causes this.
    NeedsConfiguration,
    /// A required operation request has no provider that satisfies it: none enabled, more than
    /// one, a revision older than the request's `minRevision`, or a different cartridge than the one
    /// the operator consented to. The record's `requests` say which and why. Not a fault: the
    /// nodule is enabled and waits (as a Kubernetes pod waits on a missing dependency, a systemd
    /// unit on `Requires=`), and becomes ready once a provider appears or the operator confirms.
    /// An optional request never causes this.
    ProviderMissing,
    ArtifactsUnavailable,
    ExecutorUnavailable,
    /// The handler is hosted but an initializer of this revision and start is still running or
    /// waiting on the provider of a foreign operation. Retryable: callers are not queued.
    Initializing,
    /// An initializer failed; the record carries the error. It runs again on the next reconcile
    /// of the node (boot, config change, enable).
    InitFailed,
}

impl Readiness {
    pub fn is_ready(self) -> bool {
        self == Self::Ready
    }
}

/// What travels with one call beside its input; providers receive it. The call's deadline,
/// request id and trace are not here: they are the [`service_core_rs::RequestContext`] the call
/// arrives with, and a hop re-derives the remaining budget from it (the `x-timeout-ms` header).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvocationContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Invoke {
    pub operation: String,
    pub input: Value,
    #[serde(default)]
    pub context: InvocationContext,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvokeOutput {
    pub output: Value,
}

/// Why an operation that is routed cannot be served right now. Provider readiness is the
/// [`Readiness`]; `Ambiguous` is dispatch's own, for an NSID that more than
/// one provider implements, which is never resolved by installation order (ADR 027 §5).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NotReadyReason {
    Provider(Readiness),
    Ambiguous,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
    /// The provider's own XRPC error, passed through untouched.
    #[error("the provider failed with status {status}")]
    Provider { status: u16, body: Value },
}

method_core::methods! {
    error = DispatchError, nsid_prefix = "org.example.daybook.dispatch.";
    Invoke => InvokeOutput, procedure, "invoke";
    MintDispatchCapability => Capability, procedure, "mintCapability";
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
