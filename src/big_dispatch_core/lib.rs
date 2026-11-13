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
use big_cabinet_core::dmeta::Uuid7;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use service_core_rs::declare_service_protocol;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// The operations a minted dispatch handle may invoke: individual NSIDs, and authority
/// prefixes that cover every NSID at or under them (`org.example.photos` covers
/// `org.example.photos.list`, not `org.example.photosynthesis.list`). The authority
/// [`Self::ALL_AUTHORITIES`] covers every NSID. A scope reaches a privileged method (see
/// [`method_core::XrpcMethod::privileged`]) only when `privileged` is set, whatever its names
/// cover: privilege is a separate bit rather than a list of excluded names, so attenuation stays
/// monotone (a scope covers another only when it holds every bit the other does).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DispatchScope {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub operations: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub authorities: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub privileged: bool,
}

impl DispatchScope {
    /// The authority that owns every NSID.
    pub const ALL_AUTHORITIES: &'static str = "*";

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty() && self.authorities.is_empty() && !self.privileged
    }

    /// Whether `nsid` is `authority` itself or lives under `authority.`: the one NSID-prefix
    /// rule for authorities, whole segments only (`org.example.photos` owns
    /// `org.example.photos.list`, not `org.example.photosynthesis`).
    pub fn authority_owns(authority: &str, nsid: &str) -> bool {
        authority == Self::ALL_AUTHORITIES
            || nsid
                .strip_prefix(authority)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
    }

    /// The provider `nsid` is routed to, if any route covers it (ADR 045): `routes` maps an exact
    /// NSID or an authority prefix (whole segments, as [`Self::authority_owns`] matches them) to a
    /// [`Route`], and the longest matching pattern decides, so an exact NSID beats the prefix
    /// over it. The router and whoever predicts what it will resolve share this one rule.
    pub fn route_target<'a>(routes: &'a BTreeMap<String, Route>, nsid: &str) -> Option<&'a Route> {
        routes
            .iter()
            .filter(|(pattern, _)| Self::authority_owns(pattern, nsid))
            .max_by_key(|(pattern, _)| pattern.len())
            .map(|(_, route)| route)
    }

    /// Whether this scope names `nsid`, ignoring privilege: what a nodule's grant is matched
    /// against its requests with, before any method is resolved.
    pub fn permits(&self, nsid: &str) -> bool {
        self.operations.contains(nsid)
            || self
                .authorities
                .iter()
                .any(|a| Self::authority_owns(a, nsid))
    }

    /// Whether a holder of this scope may invoke the method `nsid`, whose declaration says
    /// whether it is `privileged`.
    pub fn admits(&self, nsid: &str, privileged: bool) -> bool {
        self.permits(nsid) && (!privileged || self.privileged)
    }

    /// Whether everything `other` permits is also permitted here: the attenuation check a mint
    /// applies, so a handle can only hand out a narrower scope than it holds.
    pub fn covers(&self, other: &Self) -> bool {
        (!other.privileged || self.privileged)
            && other.operations.iter().all(|nsid| self.permits(nsid))
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
    /// The actor the new handle is bound to; `None` binds it to the presented handle's own
    /// subject. Naming another actor is an introduction: the presented handle needs
    /// `Rights::DELEGATE`, and the actor must be live and a strict descendant of the presented
    /// handle's subject in the actor tree (anyone else is `Escalation`).
    pub subject: Option<Uuid7>,
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
    /// How the call arrived when it came over HTTP; `None` for a call that did not (a direct
    /// client call, a nodule's egress), whose `input` is already the method's JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpForm>,
}

/// The HTTP form of a call through the `/xrpc/<nsid>` surface. Dispatch holds it to the method's
/// kind (a query is a GET, a procedure a POST, as in atproto) and, for a GET, builds the input
/// from the query string by the method's Lexicon parameters, after admitting the caller, so what
/// a method declares is not revealed to a caller dispatch would refuse.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "camelCase")]
pub enum HttpForm {
    /// The JSON body is the call's `input`.
    Post,
    /// The call's `input` is ignored: the query string, still percent-encoded, is the request.
    Get { query: String },
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
/// `RouteTargetMissing` is also dispatch's own: an operator's route (ADR 045) names a provider
/// that does not claim the operation, so the call is neither served by another claimant nor
/// refused for good, and is retried once the target registers.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NotReadyReason {
    Provider(Readiness),
    Ambiguous,
    RouteTargetMissing {
        provider: String,
    },
    /// A route covering the operation is damaged (it does not decode or validate), so the
    /// operation is refused rather than sent to whichever provider claims it; `reason` names the
    /// route and what is wrong with it.
    RouteInvalid {
        reason: String,
    },
}

/// What a route pattern resolves to, as the nodule service hands it to the router.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// Calls go to the provider with this id.
    Provider(String),
    /// The route's stored datum is damaged: calls it covers are refused, with this reason.
    Invalid(String),
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
    #[error("the input is refused: {0}")]
    InvalidInput(String),
    /// The provider answered with a body its method's Lexicon does not describe: a provider bug,
    /// as atproto's `InternalServerError`.
    #[error("the provider's answer is not what its method declares: {0}")]
    InvalidOutput(String),
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
            privileged: false,
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
    fn the_all_authority_owns_every_nsid_and_only_it_covers_itself() {
        let all = scope(&[], &[DispatchScope::ALL_AUTHORITIES]);
        assert!(all.permits("org.example.photos.list"));
        assert!(all.covers(&scope(&[], &["org.example.photos"])));
        assert!(all.covers(&all));
        let some = scope(&[], &["org.example"]);
        assert!(!some.covers(&all));
    }

    #[test]
    fn privilege_is_a_separate_monotone_bit() {
        let mut all = scope(&[], &[DispatchScope::ALL_AUTHORITIES]);
        assert!(all.admits("org.example.daybook.blobs.put", false));
        assert!(!all.admits("org.example.daybook.nodules.admitNodule", true));
        let mut privileged = all.clone();
        privileged.privileged = true;
        assert!(privileged.admits("org.example.daybook.nodules.admitNodule", true));
        // Privilege alone names nothing.
        let bare = DispatchScope {
            privileged: true,
            ..Default::default()
        };
        assert!(!bare.admits("org.example.daybook.nodules.admitNodule", true));
        assert!(privileged.covers(&all));
        assert!(!all.covers(&privileged));
        all.privileged = true;
        assert!(all.covers(&privileged));
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
