//! The node seam: what the review micro asks of the node. A micro reaches the node's methods
//! (`readDatums`, `pollDatums`) through [`NodeCalls`], which the micro owns:
//! [`ContextNode`] is the real implementation over `clay_mdk::Context::call`, and
//! [`crate::fixture::Fixture`] answers the same methods from memory for tests and the standalone
//! lab.
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use clay_mdk::ClientError;
use clay_mdk::Context;
use clay_mdk::FacetState;
use method_core::Unary;
use micro_review_manifest::ReviewConfig;
use std::future::Future;

/// Why a call did not return its answer: all are the edge's failures to report, not invariants.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CallError {
    /// The node answered with the method's error (or refused the call).
    #[error("the node refused the call: {0}")]
    Refused(String),
    /// The call never reached the node.
    #[error("the call did not reach the node: {0}")]
    Transport(String),
    /// The micro has no usable configuration entry yet (see [`Configuration`]).
    #[error("the review micro is not configured: {0}")]
    Unconfigured(String),
}

impl CallError {
    /// What an alert says of the failure: a short heading, and the detail the node or the
    /// transport gave.
    pub fn parts(&self) -> (&'static str, &str) {
        match self {
            Self::Refused(detail) => ("The node refused the request", detail),
            Self::Transport(detail) => ("The node could not be reached", detail),
            Self::Unconfigured(detail) => ("The micro is not configured", detail),
        }
    }
}

impl<E: std::fmt::Display> From<ClientError<E>> for CallError {
    fn from(error: ClientError<E>) -> Self {
        match error {
            ClientError::Method(refusal) => Self::Refused(refusal.to_string()),
            ClientError::Transport { message, .. } => Self::Transport(message),
        }
    }
}

/// What the micro reads where, parsed from its configuration entry ([`ReviewConfig`]).
///
/// INTERIM: the composition mints `doc_cap` for the micro's actor and writes this entry at
/// install. It becomes `#drawers` permbinds plus the 6.2 domain handle.
#[derive(Debug, Clone, PartialEq)]
pub struct Configuration {
    /// The drawer the imported pull requests live in.
    pub github_drawer: DocId,
    /// The drawers the PRs' intxts live in; the scope `doc_cap` was minted over, with
    /// `github_drawer`.
    pub intxt_drawers: Vec<DocId>,
    /// The capability every `readDatums` and `pollDatums` carries. Never minted here.
    pub doc_cap: CapId,
}

impl Configuration {
    /// The configuration from its datum's text. The entry is written by a person or a
    /// composition, so a malformed one is reported, not trusted.
    pub fn parse(config: &ReviewConfig) -> Result<Self, CallError> {
        let drawer = |text: &String| {
            text.parse::<DocId>().map_err(|error| {
                CallError::Unconfigured(format!("drawer id {text:?} is not valid: {error}"))
            })
        };
        Ok(Self {
            github_drawer: drawer(&config.github_drawer)?,
            intxt_drawers: config
                .intxt_drawers
                .iter()
                .map(drawer)
                .collect::<Result<_, _>>()?,
            doc_cap: serde_json::from_value(serde_json::Value::String(config.doc_cap.clone()))
                .map_err(|error| {
                    CallError::Unconfigured(format!(
                        "capability {:?} is not valid: {error}",
                        config.doc_cap
                    ))
                })?,
        })
    }
}

/// The micro's view of the node. `Clone + PartialEq` because each task the micro spawns holds its
/// own handle and the view restarts its reads when the node it reads through changes (a
/// configuration entry written after the micro mounted).
pub trait NodeCalls: Clone + PartialEq + 'static {
    /// One call of the node method `M`: `clay_mdk::Context::call`'s own signature, so the real
    /// implementation is `self.context.call(input)`.
    fn call<M: Unary>(
        &self,
        input: M,
    ) -> impl Future<Output = Result<M::Output, ClientError<M::Error>>>;

    /// Where to read, and the capability to read with.
    fn configuration(&self) -> Result<Configuration, CallError>;
}

/// The real node: calls go through the micro's `Context` (a `wasi:http` request to dispatch for a
/// guest, answered as this micro's actor), and the configuration is the facet's state at the
/// render that built this value.
#[derive(Clone)]
pub struct ContextNode {
    context: Context,
    config: FacetState<ReviewConfig>,
}

impl ContextNode {
    pub fn new(context: Context, config: FacetState<ReviewConfig>) -> Self {
        Self { context, config }
    }
}

/// The context is one for the life of the activation, so two nodes differ only by what they
/// would read.
impl PartialEq for ContextNode {
    fn eq(&self, other: &Self) -> bool {
        self.config == other.config
    }
}

impl NodeCalls for ContextNode {
    fn call<M: Unary>(
        &self,
        input: M,
    ) -> impl Future<Output = Result<M::Output, ClientError<M::Error>>> {
        self.context.call(input)
    }

    fn configuration(&self) -> Result<Configuration, CallError> {
        match &self.config {
            FacetState::Loading => Err(CallError::Unconfigured(
                "the configuration is still loading".to_owned(),
            )),
            FacetState::Absent => Err(CallError::Unconfigured(
                "no configuration entry was written at install".to_owned(),
            )),
            FacetState::Present(config) => Configuration::parse(config),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ReviewConfig {
        ReviewConfig {
            github_drawer: "z11111111111111111111111111111191".to_owned(),
            intxt_drawers: vec!["z11111111111111111111111111111192".to_owned()],
            doc_cap: "00000000-0000-0000-0000-000000000000".to_owned(),
        }
    }

    #[test]
    fn a_configuration_entry_parses_into_drawers_and_a_capability() {
        let parsed = Configuration::parse(&config()).unwrap();
        assert_eq!(parsed.intxt_drawers.len(), 1);
        assert_eq!(parsed.doc_cap, CapId::from_bytes([0; 16]));
    }

    #[test]
    fn a_malformed_entry_is_reported_as_unconfigured_not_trusted() {
        for broken in [
            ReviewConfig {
                github_drawer: "nope".to_owned(),
                ..config()
            },
            ReviewConfig {
                intxt_drawers: vec!["nope".to_owned()],
                ..config()
            },
            ReviewConfig {
                doc_cap: "nope".to_owned(),
                ..config()
            },
        ] {
            assert!(matches!(
                Configuration::parse(&broken),
                Err(CallError::Unconfigured(_))
            ));
        }
    }
}
