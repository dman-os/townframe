//! Who a review verb writes as (ADR 041 §2, ledger D69).
//!
//! Every caller on a node's socket is the operator's actor, so the cabinet cannot tell the
//! operator from an agent running `day` in a session. A comment therefore carries a claimed
//! `creator` and `generator`, detected here from the environment, and `inbox` compares those.
//! This is the one place that reads the environment: the rest of `day_cli` is handed the result.

use super::capture::sessions_dir;
use crate::interlude::*;

use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::reference::DatumReference;
use big_cabinet_core::reference::ReferenceDocId;
use intxt_capture::host;
use intxt_core::annotation::Participant;
use intxt_core::annotation::ParticipantKind;
use intxt_core::client::key;
use intxt_core::work::AgentSession;

/// The variables that name the agent running `day`, in the order they are consulted: the
/// cross-tool convention's `AI_AGENT`, then `AGENT`.
const AGENT_VARS: [&str; 2] = ["AI_AGENT", "AGENT"];

/// The variable that holds a harness's session id, by the name the harness has in `AI_AGENT`.
/// Each harness spells its own; an agent whose harness is not here is detected without a
/// session.
const SESSION_VARS: [(&str, &str); 1] = [("claude-code", "CLAUDE_CODE_SESSION_ID")];

/// The participant a verb writes as.
#[derive(Debug, Clone)]
pub(crate) struct Detected {
    pub(crate) creator: Participant,
    pub(crate) generator: Option<Participant>,
}

impl Detected {
    /// The operator: a `Person`, with no generator. What a verb is when no agent is detected.
    pub(crate) fn operator() -> Self {
        Self {
            creator: Participant::person(),
            generator: None,
        }
    }

    /// Detects from this process's environment. The only reader of it.
    pub(crate) fn from_process(node_dir: &Path) -> Res<Self> {
        Self::from_env(node_dir, |name| {
            std::env::var_os(name).map(|value| value.to_string_lossy().into_owned())
        })
    }

    /// Detects from `env`, with capture's state under `node_dir`. With `AI_AGENT` (then `AGENT`)
    /// unset or empty, the operator. Otherwise `Software`: the creator references the agent's
    /// session document when the harness's session variable is set and capture has recorded that
    /// session, and names nothing when it has not; the generator is the parsed agent value.
    pub(crate) fn from_env(node_dir: &Path, env: impl Fn(&str) -> Option<String>) -> Res<Self> {
        let env = |name: &str| env(name).filter(|value| !value.is_empty());
        let Some(raw) = AGENT_VARS.iter().find_map(|name| env(name)) else {
            return Ok(Self::operator());
        };
        let generator = parse_agent(&raw);
        let session = SESSION_VARS
            .iter()
            .find(|(harness, _)| generator.name.as_deref() == Some(harness))
            .and_then(|(_, var)| env(var));
        let id = match session {
            Some(session) => host::session_doc(&sessions_dir(node_dir), &session)?
                .map(|doc| -> Res<DatumReference> {
                    Ok(DatumReference::new(
                        ReferenceDocId::Concrete(doc.parse()?),
                        key(AgentSession::ID, "main")?,
                    ))
                })
                .transpose()?,
            None => None,
        };
        Ok(Self {
            creator: Participant {
                kind: ParticipantKind::Software,
                id,
                name: None,
                version: None,
                extra: Default::default(),
            },
            generator: Some(generator),
        })
    }

    /// How `show` and `inbox` name the creator of a comment: `you`, or the agent as its generator
    /// and the tail of its session's document id. A creator the cabinet did not attest as a
    /// different actor than the reader is `(claimed)`, since the one attested actor of a node is
    /// all the cabinet knows of it (`attested_as_reader`: the comment's attested authors include
    /// the reader's actor).
    pub(crate) fn name(
        &self,
        creator: &Participant,
        generator: Option<&Participant>,
        attested_as_reader: bool,
    ) -> String {
        if creator.is(&self.creator) {
            return "you".to_owned();
        }
        let mut name = match (creator.kind == ParticipantKind::Person, generator) {
            (true, _) => "someone".to_owned(),
            (false, Some(generator)) => generator.name.clone().unwrap_or_else(|| "agent".to_owned()),
            (false, None) => "agent".to_owned(),
        };
        if let Some(ReferenceDocId::Concrete(doc)) = creator.id.as_ref().map(|id| id.document()) {
            let doc = doc.to_string();
            name.push(' ');
            name.push_str(&doc[doc.len().saturating_sub(8)..]);
        }
        if attested_as_reader {
            name.push_str(" (claimed)");
        }
        name
    }
}

/// Parses an `AI_AGENT` value into the generator it names.
///
/// The convention's format is `<name>_<version>_<role>`, the version's dots written as dashes so
/// that `_` stays the separator: `claude-code_2-1-322_agent` is `claude-code` at `2.1.322`, an
/// agent. The role is not kept. A value of one segment is a name with no version. A value that
/// is not that shape (a second segment that is not a dash-separated number, or more than three
/// segments) is still an agent, so the whole value is kept as the name, unparsed.
fn parse_agent(raw: &str) -> Participant {
    let parts: Vec<&str> = raw.split('_').collect();
    let (name, version) = match parts.as_slice() {
        [name] => (*name, None),
        [name, version] | [name, version, _]
            if !name.is_empty()
                && !version.is_empty()
                && version.split('-').all(|part| {
                    !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())
                }) =>
        {
            (*name, Some(version.replace('-', ".")))
        }
        _ => (raw, None),
    };
    Participant {
        kind: ParticipantKind::Software,
        id: None,
        name: Some(name.to_owned()),
        version,
        extra: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generator(raw: &str) -> (Option<String>, Option<String>) {
        let parsed = parse_agent(raw);
        (parsed.name, parsed.version)
    }

    #[test]
    fn ai_agent_names_the_harness_and_its_version() {
        assert_eq!(
            generator("claude-code_2-1-322_agent"),
            (Some("claude-code".into()), Some("2.1.322".into()))
        );
        assert_eq!(
            generator("claude-code_2-1-322"),
            (Some("claude-code".into()), Some("2.1.322".into()))
        );
        assert_eq!(generator("codex"), (Some("codex".into()), None));
    }

    #[test]
    fn an_unparseable_ai_agent_is_still_an_agent_named_by_its_raw_value() {
        for raw in [
            "claude-code_next_agent",
            "claude-code_2--1_agent",
            "a_1_b_c",
            "_1_agent",
            "claude-code__agent",
        ] {
            assert_eq!(generator(raw), (Some(raw.into()), None), "{raw}");
        }
    }

    #[test]
    fn no_agent_variable_is_the_operator_and_agent_beats_ai_agent_order() {
        let dir = tempfile::tempdir().unwrap();
        let env = |vars: &'static [(&'static str, &'static str)]| {
            Detected::from_env(dir.path(), move |name| {
                vars.iter()
                    .find(|(var, _)| *var == name)
                    .map(|(_, value)| (*value).to_owned())
            })
            .unwrap()
        };
        let operator = env(&[("CLAUDE_CODE_SESSION_ID", "s1"), ("AI_AGENT", "")]);
        assert!(operator.creator.is(&Participant::person()));
        assert!(operator.generator.is_none());

        // `AI_AGENT` is consulted before `AGENT`; the session is not recorded, so no id.
        let agent = env(&[
            ("AGENT", "other"),
            ("AI_AGENT", "claude-code_2-1-322_agent"),
            ("CLAUDE_CODE_SESSION_ID", "s1"),
        ]);
        assert_eq!(agent.creator.kind, ParticipantKind::Software);
        assert!(agent.creator.id.is_none());
        assert_eq!(agent.generator.unwrap().version.as_deref(), Some("2.1.322"));
        let fallback = env(&[("AGENT", "other")]);
        assert_eq!(fallback.generator.unwrap().name.as_deref(), Some("other"));
    }
}
