//! Runtime grant requests (ADR 049 §4.2): a cartridge asks, while it runs, for access its manifest
//! declares, and the person answers once, on whichever surface they use.
//!
//! The queue is node state in memory, owned by the runtime that owns the grants (the nodule
//! service on this node), and it lasts the boot: "only this time" and "don't allow" end when the
//! app and node next start, so a routine files again after a restart. A request is filed by the
//! cartridge's own runtime actor on its own behalf (the cartridge is taken from the caller's
//! actor, never from the body); answering and listing are the operator's.

use super::CartridgesError;
use crate::CartridgeId;
use crate::config::GrantBody;
use crate::config::GrantKey;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use serde::Deserialize;
use serde::Serialize;

/// Files a request for access one of the calling cartridge's permbinds declares, and returns at
/// once; [`AwaitCartridgeGrantRequest`] is how a caller waits for the answer. Filing never grants
/// anything and never fails a call that was refused: nothing files implicitly (ADR 049 §4.2).
///
/// `permbind` names the manifest's binding. Only a `#method` permbind and a `#drawers` permbind
/// that declares some read, write or delete are requestable: a `#datum` permbind follows the
/// drawer grants and has no answer of its own. For a `#drawers` permbind `drawer` is the drawer
/// the cartridge suggests; the person may pick another when answering.
///
/// The caller is the cartridge's runtime actor presenting the handle dispatch minted for it. A
/// request that the cartridge's grants already satisfy comes back [`GrantRequestState::Allowed`]
/// with no request, and one the person declined this boot comes back
/// [`GrantRequestState::Declined`] with none, so a routine that retries cannot prompt in a loop.
/// Filing the same permbind again while it is pending returns the pending request.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestCartridgeGrant {
    pub permbind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub drawer: Option<DrawerId>,
}

/// Where a request stands. Only `Pending` leaves it; the others are final for the boot.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GrantRequestState {
    /// Nobody has answered.
    Pending,
    /// Answered "allow": the grant datum is written and the cartridge's handles re-minted.
    Allowed,
    /// Answered "only this time": granted until the app and node next start.
    OnlyThisBoot,
    /// Answered "don't allow", or suppressed by an earlier no this boot.
    Declined,
}

/// The result of [`RequestCartridgeGrant`]: the request to await, or none when the answer was
/// settled without one (see [`RequestCartridgeGrant`]).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FiledGrantRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<Uuid7>,
    pub state: GrantRequestState,
}

/// What a request asks for, in the manifest's terms. A person sees the cartridge and the
/// question, never the binding's name (FDR 009), so the CLI and the shell word it from this and
/// the permbind's own description.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RequestedAccess {
    /// Use of a drawer at `access`, the most the `#drawers` permbind declares. `drawer` is the
    /// cartridge's suggestion, absent when the person is to pick one.
    #[serde(rename_all = "camelCase")]
    Drawers {
        access: DrawerAccess,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
        drawer: Option<DrawerId>,
    },
    /// Calling one operation through dispatch.
    #[serde(rename_all = "camelCase")]
    Method { nsid: String },
}

/// One request waiting for an answer.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantRequestRecord {
    pub id: Uuid7,
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub cartridge_id: CartridgeId,
    /// The manifest's binding, for the advanced view; not what a person is shown.
    pub permbind: String,
    /// The permbind's own description of why the cartridge wants it.
    pub description: String,
    pub access: RequestedAccess,
    /// When the request was filed, in milliseconds since the Unix epoch.
    pub filed_at_ms: u64,
}

/// The pending requests, oldest first.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListCartridgeGrantRequests {}

/// The result of [`ListCartridgeGrantRequests`].
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantRequests {
    pub requests: Vec<GrantRequestRecord>,
}

/// How a person answers a request: the same three words on every surface (FDR 009).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GrantAnswer {
    /// Writes the grant datum, which follows the person to their other devices.
    Allow,
    /// Grants until the app and node next start; never written to the config document.
    OnlyThisBoot,
    /// Refuses, and refuses the same permbind of the same cartridge again until the next start.
    Decline,
}

/// Answers a pending request. For a `#drawers` request that does not name a drawer, `drawer` is
/// the drawer the person picks and is required unless declining; one that does name a drawer is
/// answered for it unless `drawer` says otherwise. An answer to a request that is not pending,
/// or a pick on a `#method` request, is refused.
///
/// By the time an allow or an only-this-boot answer returns, the cartridge's handles have been
/// re-minted from it, so the cartridge's next call sees the access.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnswerCartridgeGrantRequest {
    pub request: Uuid7,
    pub answer: GrantAnswer,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub drawer: Option<DrawerId>,
}

/// A long poll over one request's answer (ADR 023), for a micro's future or a workflow. Holds an
/// otherwise unchanged `Pending` answer up to `hold_ms`, which the service clamps; `0` answers at
/// once. The caller is the cartridge that filed the request, or the operator. An expired hold is
/// an ordinary `Pending`, and the caller polls again.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AwaitCartridgeGrantRequest {
    pub request: Uuid7,
    #[serde(default)]
    pub hold_ms: u32,
}

/// The result of [`AwaitCartridgeGrantRequest`] and of [`AnswerCartridgeGrantRequest`]: where the
/// request stands.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantRequestAnswer {
    pub state: GrantRequestState,
}

/// Lists the answers: every grant of `cartridge_id`, or of every cartridge when absent, written
/// in its config document or in force for this boot only.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListCartridgeGrants {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub cartridge_id: Option<CartridgeId>,
}

/// One grant of a cartridge as the answers list reports it.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeGrantRecord {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub cartridge_id: CartridgeId,
    pub grant: GrantKey,
    pub value: GrantBody,
    /// Granted "only this time": in force until the app and node next start, in no document.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub this_boot: bool,
}

/// The result of [`ListCartridgeGrants`].
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeGrants {
    pub grants: Vec<CartridgeGrantRecord>,
}

/// A long poll over one cartridge's grants (ADR 023), for a holder that applies them itself,
/// such as a micro host. Answers at once with the grants (as [`ListCartridgeGrants`] lists them,
/// "only this time" ones included) and their revision when `since` is absent or is not the
/// current revision; otherwise holds up to `hold_ms`, which the service clamps, until they
/// change. An expired hold answers the unchanged grants. A revision is opaque and names the boot
/// it was taken in, so one from before a restart never matches. A cartridge granted nothing,
/// installed or not, has no grants at a revision of its own.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AwaitCartridgeGrants {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub cartridge_id: CartridgeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    #[serde(default)]
    pub hold_ms: u32,
}

/// The result of [`AwaitCartridgeGrants`]: the cartridge's grants and the revision they are at.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeGrantsAt {
    pub grants: Vec<CartridgeGrantRecord>,
    pub revision: String,
}

/// The longest an await holds, matching the registry's own watch (ADR 023 rule 3).
pub const AWAIT_MAX_HOLD_MS: u32 = 15_000;

method_core::methods! {
    error = CartridgesError, nsid_prefix = "org.example.daybook.cartridges.";
    RequestCartridgeGrant => FiledGrantRequest, procedure, "requestCartridgeGrant";
    AwaitCartridgeGrantRequest => GrantRequestAnswer, procedure, "awaitCartridgeGrantRequest", long_poll;
    ListCartridgeGrantRequests => GrantRequests, query, "listCartridgeGrantRequests", privileged;
    AnswerCartridgeGrantRequest => GrantRequestAnswer, procedure, "answerCartridgeGrantRequest", privileged;
    ListCartridgeGrants => CartridgeGrants, query, "listCartridgeGrants", privileged;
    AwaitCartridgeGrants => CartridgeGrantsAt, procedure, "awaitCartridgeGrants", long_poll, privileged;
}
