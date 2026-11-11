//! The client library: what calls a node's cabinet methods on behalf of an operator, shared by
//! the places that do it. `intxt_nodule`'s `createIntxt` and `addToIntxt` handlers run it inside
//! the wasm guest, over the transport the host delivered the request with, presenting the
//! capability their own caller presented; `day x intxt capture` runs it in the CLI process, over
//! the connection to the node (the operator's own authority, so no bearer). Both build a
//! [`Caller`], so there is one way to mint a capability, make a call and name a command.
//!
//! [`Caller::record_observations`] (ADR 039 §4) is the recorder: it turns what one capture hook
//! observed into the datums of ADR 040. It is here, not in a nodule method, so a batch is not
//! bound by a guest's request size limit and the nodule adds no authority of its own: every
//! cabinet call is made with the caller's own authority.

mod record;

use crate::methods::IntxtError;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use method_core::Method;
use method_core::Unary;
use serde::Serialize;
use serde_json::Value;
use service_client_core_rs::Client;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::Xrpc;
use service_core_rs::HttpMetadata;
use service_core_rs::RequestContext;
use service_core_rs::xrpc::bearer_header_value;

/// What a caller needs to call cabinet methods on behalf of whoever it presents the authority of:
/// the transport to the node, the capability it presents (none when the node already admits the
/// connection as its operator), and the base of the command ids.
pub struct Caller<H> {
    client: Client<Xrpc<H>>,
    context: RequestContext,
    capability: Option<String>,
    /// The idempotency key, or the request id when the caller sent none (nothing to dedupe
    /// against, so every call is its own).
    command_base: String,
}

impl<H: HttpTransport> Caller<H> {
    /// A caller over `transport`. Each call is the next hop of `context`. `capability` is the
    /// capability id presented as `authorization: Bearer <id>`; `command_base` starts every
    /// command id, so a retry under the same base reuses the same commands and the services that
    /// dedupe by command answer the earlier result.
    pub fn new(
        transport: H,
        context: RequestContext,
        capability: Option<&str>,
        command_base: String,
    ) -> Self {
        Self {
            client: Client::new(Xrpc(transport)),
            context,
            capability: capability.map(str::to_owned),
            command_base,
        }
    }

    /// The command id of `step` writing `content`: the base, the step's fixed suffix and a short
    /// digest of the request's content (canonical JSON). An identical retry is the same command
    /// and dedupes; a write whose content changed since a partial first attempt is a new command
    /// instead of a `CommandReused` refusal.
    pub fn command(&self, step: &str, content: &impl Serialize) -> Result<CommandId, IntxtError> {
        let digest = blake3::hash(&serde_json::to_vec(content).expect("a request serializes"));
        CommandId::try_from(format!(
            "{}/{step}/{}",
            self.command_base,
            &digest.to_hex().as_str()[..16]
        ))
        .map_err(|error| {
            IntxtError::Invalid(format!("the idempotency key makes no command id: {error}"))
        })
    }

    /// Calls `input` presenting the caller's capability, if it has one, as the authority of the
    /// call.
    pub async fn call<M: Unary>(&self, input: M) -> Result<M::Output, IntxtError> {
        let metadata = HttpMetadata {
            authorization: self.capability.as_deref().map(bearer_header_value),
            idempotency_key: None,
        };
        self.client
            .call_with(self.context.next_hop(), metadata, input)
            .await
            .map_err(|error| IntxtError::Upstream {
                operation: M::XRPC.nsid.to_owned(),
                reason: error.to_string(),
            })
    }

    pub async fn doc_capability(&self, scope: DocScope) -> Result<CapId, IntxtError> {
        Ok(self
            .call(MintDocCapability {
                scope,
                rights: Rights::USE,
                subject: None,
            })
            .await?
            .capability_id)
    }

    /// The document's heads and the accepted value of `key` in it at those heads.
    pub async fn read(
        &self,
        doc: &DocId,
        key: &DatumKey,
    ) -> Result<(Vec<ChangeHash>, Value), IntxtError> {
        let capability = self
            .doc_capability(DocScope {
                read: [doc.clone()].into(),
                ..DocScope::default()
            })
            .await?;
        let mut read = self
            .call(ReadDatums {
                capability,
                doc_id: doc.clone(),
                at: At::Current,
                datums: Some([key.clone()].into()),
                optimistic: false,
            })
            .await?;
        match read.datums.remove(key) {
            Some(DatumRead::Accepted { value, .. }) => Ok((read.heads, value)),
            other => Err(IntxtError::Invalid(format!(
                "{key} in {doc} is not readable: {other:?}"
            ))),
        }
    }
}

/// `value` as the JSON a datum holds.
pub fn to_value(value: &impl Serialize) -> Value {
    serde_json::to_value(value).expect("a record serializes")
}
