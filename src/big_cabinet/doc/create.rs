//! CreateDoc: create a document, place it, and settle its command row.

use super::commands::{Kind, State};
use super::pipeline::{Plan, digest};
use super::*;
use serde_json::Value;

/// Create a document holding `facets`, then place it through the access policy. `placement` is the
/// policy's own spelling of where the document goes (for DrawerRepo, a drawer); this repository
/// does not interpret it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDoc {
    pub capability: CapId,
    pub command: CommandId,
    pub placement: String,
    pub facets: BTreeMap<FacetKey, Value>,
}

impl Operation for CreateDoc {
    type Output = WriteReceipt;
    type Error = DocsError;
    const XRPC: XrpcOperation = XrpcOperation::procedure("org.example.daybook.docs.createDoc")
        .with_schemas("WriteReceipt", "DocsError");
}

impl DocsService {
    async fn create(
        &self,
        context: &RequestContext,
        params: CreateDoc,
    ) -> Result<WriteReceipt, DocsError> {
        let actor = self.actor(context, params.capability).await?;
        self.authorize(
            context,
            &actor,
            AccessRequest::Create {
                placement: &params.placement,
            },
        )
        .await?;
        let request_digest = digest(&("create", actor.uuid, &params.placement, &params.facets));
        if let Some(receipt) = self.replay(&params.command, request_digest).await? {
            return Ok(receipt);
        }
        let set = self.schemas(params.facets, &BTreeSet::new())?;
        self.commands
            .begin(
                &params.command,
                Kind::Create,
                request_digest,
                actor.uuid,
                None,
                Some(&params.placement),
                None,
            )
            .await?;
        let doc_id = self.big_repo.allocate_id().await.map_err(repository)?;
        self.commands.set_document(&params.command, &doc_id).await?;
        let mut plan = Plan {
            document: &doc_id,
            doc_uuid: Uuid7::new(),
            actor: &actor,
            set,
            remove: BTreeSet::new(),
            now: jiff::Timestamp::now(),
            predecessors: BTreeMap::new(),
            aliases: BTreeMap::new(),
        };
        let initial = match self
            .initial_content(context, params.capability, &mut plan, Automerge::new())
            .await
        {
            Ok(initial) => initial,
            Err(error) => {
                self.big_repo
                    .abandon_allocation(doc_id.clone())
                    .await
                    .map_err(repository)?;
                self.commands.discard(&params.command).await?;
                return Err(error);
            }
        };
        let document = self
            .big_repo
            .commit_id(doc_id.clone(), initial, Vec::new(), &[])
            .await
            .map_err(repository)?;
        let heads = sorted(document.with_document_read(|doc| doc.get_heads()).await);
        self.settle_create(
            context,
            &params.command,
            actor.uuid,
            &doc_id,
            &params.placement,
            &heads,
        )
        .await?;
        Ok(WriteReceipt { doc_id, heads })
    }

    /// Persisted identity and content are acknowledged only once the policy has placed the
    /// document; a crash in between replays the placement from the committed row.
    pub(super) async fn settle_create(
        &self,
        context: &RequestContext,
        command: &CommandId,
        actor: Uuid7,
        document: &DocId,
        placement: &str,
        heads: &[ChangeHash],
    ) -> Result<(), DocsError> {
        self.commands
            .advance(command, State::Committed, heads)
            .await?;
        self.access
            .place(context, actor, document, placement)
            .await
            .map_err(access_error)?;
        self.commands.advance(command, State::Done, heads).await
    }

    /// Stage, sign and commit a plan's change on top of `doc`, a document that is not yet in BigRepo:
    /// empty for a create, the origin's forked changes for a branch.
    pub(super) async fn initial_content(
        &self,
        context: &RequestContext,
        capability: CapId,
        plan: &mut Plan<'_>,
        mut doc: Automerge,
    ) -> Result<Automerge, DocsError> {
        doc.set_actor(doc_actor(&self.node_issuer, plan.document));
        let tx = doc.transaction();
        let (tx, claims) = self.stage(tx, plan, None)?;
        tx.rollback();
        let signatures = self.sign(context, capability, &claims).await?;
        let tx = doc.transaction();
        let (tx, signed) = self.stage(tx, plan, Some(&signatures))?;
        assert_eq!(signed, claims, "a plan stages the same claims twice");
        tx.commit();
        Ok(doc)
    }
}

impl Handler<CreateDoc> for DocsService {
    async fn call(
        &self,
        request: Envelope<CreateDoc>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DocsError> {
        Ok(self.create(&request.context, request.params).await?.into())
    }
}
