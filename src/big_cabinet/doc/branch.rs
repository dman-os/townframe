//! BranchDoc: fork a document into a new one and announce it on its origin.

use super::commands::{Detail, Kind, Row, State};
use super::pipeline::{Failure, Plan, digest};
use super::*;

/// Branch `doc_id` at `at` into a new document, then place the branch through the access
/// policy like a created document.
///
/// A branch *is* the origin's changes up to `at` under a new document id: no value is copied, so
/// every signature on the origin reconstructs on the branch exactly as it did there, and the branch
/// can later be merged back with [`MergeDoc`]. The branch's first change is its
/// `org.example.daybook.branch/<branch id>` facet naming `path`.
///
/// A branch is announced on the origin by the same `org.example.daybook.branch/<branch id>` facet,
/// including its concrete parent reference and fork heads. This requires origin write access;
/// a `stealth` branch skips the announcement and needs only origin read access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchDoc {
    pub capability: CapId,
    pub command: CommandId,
    pub doc_id: DocId,
    pub at: At,
    pub path: String,
    #[serde(default)]
    pub stealth: bool,
    pub placement: String,
}

impl Operation for BranchDoc {
    type Output = WriteReceipt;
    type Error = DocsError;
    const XRPC: XrpcOperation = XrpcOperation::procedure("org.example.daybook.docs.branchDoc")
        .with_schemas("WriteReceipt", "DocsError");
}

impl DocsService {
    async fn branch(
        &self,
        context: &RequestContext,
        params: BranchDoc,
    ) -> Result<WriteReceipt, DocsError> {
        let actor = self.actor(context, params.capability).await?;
        self.authorize(context, &actor, AccessRequest::Read(&params.doc_id))
            .await?;
        if !params.stealth {
            self.authorize(context, &actor, AccessRequest::Write(&params.doc_id))
                .await?;
        }
        self.authorize(
            context,
            &actor,
            AccessRequest::Create {
                placement: &params.placement,
            },
        )
        .await?;
        let request_digest = digest(&(
            "branch",
            actor.uuid,
            &params.doc_id,
            &params.at,
            &params.path,
            params.stealth,
            &params.placement,
        ));
        if let Some(receipt) = self.replay(&params.command, request_digest).await? {
            return Ok(receipt);
        }
        let origin = self.handle(&params.doc_id).await?;
        let fork = origin
            .with_document_read(|doc| -> Result<Automerge, DocsError> {
                context.validate()?;
                let heads = resolve(doc, &params.at)?;
                doc.fork_at(&heads).map_err(repository)
            })
            .await?;
        let detail = Detail::Branch {
            origin: params.doc_id.clone(),
            path: params.path.clone(),
            stealth: params.stealth,
        };
        self.commands
            .begin(
                &params.command,
                Kind::Branch,
                request_digest,
                actor.uuid,
                None,
                Some(&params.placement),
                Some(&detail),
            )
            .await?;
        let branch = self.big_repo.allocate_id().await.map_err(repository)?;
        self.commands.set_document(&params.command, &branch).await?;
        let branch_value = serde_json::json!({
            "path": params.path,
            "from": {
                "facetRef": format!("db+facet:/{}/{}", params.doc_id, branch_key(&params.doc_id)),
                "refHeads": sorted(fork.get_heads()),
            },
        });
        let mut plan = Plan {
            document: &branch,
            doc_uuid: Uuid7::new(),
            actor: &self.system_actor,
            set: BTreeMap::from([(branch_key(&branch), (self.branch.clone(), branch_value))]),
            remove: BTreeSet::new(),
            now: jiff::Timestamp::now(),
            predecessors: BTreeMap::new(),
            aliases: BTreeMap::new(),
        };
        let initial = match self
            .initial_content(context, self.issuer, &mut plan, fork)
            .await
        {
            Ok(initial) => initial,
            Err(error) => {
                self.big_repo
                    .abandon_allocation(branch.clone())
                    .await
                    .map_err(repository)?;
                self.commands.discard(&params.command).await?;
                return Err(error);
            }
        };
        let document = self
            .big_repo
            .commit_id(branch.clone(), initial, Vec::new(), &[])
            .await
            .map_err(repository)?;
        let heads = sorted(document.with_document_read(|doc| doc.get_heads()).await);
        self.commands
            .advance(&params.command, State::Committed, &heads)
            .await?;
        self.settle_branch(
            context,
            &Row {
                command: params.command,
                kind: Kind::Branch,
                digest: request_digest,
                actor: actor.uuid,
                document: Some(branch.clone()),
                placement: Some(params.placement),
                detail: Some(detail),
                state: State::Committed,
                heads: heads.clone(),
            },
        )
        .await?;
        Ok(WriteReceipt {
            doc_id: branch,
            heads,
        })
    }

    /// Finish a committed branch from its ledger row: announce it on its origin unless it is
    /// stealth, place it, and only then acknowledge it. Replayed from the row after a crash, so
    /// each step is idempotent: the backlink is recognized by its commit message.
    pub(super) async fn settle_branch(
        &self,
        context: &RequestContext,
        row: &Row,
    ) -> Result<(), DocsError> {
        let (
            Some(branch),
            Some(placement),
            Some(Detail::Branch {
                origin,
                path: _,
                stealth,
            }),
        ) = (&row.document, &row.placement, &row.detail)
        else {
            unreachable!("a committed branch row names its document, placement and origin")
        };
        let command = &row.command;
        if !stealth {
            let handle = self.handle(origin).await?;
            if handle
                .with_document_read(|doc| committed_change(doc, command))
                .await
                .is_none()
            {
                let branch_facet = branch_key(branch);
                let value = self
                    .handle(branch)
                    .await?
                    .with_document_read(|doc| -> Result<Json, DocsError> {
                        let view = doc.fork_at(&row.heads).map_err(repository)?;
                        let (_, facets) = view
                            .get(automerge::ROOT, "facets")
                            .map_err(repository)?
                            .expect("a committed branch has facets");
                        hydrate_value(
                            &view,
                            &facets,
                            branch_facet.to_string().as_str().into(),
                            self.branch.compiled(),
                        )
                        .map_err(repository)
                    })
                    .await?;
                let mut plan = Plan {
                    document: origin,
                    doc_uuid: Uuid7::new(),
                    actor: &self.system_actor,
                    set: BTreeMap::from([(branch_facet, (self.branch.clone(), value))]),
                    remove: BTreeSet::new(),
                    now: jiff::Timestamp::now(),
                    predecessors: BTreeMap::new(),
                    aliases: BTreeMap::new(),
                };
                self.commit_write(
                    context,
                    self.issuer,
                    &WriteMode::AtCurrent,
                    &mut plan,
                    Some(command),
                )
                .await
                .map_err(Failure::into_error)?;
            }
        }
        self.access
            .place(context, row.actor, branch, placement)
            .await
            .map_err(access_error)?;
        self.commands
            .advance(command, State::Done, &row.heads)
            .await
    }
}

impl Handler<BranchDoc> for DocsService {
    async fn call(
        &self,
        request: Envelope<BranchDoc>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DocsError> {
        Ok(self.branch(&request.context, request.params).await?.into())
    }
}
