//! Boot reconciliation for the drawer's add outbox (`drawer/outbox.rs`).
//!
//! Runs inside [`DrawerRepo::load`]: every write-ahead row still owed work is
//! replayed through the drawer's own registration sequence, and every
//! reservation that outlived its outbox row is abandoned. This is the drawer
//! outbox boot pass that replaced the interim warn+abandon sweep
//! (`authority.rs::recover_pending_documents`, deleted).

use crate::interlude::*;

use super::mutations::StagedAdd;
use super::outbox;
use super::{DrawerRepo, meta};

impl DrawerRepo {
    pub(crate) async fn reconcile_add_outbox_at_boot(&self) -> Res<()> {
        outbox::ensure_outbox_schema(&self.meta_store_sql).await?;
        let reserved: std::collections::HashSet<DocumentId> = self
            .big_repo
            .reserved_doc_ids()
            .await?
            .into_iter()
            .collect();

        // The leak sweep: a reservation with no outbox row names a crash
        // before the write-ahead row was written — at that point `allocate_id`
        // is bookkeeping-only, so no content existed for the id. Abandon (the
        // reservation delete is the only act) and nothing else; nothing was
        // ever registered.
        for doc_id in &reserved {
            if outbox::get_row_for_branch(&self.meta_store_sql, doc_id)
                .await?
                .is_none()
            {
                warn_missing_outbox_row(doc_id);
                self.big_repo.abandon_allocation(doc_id.clone()).await?;
            }
        }

        for row in outbox::list_pending_rows(&self.meta_store_sql).await? {
            let sedimentree_persisted = self
                .big_repo
                .contains_sedimentree_id(row.branch_doc_id.clone())
                .await?;
            let reservation_live = reserved.contains(&row.branch_doc_id);
            if reservation_live && !sedimentree_persisted {
                if !self
                    .big_repo
                    .has_keyhive_document(&row.branch_doc_id)
                    .await?
                {
                    // `commit_id` never succeeded: no keyhive authority, no
                    // events, nothing registered. Abandon and drop the row;
                    // the caller's retry with the same key is a fresh,
                    // deterministic add.
                    warn_abandoned_uncommitted(&row);
                    self.big_repo
                        .abandon_allocation(row.branch_doc_id.clone())
                        .await?;
                    outbox::delete_row(&self.meta_store_sql, &row.idempotency_key).await?;
                    continue;
                }
                // The commit crashed between authority creation and Sedimentree
                // persistence: finish it from the reservation's staged record —
                // the byte-identical re-run ends with the row deleted.
                self.big_repo
                    .commit_reserved(row.branch_doc_id.clone(), &[])
                    .await
                    .wrap_err_with(|| {
                        eyre::eyre!(
                            "outbox replay: finishing interrupted commit for {}",
                            row.branch_doc_id
                        )
                    })?;
            }

            // Registration read: one claim-machinery pass over the drawer's
            // durable surfaces. Reservations with an entry are already
            // registered; the claim scan is paid only for the ones without.
            let Some(read) = meta::registered_allocation_shapes(
                &self.meta_store_sql,
                &self.big_repo,
                &self.drawer_doc_id,
                std::slice::from_ref(&row.branch_doc_id),
            )
            .await?
            else {
                warn_unreadable_registration(&row);
                continue;
            };
            let registered = read.shapes.contains_key(&row.branch_doc_id);
            let claimed = read.claims.contains(&row.branch_doc_id);
            if !registered && claimed {
                // A claimed temporary add (ADR 003 §19): the durable cipherBlob
                // claim names the staging doc by `keyRef`, so the commit is
                // replayed starting from the `docs.map` entry written here.
                meta::register_claimed_allocations(
                    &self.big_repo,
                    &self.drawer_doc_id,
                    std::slice::from_ref(&row.branch_doc_id),
                    self.local_actor_id.clone(),
                )
                .await?;
            } else if row.temporary && !registered {
                // An unclaimed temporary: nothing asserts its registration, so it
                // must stay the unadvertised node-local staging doc it is. The
                // row goes; the doc stays exactly as ADR 003 §19 leaves unclaimed
                // staging docs (no events, sedimentree, bytes or claims touched)
                // and its retry — or the boot pass — starts fresh.
                warn_unclaimed_temporary(&row);
                outbox::delete_row(&self.meta_store_sql, &row.idempotency_key).await?;
                continue;
            }

            // The rest of the interrupted commit_temporary/add flow, through
            // the drawer's own shared sequence: grants, the real entry (over
            // the claim's replay), partitions, caches and `done`.
            let handle = self
                .big_repo
                .get_doc(&row.branch_doc_id)
                .await?
                .into_ready(row.branch_doc_id.clone())
                .wrap_err_with(|| {
                    eyre::eyre!("outbox replay: {} not materializable", row.branch_doc_id)
                })?;
            let staged = StagedAdd {
                doc_id: daybook_types::doc::DocId::from(row.branch_doc_id.to_string()),
                handle,
                entry: row.entry,
                branch_heads: row.staged_branch_heads,
                branch_doc_id: row.branch_doc_id,
                idempotency_key: row.idempotency_key,
            };
            self.commit_staged_adds(std::slice::from_ref(&staged))
                .await?;
        }

        outbox::purge_done_rows(&self.meta_store_sql).await?;
        Ok(())
    }
}

fn warn_missing_outbox_row(doc_id: &DocumentId) {
    tracing::warn!(
        %doc_id,
        "abandoning a reservation with no outbox row: the crash predates the \
         write-ahead write, so no content existed for this id"
    );
}

fn warn_abandoned_uncommitted(row: &outbox::OutboxRow) {
    tracing::warn!(
        doc_id = %row.branch_doc_id,
        key = %row.idempotency_key,
        "abandoning an uncommitted pending-add: nothing was registered and the \
         caller's retry with the same key re-executes as a fresh add"
    );
}

fn warn_unreadable_registration(row: &outbox::OutboxRow) {
    tracing::warn!(
        doc_id = %row.branch_doc_id,
        key = %row.idempotency_key,
        "outbox row keeps its replay: the drawer registration surfaces could \
         not be read at this boot stage"
    );
}

fn warn_unclaimed_temporary(row: &outbox::OutboxRow) {
    tracing::warn!(
        doc_id = %row.branch_doc_id,
        key = %row.idempotency_key,
        "dropping an unclaimed temporary-add outbox row: the staging doc stays \
         node-local and unregistered; the retry re-derives it fresh"
    );
}
