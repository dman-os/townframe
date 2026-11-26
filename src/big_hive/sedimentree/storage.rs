//! Subduction's [`Storage`] over the tables. Each method is a thin wrapper: a mutation opens a
//! transaction of its own around [`SedimentreeStore::apply_in`], a read opens a snapshot.

use future_form::FutureForm;
use future_form::Local;
use future_form::Sendable;
use future_form::future_form;
use sedimentree_core::blob::Blob;
use sedimentree_core::collections::Set;
use sedimentree_core::depth::DepthMetric;
use sedimentree_core::fragment::Fragment;
use sedimentree_core::id::SedimentreeId;
use sedimentree_core::loose_commit::LooseCommit;
use sedimentree_core::loose_commit::id::CommitId;
use sqlite_rs::SqliteDb;
use subduction_core::storage::traits::Storage;
use subduction_crypto::signed::Signed;
use subduction_crypto::verified_meta::VerifiedMeta;

use super::Error;
use super::Mutation;
use super::SedimentreeStore;
use super::write::commit_id_of;
use crate::sqlite::queries;

impl<D: SqliteDb, M: DepthMetric> SedimentreeStore<D, M> {
    /// The blobs of the commits and fragments of tree `id` encrypted under the PCS update
    /// `pcs_update_hash`, served by the partial indexes on that column.
    pub async fn blobs_under(
        &self,
        id: SedimentreeId,
        pcs_update_hash: [u8; 32],
    ) -> Result<Vec<Vec<u8>>, Error> {
        let tx = self.db.begin_read().await?;
        let read = async {
            let tree_id = id.as_bytes().to_vec();
            let hash = pcs_update_hash.to_vec();
            let mut blobs =
                queries::commit_blobs_by_pcs_update(&tx, tree_id.clone(), hash.clone()).await?;
            blobs.extend(queries::fragment_blobs_by_pcs_update(&tx, tree_id, hash).await?);
            Ok::<_, Error>(blobs)
        }
        .await;
        D::end_read(tx).await?;
        read
    }

    /// Run `read` against a snapshot.
    async fn snapshot<T>(
        &self,
        read: impl AsyncFnOnce(&D::Read) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let tx = self.db.begin_read().await?;
        let out = read(&tx).await;
        D::end_read(tx).await?;
        out
    }
}

fn commit_of(signed: &[u8], blob: Vec<u8>) -> Result<VerifiedMeta<LooseCommit>, Error> {
    Ok(VerifiedMeta::try_from_trusted(
        Signed::try_decode(signed)?,
        Blob::new(blob),
    )?)
}

fn fragment_of(signed: &[u8], blob: Vec<u8>) -> Result<VerifiedMeta<Fragment>, Error> {
    Ok(VerifiedMeta::try_from_trusted(
        Signed::try_decode(signed)?,
        Blob::new(blob),
    )?)
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm, D: SqliteDb, M: DepthMetric + Send + Sync + 'static> Storage<Async>
    for SedimentreeStore<D, M>
{
    type Error = Error;

    fn save_sedimentree_id(&self, id: SedimentreeId) -> Async::Future<'_, Result<(), Error>> {
        Async::from_future(async move {
            self.apply(id, Mutation::Register).await?;
            Ok(())
        })
    }

    fn delete_sedimentree_id(&self, id: SedimentreeId) -> Async::Future<'_, Result<(), Error>> {
        Async::from_future(async move {
            self.apply(id, Mutation::Retire).await?;
            Ok(())
        })
    }

    fn load_all_sedimentree_ids(&self) -> Async::Future<'_, Result<Set<SedimentreeId>, Error>> {
        Async::from_future(async move {
            self.snapshot(async |tx| {
                Ok(queries::live_tree_ids(tx)
                    .await?
                    .into_iter()
                    .map(|id| {
                        SedimentreeId::new(id.try_into().expect("a tree id is 32 bytes (CHECK)"))
                    })
                    .collect())
            })
            .await
        })
    }

    fn contains_sedimentree_id(&self, id: SedimentreeId) -> Async::Future<'_, Result<bool, Error>> {
        Async::from_future(async move {
            self.snapshot(async |tx| {
                Ok(queries::tree_state(tx, id.as_bytes().to_vec())
                    .await?
                    .is_some_and(|state| state.live))
            })
            .await
        })
    }

    fn save_loose_commit(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> Async::Future<'_, Result<(), Error>> {
        Async::from_future(async move {
            self.apply(
                id,
                Mutation::Save {
                    commits: vec![verified.into()],
                    fragments: Vec::new(),
                },
            )
            .await?;
            Ok(())
        })
    }

    fn list_commit_ids(
        &self,
        id: SedimentreeId,
    ) -> Async::Future<'_, Result<Set<CommitId>, Error>> {
        Async::from_future(async move {
            self.snapshot(async |tx| {
                Ok(queries::commit_ids(tx, id.as_bytes().to_vec())
                    .await?
                    .into_iter()
                    .map(|row| commit_id_of(&row))
                    .collect())
            })
            .await
        })
    }

    fn load_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Error>> {
        Async::from_future(async move {
            let rows = self
                .snapshot(async |tx| Ok(queries::commit_rows(tx, id.as_bytes().to_vec()).await?))
                .await?;
            rows.into_iter()
                .map(|row| commit_of(&row.signed, row.blob))
                .collect()
        })
    }

    fn load_loose_commit_metas(
        &self,
        id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<LooseCommit>, Error>> {
        Async::from_future(async move {
            let rows = self
                .snapshot(async |tx| Ok(queries::commit_metas(tx, id.as_bytes().to_vec()).await?))
                .await?;
            rows.iter()
                .map(|signed| {
                    Ok(Signed::<LooseCommit>::try_decode(signed)?.try_decode_trusted_payload()?)
                })
                .collect()
        })
    }

    fn load_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Error>> {
        Async::from_future(async move {
            let row = self
                .snapshot(async |tx| {
                    Ok(queries::commit_row(
                        tx,
                        id.as_bytes().to_vec(),
                        commit_id.as_bytes().to_vec(),
                    )
                    .await?)
                })
                .await?;
            row.map(|row| commit_of(&row.signed, row.blob)).transpose()
        })
    }

    fn delete_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<(), Error>> {
        Async::from_future(async move {
            self.apply(id, Mutation::DeleteCommit(commit_id)).await?;
            Ok(())
        })
    }

    fn delete_loose_commits(&self, id: SedimentreeId) -> Async::Future<'_, Result<(), Error>> {
        Async::from_future(async move {
            self.apply(id, Mutation::DeleteCommits).await?;
            Ok(())
        })
    }

    fn save_fragment(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> Async::Future<'_, Result<(), Error>> {
        Async::from_future(async move {
            self.apply(
                id,
                Mutation::Save {
                    commits: Vec::new(),
                    fragments: vec![verified.into()],
                },
            )
            .await?;
            Ok(())
        })
    }

    fn load_fragment(
        &self,
        id: SedimentreeId,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Error>> {
        Async::from_future(async move {
            let row = self
                .snapshot(async |tx| {
                    Ok(queries::fragment_row(
                        tx,
                        id.as_bytes().to_vec(),
                        fragment_head.as_bytes().to_vec(),
                    )
                    .await?)
                })
                .await?;
            row.map(|row| fragment_of(&row.signed, row.blob))
                .transpose()
        })
    }

    fn list_fragment_ids(
        &self,
        id: SedimentreeId,
    ) -> Async::Future<'_, Result<Set<CommitId>, Error>> {
        Async::from_future(async move {
            self.snapshot(async |tx| {
                Ok(queries::fragment_heads(tx, id.as_bytes().to_vec())
                    .await?
                    .into_iter()
                    .map(|row| commit_id_of(&row))
                    .collect())
            })
            .await
        })
    }

    fn load_fragments(
        &self,
        id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Error>> {
        Async::from_future(async move {
            let rows = self
                .snapshot(async |tx| Ok(queries::fragment_rows(tx, id.as_bytes().to_vec()).await?))
                .await?;
            rows.into_iter()
                .map(|row| fragment_of(&row.signed, row.blob))
                .collect()
        })
    }

    fn load_fragment_metas(
        &self,
        id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<Fragment>, Error>> {
        Async::from_future(async move {
            let rows = self
                .snapshot(async |tx| Ok(queries::fragment_metas(tx, id.as_bytes().to_vec()).await?))
                .await?;
            rows.iter()
                .map(|signed| {
                    Ok(Signed::<Fragment>::try_decode(signed)?.try_decode_trusted_payload()?)
                })
                .collect()
        })
    }

    fn delete_fragment(
        &self,
        id: SedimentreeId,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<(), Error>> {
        Async::from_future(async move {
            self.apply(id, Mutation::DeleteFragment(fragment_head))
                .await?;
            Ok(())
        })
    }

    fn delete_fragments(&self, id: SedimentreeId) -> Async::Future<'_, Result<(), Error>> {
        Async::from_future(async move {
            self.apply(id, Mutation::DeleteFragments).await?;
            Ok(())
        })
    }

    fn save_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> Async::Future<'_, Result<usize, Error>> {
        Async::from_future(async move {
            self.apply(
                id,
                Mutation::Save {
                    commits: commits.into_iter().map(Into::into).collect(),
                    fragments: fragments.into_iter().map(Into::into).collect(),
                },
            )
            .await
        })
    }
}
