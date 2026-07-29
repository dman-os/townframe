use crate::interlude::*;

use big_repo::{BigKeyhiveAuthority, BigKeyhiveGroup, SharedBigRepo};

const REPO_AGENTS_GROUP_KEY: &str = "global.authority.repo_agents_group";
const CORE_DOCS_GROUP_KEY: &str = "global.authority.core_docs_group";
const CONTENT_DOCS_GROUP_KEY: &str = "global.authority.content_docs_group";
const DRAWER_GROUP_KEY: &str = "global.authority.default_drawer_group";

/// Stable identifiers for the initial repository authority groups.
///
/// The group objects themselves live in BigRepo's persisted Keyhive graph. These
/// ids are only the local lookup/bootstrap record and are safe to send as part
/// of clone metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RepoAuthorityIds {
    pub repo_agents: [u8; 32],
    pub core_docs: [u8; 32],
    pub content_docs: [u8; 32],
    pub default_drawer: [u8; 32],
}

#[derive(Clone)]
pub(crate) struct RepoAuthority {
    pub repo_agents: BigKeyhiveGroup,
    pub core_docs: BigKeyhiveGroup,
    pub content_docs: BigKeyhiveGroup,
    pub default_drawer: BigKeyhiveGroup,
}

impl RepoAuthority {
    pub(crate) fn ids(&self) -> RepoAuthorityIds {
        RepoAuthorityIds {
            repo_agents: self.repo_agents.id().to_bytes(),
            core_docs: self.core_docs.id().to_bytes(),
            content_docs: self.content_docs.id().to_bytes(),
            default_drawer: self.default_drawer.id().to_bytes(),
        }
    }

    pub(crate) fn core_docs_parent(&self) -> BigKeyhiveAuthority {
        self.core_docs.clone().into()
    }


    pub(crate) fn core_docs_part_id(&self) -> PartId {
        big_repo::group_part_id(self.core_docs.id().to_bytes())
    }
    pub(crate) fn content_docs_part_id(&self) -> PartId {
        big_repo::group_part_id(self.content_docs.id().to_bytes())
    }
    pub(crate) fn default_drawer_part_id(&self) -> PartId {
        big_repo::group_part_id(self.default_drawer.id().to_bytes())
    }
}

pub(crate) async fn ensure(
    big_repo: &SharedBigRepo,
    sql: &SqlCtx,
    supplied_ids: Option<RepoAuthorityIds>,
) -> Res<RepoAuthority> {
    let (repo_agents, repo_agents_created) = ensure_group(
        big_repo,
        sql,
        REPO_AGENTS_GROUP_KEY,
        supplied_ids.map(|ids| ids.repo_agents),
    )
    .await?;
    let (core_docs, core_docs_created) = ensure_group(
        big_repo,
        sql,
        CORE_DOCS_GROUP_KEY,
        supplied_ids.map(|ids| ids.core_docs),
    )
    .await?;
    let (content_docs, content_docs_created) = ensure_group(
        big_repo,
        sql,
        CONTENT_DOCS_GROUP_KEY,
        supplied_ids.map(|ids| ids.content_docs),
    )
    .await?;
    let (default_drawer, default_drawer_created) = ensure_group(
        big_repo,
        sql,
        DRAWER_GROUP_KEY,
        supplied_ids.map(|ids| ids.default_drawer),
    )
    .await?;

    if repo_agents_created
        || core_docs_created
        || content_docs_created
        || default_drawer_created {
        let local_agent = big_repo.local_keyhive_agent().await?;
        if repo_agents_created {
            big_repo
                .add_admin_member_to_group(local_agent, &repo_agents)
                .await?;
        }
        if core_docs_created {
            big_repo
                .add_admin_member_to_group(repo_agents.clone(), &core_docs)
                .await?;
        }
        if content_docs_created {
            big_repo
                .add_admin_member_to_group(repo_agents.clone(), &content_docs)
                .await?;
        }
        if default_drawer_created {
            big_repo
                .add_admin_member_to_group(repo_agents.clone(), &default_drawer)
                .await?;
        }
    }

    Ok(RepoAuthority {
        repo_agents,
        core_docs,
        content_docs,
        default_drawer,
    })
}

async fn ensure_group(
    big_repo: &SharedBigRepo,
    sql: &SqlCtx,
    key: &str,
    supplied_id: Option<[u8; 32]>,
) -> Res<(BigKeyhiveGroup, bool)> {
    let stored_id = load_group_id(sql, key).await?;
    if let (Some(stored_id), Some(supplied_id)) = (stored_id, supplied_id) {
        if stored_id != supplied_id {
            eyre::bail!("clone authority group id disagrees with local state: {key}");
        }
    }

    let (group_id, created) = match (stored_id, supplied_id) {
        (Some(id), _) => (id, false),
        (None, Some(id)) => {
            persist_group_id(sql, key, id).await?;
            (id, false)
        }
        (None, None) => {
            let group = big_repo.create_group_with_parents(Vec::new()).await?;
            let id = group.id().to_bytes();
            persist_group_id(sql, key, id).await?;
            return Ok((group, true));
        }
    };
    let group = big_repo
        .get_group_by_id(group_id)
        .await
        .ok_or_else(|| ferr!("persisted Keyhive group is unavailable: {key}"))?;
    Ok((group, created))
}

async fn load_group_id(sql: &SqlCtx, key: &str) -> Res<Option<[u8; 32]>> {
    let Some(value) = crate::repo::globals::get_string_global(sql, key).await? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_str(&value).wrap_err_with(|| {
        format!("invalid authority group id stored at {key}")
    })?))
}
pub(crate) async fn persist_ids(sql: &SqlCtx, ids: RepoAuthorityIds) -> Res<()> {
    persist_group_id(sql, REPO_AGENTS_GROUP_KEY, ids.repo_agents).await?;
    persist_group_id(sql, CORE_DOCS_GROUP_KEY, ids.core_docs).await?;
    persist_group_id(sql, CONTENT_DOCS_GROUP_KEY, ids.content_docs).await?;
    persist_group_id(sql, DRAWER_GROUP_KEY, ids.default_drawer).await
}
pub(crate) async fn grant_docs_admin(
    big_repo: &SharedBigRepo,
    group: &BigKeyhiveGroup,
    doc_ids: impl IntoIterator<Item = DocumentId>,
) -> Res<()> {
    for doc_id in doc_ids {
        big_repo
            .add_admin_member_to_doc(doc_id, group.clone())
            .await?;
    }
    Ok(())
}

async fn persist_group_id(sql: &SqlCtx, key: &str, id: [u8; 32]) -> Res<()> {
    crate::repo::globals::upsert_string_global(sql, key, &serde_json::to_string(&id)?).await
}
