use crate::interlude::*;

use daybook_pdk::InvokeCommandAccepted;
use daybook_pdk::InvokeCommandRequest;
use daybook_types::doc::ChangeHashSet;
use daybook_types::doc::DocId;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;

use super::DaybookPlugin;
use super::capabilities;
use super::root_doc;
use super::wit_doc;

fn wasmtime_err(msg: impl std::fmt::Display) -> wasmtime::Error {
    wasmtime::Error::msg(msg.to_string())
}

pub fn facet_rights_from_access(
    access: &daybook_types::manifest::RoutineFacetAccess,
) -> capabilities::FacetRights {
    let mut rights = capabilities::FacetRights::empty();
    if access.read {
        rights |= capabilities::FacetRights::READ;
    }
    if access.write {
        rights |= capabilities::FacetRights::UPDATE;
    }
    if access.create {
        rights |= capabilities::FacetRights::CREATE;
    }
    if access.delete {
        rights |= capabilities::FacetRights::DELETE;
    }
    rights
}

pub fn doc_rights_from_facet_acl(
    _facet_acl: &[daybook_types::manifest::RoutineFacetAccess],
) -> capabilities::DocRights {
    let mut rights = capabilities::DocRights::empty();
    rights |= capabilities::DocRights::META_READ;
    rights |= capabilities::DocRights::FACET_LIST;
    rights
}

async fn ensure_staging_branch(
    plugin: &DaybookPlugin,
    doc_id: &DocId,
    source_branch_path: &daybook_types::doc::BranchPath,
    staging_branch_path: &daybook_types::doc::BranchPath,
    heads: &ChangeHashSet,
) -> Result<(), crate::drawer::types::DrawerError> {
    if source_branch_path == staging_branch_path {
        return Ok(());
    }
    match plugin
        .drawer_repo
        .create_branch_at_heads_from_branch(
            doc_id,
            staging_branch_path,
            source_branch_path,
            heads,
            None,
        )
        .await
    {
        Ok(()) | Err(crate::drawer::types::DrawerError::BranchAlreadyExists { .. }) => Ok(()),
        Err(err) => Err(err),
    }
}

#[derive(Clone)]
pub struct DocToken {
    pub doc_id: DocId,
    pub branch_path: daybook_types::doc::BranchPathBuf,
    pub staging_branch_path: daybook_types::doc::BranchPathBuf,
    pub heads: ChangeHashSet,
    pub rights: capabilities::DocRights,
    pub facet_acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
}

impl capabilities::HostDocToken for SharedWashCtx {
    async fn id(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<String> {
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
        Ok(token.doc_id.clone())
    }

    async fn rights(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<capabilities::DocRights> {
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
        Ok(token.rights)
    }

    async fn clone(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
        downscope: Option<capabilities::DocRights>,
    ) -> wasmtime::Result<
        Result<wasmtime::component::Resource<capabilities::DocToken>, capabilities::AccessError>,
    > {
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
        let new_rights = match downscope {
            Some(down) => {
                if !token.rights.contains(down) {
                    return Ok(Err(capabilities::AccessError::Denied));
                }
                down
            }
            None => token.rights,
        };
        let new_token = self.table.push(DocToken {
            doc_id: token.doc_id.clone(),
            branch_path: token.branch_path.clone(),
            staging_branch_path: token.staging_branch_path.clone(),
            heads: token.heads.clone(),
            rights: new_rights,
            facet_acl: token.facet_acl.clone(),
        })?;
        Ok(Ok(new_token))
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct FacetToken {
    pub doc_id: DocId,
    pub branch_path: daybook_types::doc::BranchPathBuf,
    pub staging_branch_path: daybook_types::doc::BranchPathBuf,
    pub heads: ChangeHashSet,
    pub facet_key: daybook_types::doc::FacetKey,
    pub rights: capabilities::FacetRights,
}

impl capabilities::HostFacetToken for SharedWashCtx {
    async fn key(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<String> {
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        Ok(token.facet_key.to_string())
    }

    async fn rights(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<capabilities::FacetRights> {
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        Ok(token.rights)
    }

    async fn clone(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
        downscope: Option<capabilities::FacetRights>,
    ) -> wasmtime::Result<
        Result<wasmtime::component::Resource<capabilities::FacetToken>, capabilities::AccessError>,
    > {
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        let new_rights = match downscope {
            Some(down) => {
                if !token.rights.contains(down) {
                    return Ok(Err(capabilities::AccessError::Denied));
                }
                down
            }
            None => token.rights,
        };
        let new_token = self.table.push(FacetToken {
            doc_id: token.doc_id.clone(),
            branch_path: token.branch_path.clone(),
            staging_branch_path: token.staging_branch_path.clone(),
            heads: token.heads.clone(),
            facet_key: token.facet_key.clone(),
            rights: new_rights,
        })?;
        Ok(Ok(new_token))
    }

    async fn heads(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<Result<Vec<String>, capabilities::AccessError>> {
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        Ok(Ok(am_utils_rs::serialize_commit_heads(
            token.heads.as_ref(),
        )))
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct FacetCreateToken {
    pub doc_id: DocId,
    pub branch_path: daybook_types::doc::BranchPathBuf,
    pub target_branch_path: daybook_types::doc::BranchPathBuf,
    pub heads: ChangeHashSet,
    pub facet_key: daybook_types::doc::FacetKey,
    pub rights: capabilities::FacetRights,
    pub facet_acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
}

impl capabilities::HostFacetCreateToken for SharedWashCtx {
    async fn key(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetCreateToken>,
    ) -> wasmtime::Result<String> {
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime_err(format!("error locating facet create token: {err}")))?;
        Ok(token.facet_key.to_string())
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::FacetCreateToken>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct FacetTagToken {
    pub doc_id: DocId,
    pub branch_path: daybook_types::doc::BranchPathBuf,
    pub staging_branch_path: daybook_types::doc::BranchPathBuf,
    pub heads: ChangeHashSet,
    pub tag: String,
    pub rights: capabilities::FacetRights,
    pub facet_acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
}

impl capabilities::HostFacetTagToken for SharedWashCtx {
    async fn tag(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTagToken>,
    ) -> wasmtime::Result<String> {
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?;
        Ok(token.tag.clone())
    }

    async fn rights(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTagToken>,
    ) -> wasmtime::Result<capabilities::FacetRights> {
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?;
        Ok(token.rights)
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::FacetTagToken>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

impl capabilities::HostCommandInvokeToken for SharedWashCtx {
    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::CommandInvokeToken>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

impl capabilities::Host for SharedWashCtx {}

#[derive(Clone)]
pub struct CommandInvokeToken {
    pub parent_wflow_job_id: Arc<str>,
    pub target_url: String,
}

pub(super) fn with_host_ctx<T: 'static, R>(
    accessor: &wasmtime::component::Accessor<T, wasmtime::component::HasSelf<SharedWashCtx>>,
    f: impl FnOnce(&mut SharedWashCtx) -> R,
) -> R {
    accessor.with(|mut access| f(access.get()))
}

impl<T: 'static> capabilities::HostDocTokenWithStore<T>
    for wasmtime::component::HasSelf<SharedWashCtx>
{
    async fn get_meta(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<Result<capabilities::DocMeta, capabilities::AccessError>> {
        let (plugin, token) = with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?
                .clone();
            Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
        })?;
        if !token.rights.contains(capabilities::DocRights::META_READ) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let Some(doc) = plugin
            .get_doc(&token.doc_id, &token.branch_path, &token.heads)
            .await
            .map_err(wasmtime_err)?
        else {
            return Ok(Err(capabilities::AccessError::NotFound));
        };
        let dmeta = match doc.facets.get(&daybook_types::doc::FacetKey::from(
            daybook_types::doc::WellKnownFacetTag::Dmeta,
        )) {
            Some(facet) => match daybook_types::doc::WellKnownFacet::from_json(
                facet.clone(),
                daybook_types::doc::WellKnownFacetTag::Dmeta,
            ) {
                Ok(daybook_types::doc::WellKnownFacet::Dmeta(dmeta)) => Some(dmeta),
                _ => None,
            },
            None => None,
        };
        let created_at = dmeta
            .as_ref()
            .map(|meta| meta.created_at.as_second() as u64)
            .unwrap_or(0);
        let updated_at = dmeta
            .as_ref()
            .map(|meta| {
                meta.updated_at
                    .iter()
                    .map(|ts| ts.as_second() as u64)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(Ok(capabilities::DocMeta {
            created_at: capabilities::Datetime {
                seconds: created_at,
                nanoseconds: 0,
            },
            updated_at: updated_at
                .into_iter()
                .map(|seconds| capabilities::Datetime {
                    seconds,
                    nanoseconds: 0,
                })
                .collect(),
        }))
    }

    async fn list_facets(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<
        Result<
            Vec<(
                String,
                wasmtime::component::Resource<capabilities::FacetToken>,
            )>,
            capabilities::AccessError,
        >,
    > {
        let (plugin, doc_id, branch_path, staging_branch_path, heads, facet_acl) =
            with_host_ctx(accessor, |ctx| {
                let token = ctx
                    .table
                    .get(&handle)
                    .map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?
                    .clone();
                Ok::<_, wasmtime::Error>((
                    DaybookPlugin::from_ctx(ctx),
                    token.doc_id,
                    token.branch_path,
                    token.staging_branch_path,
                    token.heads,
                    token.facet_acl,
                ))
            })?;
        if !with_host_ctx(accessor, |ctx| {
            ctx.table
                .get(&handle)
                .map(|token| token.rights.contains(capabilities::DocRights::FACET_LIST))
                .map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))
        })? {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let Some(doc) = plugin
            .get_doc(&doc_id, &branch_path, &heads)
            .await
            .map_err(wasmtime_err)?
        else {
            return Ok(Err(capabilities::AccessError::NotFound));
        };
        let mut values = Vec::new();
        for facet_key in doc.facets.keys() {
            let mut rights = capabilities::FacetRights::empty();
            for access in &facet_acl {
                if access.tag.0 != facet_key.tag.to_string()
                    || access.key_id.as_ref().is_some_and(|id| *id != facet_key.id)
                {
                    continue;
                }
                rights |= facet_rights_from_access(access);
            }
            if rights == capabilities::FacetRights::empty() {
                continue;
            }
            values.push((facet_key.to_string(), facet_key.clone(), rights));
        }
        let result = with_host_ctx(accessor, |ctx| {
            values
                .into_iter()
                .map(|(key, facet_key, rights)| {
                    let resource = ctx.table.push(FacetToken {
                        doc_id: doc_id.clone(),
                        branch_path: branch_path.clone(),
                        staging_branch_path: staging_branch_path.clone(),
                        heads: heads.clone(),
                        facet_key,
                        rights,
                    })?;
                    Ok::<_, wasmtime::Error>((key, resource))
                })
                .collect::<wasmtime::Result<Vec<_>>>()
        })?;
        Ok(Ok(result))
    }

    async fn get_facet(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
        key: String,
    ) -> wasmtime::Result<
        Result<
            Option<wasmtime::component::Resource<capabilities::FacetToken>>,
            capabilities::AccessError,
        >,
    > {
        let (plugin, token) = with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?
                .clone();
            Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
        })?;
        if !token.rights.contains(capabilities::DocRights::FACET_LIST) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let facet_key = daybook_types::doc::FacetKey::from(key.as_str());
        let Some(doc) = plugin
            .get_doc(&token.doc_id, &token.branch_path, &token.heads)
            .await
            .map_err(wasmtime_err)?
        else {
            return Ok(Err(capabilities::AccessError::NotFound));
        };
        if !doc.facets.contains_key(&facet_key) {
            return Ok(Ok(None));
        }
        let mut rights = capabilities::FacetRights::empty();
        for access in &token.facet_acl {
            if access.tag.0 != facet_key.tag.to_string()
                || access.key_id.as_ref().is_some_and(|id| *id != facet_key.id)
            {
                continue;
            }
            rights |= facet_rights_from_access(access);
        }
        if rights == capabilities::FacetRights::empty() {
            return Ok(Ok(None));
        }
        let resource = with_host_ctx(accessor, |ctx| {
            ctx.table.push(FacetToken {
                doc_id: token.doc_id.clone(),
                branch_path: token.branch_path.clone(),
                staging_branch_path: token.staging_branch_path.clone(),
                heads: token.heads.clone(),
                facet_key,
                rights,
            })
        })?;
        Ok(Ok(Some(resource)))
    }

    async fn list_tags(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<
        Result<
            Vec<(
                String,
                wasmtime::component::Resource<capabilities::FacetTagToken>,
            )>,
            capabilities::AccessError,
        >,
    > {
        let result: std::result::Result<
            Vec<(
                String,
                wasmtime::component::Resource<capabilities::FacetTagToken>,
            )>,
            capabilities::AccessError,
        > = with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?
                .clone();
            if !token.rights.contains(capabilities::DocRights::FACET_LIST) {
                return Ok::<_, wasmtime::Error>(Err(capabilities::AccessError::Denied));
            }
            let mut tag_rights = std::collections::HashMap::new();
            for access in &token.facet_acl {
                if access.key_id.is_none() {
                    tag_rights
                        .entry(access.tag.0.clone())
                        .and_modify(|rights| *rights |= facet_rights_from_access(access))
                        .or_insert_with(|| facet_rights_from_access(access));
                }
            }
            let values = tag_rights
                .into_iter()
                .map(|(tag, rights)| {
                    let resource = ctx.table.push(FacetTagToken {
                        doc_id: token.doc_id.clone(),
                        branch_path: token.branch_path.clone(),
                        staging_branch_path: token.staging_branch_path.clone(),
                        heads: token.heads.clone(),
                        tag: tag.clone(),
                        rights,
                        facet_acl: token.facet_acl.clone(),
                    })?;
                    Ok::<_, wasmtime::Error>((tag, resource))
                })
                .collect::<wasmtime::Result<Vec<_>>>()?;
            Ok(Ok(values))
        })?;
        Ok(result)
    }
}

impl<T: 'static> capabilities::HostFacetTokenWithStore<T>
    for wasmtime::component::HasSelf<SharedWashCtx>
{
    async fn meta(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<Result<capabilities::FacetMeta, capabilities::AccessError>> {
        let (plugin, token) = with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?
                .clone();
            Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
        })?;
        if !token.rights.contains(capabilities::FacetRights::READ) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let Some(doc) = plugin
            .get_doc(&token.doc_id, &token.branch_path, &token.heads)
            .await
            .map_err(wasmtime_err)?
        else {
            return Ok(Err(capabilities::AccessError::NotFound));
        };
        let dmeta = match doc.facets.get(&daybook_types::doc::FacetKey::from(
            daybook_types::doc::WellKnownFacetTag::Dmeta,
        )) {
            Some(facet) => match daybook_types::doc::WellKnownFacet::from_json(
                facet.clone(),
                daybook_types::doc::WellKnownFacetTag::Dmeta,
            ) {
                Ok(daybook_types::doc::WellKnownFacet::Dmeta(meta)) => meta,
                _ => {
                    return Ok(Err(capabilities::AccessError::Other(
                        "invalid dmeta facet".into(),
                    )));
                }
            },
            None => return Ok(Err(capabilities::AccessError::NotFound)),
        };
        let Some(facet_meta) = dmeta.facets.get(&token.facet_key) else {
            return Ok(Err(capabilities::AccessError::NotFound));
        };
        Ok(Ok(capabilities::FacetMeta {
            created_at: capabilities::Datetime {
                seconds: facet_meta.created_at.as_second() as u64,
                nanoseconds: 0,
            },
        }))
    }

    async fn get(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<Result<String, capabilities::AccessError>> {
        let (plugin, token) = with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?
                .clone();
            Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
        })?;
        if !token.rights.contains(capabilities::FacetRights::READ) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let Some(doc) = plugin
            .get_doc(&token.doc_id, &token.branch_path, &token.heads)
            .await
            .map_err(wasmtime_err)?
        else {
            return Ok(Err(capabilities::AccessError::NotFound));
        };
        let Some(facet) = doc.facets.get(&token.facet_key) else {
            return Ok(Err(capabilities::AccessError::NotFound));
        };
        Ok(Ok(wit_doc::facet_from(facet)))
    }

    async fn update(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
        facet_json: String,
    ) -> wasmtime::Result<Result<Result<(), capabilities::UpdateDocError>, capabilities::AccessError>>
    {
        let (plugin, token) = with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?
                .clone();
            Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
        })?;
        if !token.rights.contains(capabilities::FacetRights::UPDATE) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let facet: daybook_types::doc::FacetRaw = wit_doc::facet_into(&facet_json)
            .map_err(|err| capabilities::UpdateDocError::InvalidPatch(err.to_string()))?;
        if let Err(err) = ensure_staging_branch(
            &plugin,
            &token.doc_id,
            &token.branch_path,
            &token.staging_branch_path,
            &token.heads,
        )
        .await
        {
            return Ok(Ok(Err(match err {
                crate::drawer::types::DrawerError::DocNotFound { .. }
                | crate::drawer::types::DrawerError::BranchNotFound { .. } => {
                    capabilities::UpdateDocError::Other("document or branch not found".into())
                }
                crate::drawer::types::DrawerError::InvalidKey {
                    inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag },
                } => capabilities::UpdateDocError::InvalidKey(tag),
                crate::drawer::types::DrawerError::Other { inner } => {
                    return Err(wasmtime_err(format!("unexpected error: {inner}")));
                }
                crate::drawer::types::DrawerError::BranchAlreadyExists { .. } => unreachable!(),
            })));
        }
        match plugin
            .drawer_repo
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: token.doc_id.clone(),
                    facets_set: HashMap::from([(token.facet_key.clone(), facet)]),
                    facets_remove: default(),
                    user_path: None,
                },
                &token.staging_branch_path,
                None,
            )
            .await
        {
            Ok(_) => Ok(Ok(Ok(()))),
            Err(crate::drawer::types::DrawerError::DocNotFound { .. })
            | Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => Ok(Ok(Err(
                capabilities::UpdateDocError::Other("document or branch not found".into()),
            ))),
            Err(crate::drawer::types::DrawerError::InvalidKey {
                inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag },
            }) => Ok(Ok(Err(capabilities::UpdateDocError::InvalidKey(tag)))),
            Err(crate::drawer::types::DrawerError::Other { inner }) => {
                Err(wasmtime_err(format!("unexpected error: {inner}")))
            }
            Err(crate::drawer::types::DrawerError::BranchAlreadyExists { .. }) => {
                Err(wasmtime_err("unexpected branch already exists"))
            }
        }
    }
}

impl<T: 'static> capabilities::HostFacetCreateTokenWithStore<T>
    for wasmtime::component::HasSelf<SharedWashCtx>
{
    async fn create_facet(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::FacetCreateToken>,
        data: String,
    ) -> wasmtime::Result<
        Result<
            wasmtime::component::Resource<capabilities::FacetToken>,
            capabilities::UpdateDocError,
        >,
    > {
        let (plugin, token) = with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating facet create token: {err}")))?
                .clone();
            Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
        })?;
        let facet: daybook_types::doc::FacetRaw = wit_doc::facet_into(&data)
            .map_err(|err| capabilities::UpdateDocError::InvalidPatch(err.to_string()))?;
        if let Err(err) = ensure_staging_branch(
            &plugin,
            &token.doc_id,
            &token.branch_path,
            &token.target_branch_path,
            &token.heads,
        )
        .await
        {
            return Ok(Err(match err {
                crate::drawer::types::DrawerError::DocNotFound { .. }
                | crate::drawer::types::DrawerError::BranchNotFound { .. } => {
                    capabilities::UpdateDocError::Other("document or branch not found".into())
                }
                crate::drawer::types::DrawerError::InvalidKey {
                    inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag },
                } => capabilities::UpdateDocError::InvalidKey(tag),
                crate::drawer::types::DrawerError::Other { inner } => {
                    return Err(wasmtime_err(format!("unexpected error: {inner}")));
                }
                crate::drawer::types::DrawerError::BranchAlreadyExists { .. } => unreachable!(),
            }));
        }
        match plugin
            .drawer_repo
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: token.doc_id.clone(),
                    facets_set: HashMap::from([(token.facet_key.clone(), facet)]),
                    facets_remove: default(),
                    user_path: None,
                },
                &token.target_branch_path,
                None,
            )
            .await
        {
            Ok(_) => {
                let Some(branches) = plugin
                    .drawer_repo
                    .get_doc_branches(&token.doc_id)
                    .await
                    .map_err(wasmtime_err)?
                else {
                    return Ok(Err(capabilities::UpdateDocError::Other(
                        "doc not found".into(),
                    )));
                };
                let Some(heads) = branches
                    .branches
                    .get(token.target_branch_path.as_str())
                    .cloned()
                else {
                    return Ok(Err(capabilities::UpdateDocError::Other(
                        "branch not found".into(),
                    )));
                };
                let resource = with_host_ctx(accessor, |ctx| {
                    ctx.table.push(FacetToken {
                        doc_id: token.doc_id.clone(),
                        branch_path: token.target_branch_path.clone(),
                        staging_branch_path: token.target_branch_path.clone(),
                        heads,
                        facet_key: token.facet_key.clone(),
                        rights: token.rights,
                    })
                })?;
                Ok(Ok(resource))
            }
            Err(crate::drawer::types::DrawerError::DocNotFound { .. })
            | Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => Ok(Err(
                capabilities::UpdateDocError::Other("document or branch not found".into()),
            )),
            Err(crate::drawer::types::DrawerError::InvalidKey {
                inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag },
            }) => Ok(Err(capabilities::UpdateDocError::InvalidKey(tag))),
            Err(crate::drawer::types::DrawerError::Other { inner }) => {
                Err(wasmtime_err(format!("unexpected error: {inner}")))
            }
            Err(crate::drawer::types::DrawerError::BranchAlreadyExists { .. }) => {
                Err(wasmtime_err("unexpected branch already exists"))
            }
        }
    }
}

impl<T: 'static> capabilities::HostFacetTagTokenWithStore<T>
    for wasmtime::component::HasSelf<SharedWashCtx>
{
    async fn list_facets(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::FacetTagToken>,
    ) -> wasmtime::Result<
        Result<
            Vec<wasmtime::component::Resource<capabilities::FacetToken>>,
            capabilities::AccessError,
        >,
    > {
        let (plugin, token) = with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?
                .clone();
            Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
        })?;
        if !token.rights.contains(capabilities::FacetRights::READ) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let Some(doc) = plugin
            .get_doc(&token.doc_id, &token.branch_path, &token.heads)
            .await
            .map_err(wasmtime_err)?
        else {
            return Ok(Err(capabilities::AccessError::NotFound));
        };
        let mut values = Vec::new();
        for facet_key in doc.facets.keys() {
            if facet_key.tag.to_string() != token.tag {
                continue;
            }
            let mut rights = capabilities::FacetRights::empty();
            for access in &token.facet_acl {
                if access.tag.0 == token.tag
                    && access.key_id.as_ref().is_none_or(|id| id == &facet_key.id)
                {
                    rights |= facet_rights_from_access(access);
                }
            }
            if rights != capabilities::FacetRights::empty() {
                values.push((facet_key.clone(), rights));
            }
        }
        let result = with_host_ctx(accessor, |ctx| {
            values
                .into_iter()
                .map(|(facet_key, rights)| {
                    Ok::<_, wasmtime::Error>(ctx.table.push(FacetToken {
                        doc_id: token.doc_id.clone(),
                        branch_path: token.branch_path.clone(),
                        staging_branch_path: token.staging_branch_path.clone(),
                        heads: token.heads.clone(),
                        facet_key,
                        rights,
                    })?)
                })
                .collect::<wasmtime::Result<Vec<_>>>()
        })?;
        Ok(Ok(result))
    }

    async fn get(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::FacetTagToken>,
        key_id: String,
    ) -> wasmtime::Result<
        Result<
            Option<wasmtime::component::Resource<capabilities::FacetToken>>,
            capabilities::AccessError,
        >,
    > {
        let (plugin, token) = with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?
                .clone();
            Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
        })?;
        if !token.rights.contains(capabilities::FacetRights::READ) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let facet_key = daybook_types::doc::FacetKey {
            tag: daybook_types::doc::FacetTag::from(token.tag.as_str()),
            id: key_id,
        };
        let Some(doc) = plugin
            .get_doc(&token.doc_id, &token.branch_path, &token.heads)
            .await
            .map_err(wasmtime_err)?
        else {
            return Ok(Err(capabilities::AccessError::NotFound));
        };
        if !doc.facets.contains_key(&facet_key) {
            return Ok(Ok(None));
        }
        let mut rights = capabilities::FacetRights::empty();
        for access in &token.facet_acl {
            if access.tag.0 == token.tag
                && access.key_id.as_ref().is_none_or(|id| id == &facet_key.id)
            {
                rights |= facet_rights_from_access(access);
            }
        }
        if rights == capabilities::FacetRights::empty() {
            return Ok(Ok(None));
        }
        let resource = with_host_ctx(accessor, |ctx| {
            ctx.table.push(FacetToken {
                doc_id: token.doc_id.clone(),
                branch_path: token.branch_path.clone(),
                staging_branch_path: token.staging_branch_path.clone(),
                heads: token.heads.clone(),
                facet_key,
                rights,
            })
        })?;
        Ok(Ok(Some(resource)))
    }

    async fn create(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::FacetTagToken>,
        key_id: String,
        data: String,
    ) -> wasmtime::Result<
        Result<
            wasmtime::component::Resource<capabilities::FacetToken>,
            capabilities::UpdateDocError,
        >,
    > {
        let (plugin, token) = with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?
                .clone();
            Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
        })?;
        if !token.rights.contains(capabilities::FacetRights::CREATE) {
            return Ok(Err(capabilities::UpdateDocError::Other(
                "create denied".into(),
            )));
        }
        let key_authorized = token.facet_acl.iter().any(|access| {
            access.tag.0 == token.tag
                && access.create
                && (access.key_id.is_none() || access.key_id.as_deref() == Some(&key_id))
        });
        if !key_authorized {
            return Ok(Err(capabilities::UpdateDocError::Other(format!(
                "create denied for key_id: {key_id}"
            ))));
        }
        let facet_key = daybook_types::doc::FacetKey {
            tag: daybook_types::doc::FacetTag::from(token.tag.as_str()),
            id: key_id,
        };
        let facet: daybook_types::doc::FacetRaw = wit_doc::facet_into(&data)
            .map_err(|err| capabilities::UpdateDocError::InvalidPatch(err.to_string()))?;
        ensure_staging_branch(
            &plugin,
            &token.doc_id,
            &token.branch_path,
            &token.staging_branch_path,
            &token.heads,
        )
        .await
        .map_err(|err| wasmtime_err(format!("failed to ensure staging branch: {err}")))?;
        match plugin
            .drawer_repo
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: token.doc_id.clone(),
                    facets_set: HashMap::from([(facet_key.clone(), facet)]),
                    facets_remove: default(),
                    user_path: None,
                },
                &token.staging_branch_path,
                None,
            )
            .await
        {
            Ok(_) => {
                let Some(branches) = plugin
                    .drawer_repo
                    .get_doc_branches(&token.doc_id)
                    .await
                    .map_err(wasmtime_err)?
                else {
                    return Ok(Err(capabilities::UpdateDocError::Other(
                        "doc not found".into(),
                    )));
                };
                let Some(heads) = branches
                    .branches
                    .get(token.staging_branch_path.as_str())
                    .cloned()
                else {
                    return Ok(Err(capabilities::UpdateDocError::Other(
                        "branch not found".into(),
                    )));
                };
                let resource = with_host_ctx(accessor, |ctx| {
                    ctx.table.push(FacetToken {
                        doc_id: token.doc_id.clone(),
                        branch_path: token.staging_branch_path.clone(),
                        staging_branch_path: token.staging_branch_path.clone(),
                        heads,
                        facet_key,
                        rights: token.rights,
                    })
                })?;
                Ok(Ok(resource))
            }
            Err(crate::drawer::types::DrawerError::DocNotFound { .. })
            | Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => Ok(Err(
                capabilities::UpdateDocError::Other("document or branch not found".into()),
            )),
            Err(crate::drawer::types::DrawerError::InvalidKey {
                inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag },
            }) => Ok(Err(capabilities::UpdateDocError::InvalidKey(tag))),
            Err(crate::drawer::types::DrawerError::Other { inner }) => {
                Err(wasmtime_err(format!("unexpected error: {inner}")))
            }
            Err(crate::drawer::types::DrawerError::BranchAlreadyExists { .. }) => {
                Err(wasmtime_err("unexpected branch already exists"))
            }
        }
    }

    async fn get_create_token(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::FacetTagToken>,
        key_id: String,
    ) -> wasmtime::Result<
        Result<
            wasmtime::component::Resource<capabilities::FacetCreateToken>,
            capabilities::UpdateDocError,
        >,
    > {
        with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?
                .clone();
            if !token.rights.contains(capabilities::FacetRights::CREATE) {
                return Ok(Err(capabilities::UpdateDocError::Other(
                    "create denied".into(),
                )));
            }
            let authorized = token.facet_acl.iter().any(|access| {
                access.tag.0 == token.tag
                    && access.create
                    && (access.key_id.is_none() || access.key_id.as_deref() == Some(&key_id))
            });
            if !authorized {
                return Ok(Err(capabilities::UpdateDocError::Other(format!(
                    "create denied for key_id: {key_id}"
                ))));
            }
            let facet_key = daybook_types::doc::FacetKey {
                tag: daybook_types::doc::FacetTag::from(token.tag.as_str()),
                id: key_id,
            };
            let resource = ctx.table.push(FacetCreateToken {
                doc_id: token.doc_id,
                branch_path: token.branch_path,
                target_branch_path: token.staging_branch_path,
                heads: token.heads,
                facet_key,
                rights: token.rights,
                facet_acl: token.facet_acl,
            })?;
            Ok(Ok(resource))
        })
    }
}

impl<T: 'static> capabilities::HostWithStore<T> for wasmtime::component::HasSelf<SharedWashCtx> {
    async fn delete_facet(
        accessor: &wasmtime::component::Accessor<T, Self>,
        token: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<Result<(), capabilities::AccessError>> {
        let (plugin, facet) = with_host_ctx(accessor, |ctx| {
            let facet = ctx
                .table
                .get(&token)
                .map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?
                .clone();
            Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), facet))
        })?;
        if !facet.rights.contains(capabilities::FacetRights::DELETE) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        ensure_staging_branch(
            &plugin,
            &facet.doc_id,
            &facet.branch_path,
            &facet.staging_branch_path,
            &facet.heads,
        )
        .await
        .map_err(|err| wasmtime_err(format!("failed to ensure staging branch: {err}")))?;
        match plugin
            .drawer_repo
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: facet.doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![facet.facet_key.clone()],
                    user_path: None,
                },
                &facet.staging_branch_path,
                None,
            )
            .await
        {
            Ok(_) => {
                with_host_ctx(accessor, |ctx| ctx.table.delete(token))?;
                Ok(Ok(()))
            }
            Err(crate::drawer::types::DrawerError::DocNotFound { .. })
            | Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => {
                Ok(Err(capabilities::AccessError::NotFound))
            }
            Err(crate::drawer::types::DrawerError::Other { inner }) => {
                Err(wasmtime_err(format!("unexpected error: {inner}")))
            }
            Err(crate::drawer::types::DrawerError::InvalidKey { .. })
            | Err(crate::drawer::types::DrawerError::BranchAlreadyExists { .. }) => {
                Err(wasmtime_err("unexpected error deleting facet"))
            }
        }
    }

    async fn delete_doc(
        accessor: &wasmtime::component::Accessor<T, Self>,
        token: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<Result<(), capabilities::AccessError>> {
        with_host_ctx(accessor, |ctx| {
            let doc = ctx
                .table
                .get(&token)
                .map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
            if !doc.rights.contains(capabilities::DocRights::DELETE) {
                return Ok(Err(capabilities::AccessError::Denied));
            }
            Ok(Err(capabilities::AccessError::Other(
                "doc deletion not yet implemented".into(),
            )))
        })
    }
}

impl<T: 'static> capabilities::HostCommandInvokeTokenWithStore<T>
    for wasmtime::component::HasSelf<SharedWashCtx>
{
    async fn invoke(
        accessor: &wasmtime::component::Accessor<T, Self>,
        handle: wasmtime::component::Resource<capabilities::CommandInvokeToken>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, capabilities::InvokeCommandError>> {
        let (plugin, token) = with_host_ctx(accessor, |ctx| {
            let token = ctx
                .table
                .get(&handle)
                .map_err(|err| wasmtime_err(format!("error locating command invoke token: {err}")))?
                .clone();
            Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
        })?;
        let request: InvokeCommandRequest = match serde_json::from_str(&request_json) {
            Ok(value) => value,
            Err(err) => {
                return Ok(Err(capabilities::InvokeCommandError::BadRequest(
                    err.to_string(),
                )));
            }
        };
        let rt = match plugin.rt() {
            Ok(rt) => rt,
            Err(err) => {
                return Ok(Err(capabilities::InvokeCommandError::Other(
                    err.to_string(),
                )));
            }
        };
        let dispatch_id = match rt
            .invoke_command_from_wflow_job(&token.parent_wflow_job_id, &token.target_url, request)
            .await
        {
            Ok(dispatch_id) => dispatch_id,
            Err(crate::rt::InvokeCommandFromWflowError::Denied(reason)) => {
                return Ok(Err(capabilities::InvokeCommandError::Denied(reason)));
            }
            Err(crate::rt::InvokeCommandFromWflowError::Other(err)) => {
                return Ok(Err(capabilities::InvokeCommandError::Other(
                    err.to_string(),
                )));
            }
        };
        let response_json =
            serde_json::to_string(&InvokeCommandAccepted { dispatch_id }).expect(ERROR_JSON);
        Ok(Ok(response_json))
    }
}

pub(super) async fn get_blob_facet_from_owned_token(
    plugin: &Arc<DaybookPlugin>,
    token: &FacetToken,
) -> Result<daybook_types::doc::Blob, String> {
    let Some(doc) = plugin
        .get_doc(&token.doc_id, &token.branch_path, &token.heads)
        .await
        .map_err(|err| err.to_string())?
    else {
        return Err(format!("doc not found: {}", token.doc_id));
    };
    let Some(facet_raw) = doc.facets.get(&token.facet_key) else {
        return Err(format!("facet not found: {}", token.facet_key));
    };
    let blob_facet_value = daybook_types::doc::WellKnownFacet::from_json(
        facet_raw.clone(),
        daybook_types::doc::WellKnownFacetTag::Blob,
    )
    .map_err(|err| err.to_string())?;
    let daybook_types::doc::WellKnownFacet::Blob(blob) = blob_facet_value else {
        return Err("facet is not a blob".to_string());
    };
    Ok(blob)
}

pub(super) async fn resolve_blob_path_from_blob_facet(
    plugin: &DaybookPlugin,
    blob: &daybook_types::doc::Blob,
) -> Result<std::path::PathBuf, String> {
    let hash = blob_hash_from_blob_facet(blob)?;
    let blob_id = hash
        .parse::<crate::blobs::BlobId>()
        .map_err(|err| err.to_string())?;
    plugin
        .blobs_repo
        .get_path(blob_id)
        .await
        .map_err(|err| err.to_string())
}

pub(super) fn blob_hash_from_blob_facet(blob: &daybook_types::doc::Blob) -> Result<String, String> {
    let Some(urls) = blob.urls.as_ref() else {
        return Err("blob facet is missing urls".to_string());
    };
    let Some(first_url) = urls.first() else {
        return Err("blob facet urls is empty".to_string());
    };

    let parsed_url = url::Url::parse(first_url).map_err(|err| err.to_string())?;
    if parsed_url.scheme() != crate::blobs::BLOB_SCHEME {
        return Err(format!(
            "unsupported blob url scheme '{}'",
            parsed_url.scheme()
        ));
    }
    if parsed_url.host_str().is_some() {
        return Err("blob url authority must be empty".to_string());
    }

    let hash = parsed_url.path().trim_start_matches('/');
    if hash.is_empty() {
        return Err("blob url path is missing hash".to_string());
    }
    Ok(hash.to_string())
}
