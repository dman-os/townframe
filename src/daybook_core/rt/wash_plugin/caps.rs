use crate::interlude::*;

use daybook_pdk::{InvokeCommandAccepted, InvokeCommandRequest};
use daybook_types::doc::ChangeHashSet;
use daybook_types::doc::DocId;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;

use super::{bindgen_doc, binds_guest, capabilities, drawer, root_doc, wit_doc, DaybookPlugin};

fn wasmtime_err(msg: impl std::fmt::Display) -> wasmtime::Error {
    wasmtime::Error::msg(msg.to_string())
}

pub fn facet_rights_from_access(access: &daybook_types::manifest::RoutineFacetAccess) -> capabilities::FacetRights {
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

pub fn doc_rights_from_facet_acl(_facet_acl: &[daybook_types::manifest::RoutineFacetAccess]) -> capabilities::DocRights {
    let mut rights = capabilities::DocRights::empty();
    rights |= capabilities::DocRights::META_READ;
    rights |= capabilities::DocRights::FACET_LIST;
    rights
}

pub struct DocToken {
    pub doc_id: DocId,
    pub branch_path: daybook_types::doc::BranchPath,
    pub heads: ChangeHashSet,
    pub rights: capabilities::DocRights,
    pub facet_acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
}

impl capabilities::HostDocToken for SharedWashCtx {
    async fn id(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<String> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
        Ok(token.doc_id.clone())
    }

    async fn rights(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<capabilities::DocRights> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
        Ok(token.rights)
    }

    async fn clone(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
        downscope: Option<capabilities::DocRights>,
    ) -> wasmtime::Result<Result<wasmtime::component::Resource<capabilities::DocToken>, capabilities::AccessError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
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
            heads: token.heads.clone(),
            rights: new_rights,
            facet_acl: token.facet_acl.clone(),
        })?;
        Ok(Ok(new_token))
    }

    async fn get_meta(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<Result<capabilities::DocMeta, capabilities::AccessError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
        if !token.rights.contains(capabilities::DocRights::META_READ) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let plugin = DaybookPlugin::from_ctx(self);
        let doc = match plugin.get_doc(&token.doc_id, &token.branch_path, &token.heads).await.map_err(wasmtime_err)? {
            Some(doc) => doc,
            None => return Ok(Err(capabilities::AccessError::NotFound)),
        };
        let dmeta = match doc.facets.get(&daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::Dmeta)) {
            Some(facet) => match daybook_types::doc::WellKnownFacet::from_json(facet.clone(), daybook_types::doc::WellKnownFacetTag::Dmeta) {
                Ok(daybook_types::doc::WellKnownFacet::Dmeta(dmeta)) => Some(dmeta),
                _ => None,
            },
            None => None,
        };
        let created_at = dmeta.as_ref().map(|m| m.created_at.as_second() as u64).unwrap_or(0);
        let updated_at: Vec<u64> = dmeta.as_ref().map(|m| m.updated_at.iter().map(|t| t.as_second() as u64).collect()).unwrap_or_default();
        Ok(Ok(capabilities::DocMeta {
            created_at: capabilities::Datetime { seconds: created_at, nanoseconds: 0 },
            updated_at: updated_at.into_iter().map(|s| capabilities::Datetime { seconds: s, nanoseconds: 0 }).collect(),
        }))
    }

    async fn list_facets(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<Result<Vec<(String, wasmtime::component::Resource<capabilities::FacetToken>)>, capabilities::AccessError>> {
        let (doc_id, branch_path, heads, _rights, facet_acl) = {
            let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
            if !token.rights.contains(capabilities::DocRights::FACET_LIST) {
                return Ok(Err(capabilities::AccessError::Denied));
            }
            (token.doc_id.clone(), token.branch_path.clone(), token.heads.clone(), token.rights, token.facet_acl.clone())
        };
        let plugin = DaybookPlugin::from_ctx(self);
        let doc = match plugin.get_doc(&doc_id, &branch_path, &heads).await.map_err(wasmtime_err)? {
            Some(doc) => doc,
            None => return Ok(Err(capabilities::AccessError::NotFound)),
        };
        let mut result = Vec::new();
        for (facet_key, _facet_value) in doc.facets.iter() {
            let mut rights = capabilities::FacetRights::empty();
            for access in &facet_acl {
                if access.tag.0 != facet_key.tag.to_string() {
                    continue;
                }
                if let Some(ref id) = access.key_id {
                    if *id != facet_key.id {
                        continue;
                    }
                }
                rights |= facet_rights_from_access(access);
            }
            if rights == capabilities::FacetRights::empty() {
                continue;
            }
            let ftoken = self.table.push(FacetToken {
                doc_id: doc_id.clone(),
                branch_path: branch_path.clone(),
                heads: heads.clone(),
                facet_key: facet_key.clone(),
                rights,
            })?;
            result.push((facet_key.to_string(), ftoken));
        }
        Ok(Ok(result))
    }

    async fn get_facet(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
        key: String,
    ) -> wasmtime::Result<Result<Option<wasmtime::component::Resource<capabilities::FacetToken>>, capabilities::AccessError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
        if !token.rights.contains(capabilities::DocRights::FACET_LIST) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let facet_key = daybook_types::doc::FacetKey::from(key.as_str());
        let plugin = DaybookPlugin::from_ctx(self);
        let doc = match plugin.get_doc(&token.doc_id, &token.branch_path, &token.heads).await.map_err(wasmtime_err)? {
            Some(doc) => doc,
            None => return Ok(Err(capabilities::AccessError::NotFound)),
        };
        if !doc.facets.contains_key(&facet_key) {
            return Ok(Ok(None));
        }
        let mut rights = capabilities::FacetRights::empty();
        for access in &token.facet_acl {
            if access.tag.0 != facet_key.tag.to_string() {
                continue;
            }
            if let Some(ref id) = access.key_id {
                if *id != facet_key.id {
                    continue;
                }
            }
            rights |= facet_rights_from_access(access);
        }
if rights == capabilities::FacetRights::empty() {
            return Ok(Ok(None));
        }
        let ftoken = self.table.push(FacetToken {
            doc_id: token.doc_id.clone(),
            branch_path: token.branch_path.clone(),
            heads: token.heads.clone(),
            facet_key,
            rights,
        })?;
        Ok(Ok(Some(ftoken)))
    }

    async fn list_tags(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<Result<Vec<(String, wasmtime::component::Resource<capabilities::FacetTagToken>)>, capabilities::AccessError>> {
        let (doc_id, branch_path, heads, _rights, facet_acl) = {
            let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
            if !token.rights.contains(capabilities::DocRights::FACET_LIST) {
                return Ok(Err(capabilities::AccessError::Denied));
            }
            (token.doc_id.clone(), token.branch_path.clone(), token.heads.clone(), token.rights, token.facet_acl.clone())
        };
        let mut tag_rights: std::collections::HashMap<String, capabilities::FacetRights> = std::collections::HashMap::new();
        for access in &facet_acl {
            if access.key_id.is_some() {
                continue;
            }
            let tag_str = access.tag.0.clone();
            let entry_rights = facet_rights_from_access(access);
            tag_rights.entry(tag_str).and_modify(|rights| *rights |= entry_rights).or_insert(entry_rights);
        }
        let mut result = Vec::new();
        for (tag_str, rights) in tag_rights {
            let ttoken = self.table.push(FacetTagToken {
                doc_id: doc_id.clone(),
                branch_path: branch_path.clone(),
                heads: heads.clone(),
                tag: tag_str.clone(),
                rights,
                facet_acl: facet_acl.clone(),
            })?;
            result.push((tag_str, ttoken));
        }
        Ok(Ok(result))
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

pub struct FacetToken {
    pub doc_id: DocId,
    pub branch_path: daybook_types::doc::BranchPath,
    pub heads: ChangeHashSet,
    pub facet_key: daybook_types::doc::FacetKey,
    pub rights: capabilities::FacetRights,
}

impl capabilities::HostFacetToken for SharedWashCtx {
    async fn key(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<String> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        Ok(token.facet_key.to_string())
    }

    async fn rights(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<capabilities::FacetRights> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        Ok(token.rights)
    }

    async fn clone(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
        downscope: Option<capabilities::FacetRights>,
    ) -> wasmtime::Result<Result<wasmtime::component::Resource<capabilities::FacetToken>, capabilities::AccessError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
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
            heads: token.heads.clone(),
            facet_key: token.facet_key.clone(),
            rights: new_rights,
        })?;
        Ok(Ok(new_token))
    }

    async fn meta(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<Result<capabilities::FacetMeta, capabilities::AccessError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        if !token.rights.contains(capabilities::FacetRights::READ) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let plugin = DaybookPlugin::from_ctx(self);
        let doc = match plugin.get_doc(&token.doc_id, &token.branch_path, &token.heads).await.map_err(wasmtime_err)? {
            Some(doc) => doc,
            None => return Ok(Err(capabilities::AccessError::NotFound)),
        };
        let dmeta = match doc.facets.get(&daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::Dmeta)) {
            Some(facet) => match daybook_types::doc::WellKnownFacet::from_json(facet.clone(), daybook_types::doc::WellKnownFacetTag::Dmeta) {
                Ok(daybook_types::doc::WellKnownFacet::Dmeta(dmeta)) => dmeta,
                _ => return Ok(Err(capabilities::AccessError::Other("invalid dmeta facet".into()))),
            },
            None => return Ok(Err(capabilities::AccessError::NotFound)),
        };
        let facet_meta = match dmeta.facets.get(&token.facet_key) {
            Some(meta) => meta,
            None => return Ok(Err(capabilities::AccessError::NotFound)),
        };
        Ok(Ok(capabilities::FacetMeta {
            created_at: capabilities::Datetime { seconds: facet_meta.created_at.as_second() as u64, nanoseconds: 0 },
        }))
    }

    async fn get(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<Result<String, capabilities::AccessError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        if !token.rights.contains(capabilities::FacetRights::READ) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let plugin = DaybookPlugin::from_ctx(self);
        let doc = match plugin.get_doc(&token.doc_id, &token.branch_path, &token.heads).await.map_err(wasmtime_err)? {
            Some(doc) => doc,
            None => return Ok(Err(capabilities::AccessError::NotFound)),
        };
        let facet = match doc.facets.get(&token.facet_key) {
            Some(facet) => facet,
            None => return Ok(Err(capabilities::AccessError::NotFound)),
        };
        Ok(Ok(wit_doc::facet_from(facet)))
    }

    async fn heads(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<Result<Vec<String>, capabilities::AccessError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        Ok(Ok(am_utils_rs::serialize_commit_heads(token.heads.as_ref())))
    }

    async fn update(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetToken>,
        facet_json: String,
    ) -> wasmtime::Result<Result<Result<(), capabilities::UpdateDocError>, capabilities::AccessError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        if !token.rights.contains(capabilities::FacetRights::UPDATE) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let facet: daybook_types::doc::FacetRaw = wit_doc::facet_into(&facet_json)
            .map_err(|err| capabilities::UpdateDocError::InvalidPatch(err.to_string()))?;
        let plugin = DaybookPlugin::from_ctx(self);
        match plugin.drawer_repo.update_at_heads(
            daybook_types::doc::DocPatch {
                id: token.doc_id.clone(),
                facets_set: HashMap::from([(token.facet_key.clone(), facet)]),
                facets_remove: default(),
                user_path: None,
            },
            token.branch_path.clone(),
            Some(token.heads.clone()),
        ).await {
            Ok(_) => Ok(Ok(Ok(()))),
            Err(crate::drawer::types::DrawerError::DocNotFound { .. }) => Ok(Ok(Err(capabilities::UpdateDocError::Other("doc not found".into())))),
            Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => Ok(Ok(Err(capabilities::UpdateDocError::Other("branch not found".into())))),
            Err(crate::drawer::types::DrawerError::InvalidKey { inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag } }) => Ok(Ok(Err(capabilities::UpdateDocError::InvalidKey(tag)))),
            Err(crate::drawer::types::DrawerError::Other { inner }) => Err(wasmtime_err(format!("unexpected error: {inner}"))),
            Err(crate::drawer::types::DrawerError::BranchAlreadyExists { .. }) => Err(wasmtime_err("unexpected branch already exists")),
        }
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

pub struct FacetCreateToken {
    pub doc_id: DocId,
    pub branch_path: daybook_types::doc::BranchPath,
    pub target_branch_path: daybook_types::doc::BranchPath,
    pub heads: ChangeHashSet,
    pub facet_key: daybook_types::doc::FacetKey,
    pub facet_acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
}

impl capabilities::HostFacetCreateToken for SharedWashCtx {
    async fn key(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetCreateToken>,
    ) -> wasmtime::Result<String> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet create token: {err}")))?;
        Ok(token.facet_key.to_string())
    }

    async fn create_facet(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetCreateToken>,
        data: String,
    ) -> wasmtime::Result<Result<wasmtime::component::Resource<capabilities::FacetToken>, capabilities::UpdateDocError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet create token: {err}")))?;
        let facet: daybook_types::doc::FacetRaw = wit_doc::facet_into(&data)
            .map_err(|err| capabilities::UpdateDocError::InvalidPatch(err.to_string()))?;
        let plugin = DaybookPlugin::from_ctx(self);
        if token.branch_path != token.target_branch_path {
            match plugin.drawer_repo.create_branch_at_heads_from_branch(
                &token.doc_id,
                &token.branch_path,
                &token.target_branch_path,
                &token.heads,
                None,
            ).await {
                Ok(()) | Err(crate::drawer::types::DrawerError::BranchAlreadyExists { .. }) => {}
                Err(crate::drawer::types::DrawerError::DocNotFound { .. }) => return Ok(Err(capabilities::UpdateDocError::Other("doc not found".into()))),
                Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => return Ok(Err(capabilities::UpdateDocError::Other("branch not found".into()))),
                Err(crate::drawer::types::DrawerError::Other { inner }) => return Err(wasmtime_err(format!("unexpected error ensuring branch: {inner}"))),
                Err(crate::drawer::types::DrawerError::InvalidKey { .. }) => return Err(wasmtime_err("unexpected invalid key")),
            }
        }
        match plugin.drawer_repo.update_at_heads(
            daybook_types::doc::DocPatch {
                id: token.doc_id.clone(),
                facets_set: HashMap::from([(token.facet_key.clone(), facet)]),
                facets_remove: default(),
                user_path: None,
            },
            token.branch_path.clone(),
            Some(token.heads.clone()),
        ).await {
            Ok(_) => {
                let ftoken = self.table.push(FacetToken {
                    doc_id: token.doc_id.clone(),
                    branch_path: token.branch_path.clone(),
                    heads: token.heads.clone(),
                    facet_key: token.facet_key.clone(),
                    rights: capabilities::FacetRights::READ | capabilities::FacetRights::UPDATE | capabilities::FacetRights::DELETE,
                })?;
                Ok(Ok(ftoken))
            }
            Err(crate::drawer::types::DrawerError::DocNotFound { .. }) => Ok(Err(capabilities::UpdateDocError::Other("doc not found".into()))),
            Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => Ok(Err(capabilities::UpdateDocError::Other("branch not found".into()))),
            Err(crate::drawer::types::DrawerError::InvalidKey { inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag } }) => Ok(Err(capabilities::UpdateDocError::InvalidKey(tag))),
            Err(crate::drawer::types::DrawerError::Other { inner }) => Err(wasmtime_err(format!("unexpected error: {inner}"))),
            Err(crate::drawer::types::DrawerError::BranchAlreadyExists { .. }) => Err(wasmtime_err("unexpected branch already exists")),
        }
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::FacetCreateToken>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

pub struct FacetTagToken {
    pub doc_id: DocId,
    pub branch_path: daybook_types::doc::BranchPath,
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
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?;
        Ok(token.tag.clone())
    }

    async fn rights(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTagToken>,
    ) -> wasmtime::Result<capabilities::FacetRights> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?;
        Ok(token.rights)
    }

    async fn list_facets(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTagToken>,
    ) -> wasmtime::Result<Result<Vec<wasmtime::component::Resource<capabilities::FacetToken>>, capabilities::AccessError>> {
        let (doc_id, branch_path, heads, _rights, tag, facet_acl) = {
            let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?;
            if !token.rights.contains(capabilities::FacetRights::READ) {
                return Ok(Err(capabilities::AccessError::Denied));
            }
            (token.doc_id.clone(), token.branch_path.clone(), token.heads.clone(), token.rights, token.tag.clone(), token.facet_acl.clone())
        };
        let plugin = DaybookPlugin::from_ctx(self);
        let doc = match plugin.get_doc(&doc_id, &branch_path, &heads).await.map_err(wasmtime_err)? {
            Some(doc) => doc,
            None => return Ok(Err(capabilities::AccessError::NotFound)),
        };
        let mut result = Vec::new();
        for (facet_key, _facet_value) in doc.facets.iter() {
            if facet_key.tag.to_string() != tag {
                continue;
            }
            let mut rights = capabilities::FacetRights::empty();
            for access in &facet_acl {
                if access.tag.0 != tag {
                    continue;
                }
                match &access.key_id {
                    Some(id) if id != &facet_key.id => continue,
                    Some(_) => {},
                    None => {},
                }
                rights |= facet_rights_from_access(access);
            }
            if rights == capabilities::FacetRights::empty() {
                continue;
            }
            let ftoken = self.table.push(FacetToken {
                doc_id: doc_id.clone(),
                branch_path: branch_path.clone(),
                heads: heads.clone(),
                facet_key: facet_key.clone(),
                rights,
            })?;
            result.push(ftoken);
        }
        Ok(Ok(result))
    }

    async fn get(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTagToken>,
        key_id: String,
    ) -> wasmtime::Result<Result<Option<wasmtime::component::Resource<capabilities::FacetToken>>, capabilities::AccessError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?;
        if !token.rights.contains(capabilities::FacetRights::READ) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let facet_key = daybook_types::doc::FacetKey {
            tag: daybook_types::doc::FacetTag::from(token.tag.as_str()),
            id: key_id,
        };
        let plugin = DaybookPlugin::from_ctx(self);
        let doc = match plugin.get_doc(&token.doc_id, &token.branch_path, &token.heads).await.map_err(wasmtime_err)? {
            Some(doc) => doc,
            None => return Ok(Err(capabilities::AccessError::NotFound)),
        };
        if !doc.facets.contains_key(&facet_key) {
            return Ok(Ok(None));
        }
        let mut rights = capabilities::FacetRights::empty();
        for access in &token.facet_acl {
            if access.tag.0 != token.tag {
                continue;
            }
            match &access.key_id {
                Some(id) if id != &facet_key.id => continue,
                Some(_) => {},
                None => {},
            }
            rights |= facet_rights_from_access(access);
        }
        if rights == capabilities::FacetRights::empty() {
            return Ok(Ok(None));
        }
        let ftoken = self.table.push(FacetToken {
            doc_id: token.doc_id.clone(),
            branch_path: token.branch_path.clone(),
            heads: token.heads.clone(),
            facet_key: facet_key.clone(),
            rights,
        })?;
        Ok(Ok(Some(ftoken)))
    }

    async fn create(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTagToken>,
        key_id: String,
        data: String,
    ) -> wasmtime::Result<Result<wasmtime::component::Resource<capabilities::FacetToken>, capabilities::UpdateDocError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?;
        if !token.rights.contains(capabilities::FacetRights::CREATE) {
            return Ok(Err(capabilities::UpdateDocError::Other("create denied".into())));
        }
        let facet_key = daybook_types::doc::FacetKey {
            tag: daybook_types::doc::FacetTag::from(token.tag.as_str()),
            id: key_id,
        };
        let facet: daybook_types::doc::FacetRaw = wit_doc::facet_into(&data)
            .map_err(|err| capabilities::UpdateDocError::InvalidPatch(err.to_string()))?;
        let plugin = DaybookPlugin::from_ctx(self);
        match plugin.drawer_repo.update_at_heads(
            daybook_types::doc::DocPatch {
                id: token.doc_id.clone(),
                facets_set: HashMap::from([(facet_key.clone(), facet)]),
                facets_remove: default(),
                user_path: None,
            },
            token.branch_path.clone(),
            Some(token.heads.clone()),
        ).await {
            Ok(_) => {
                let ftoken = self.table.push(FacetToken {
                    doc_id: token.doc_id.clone(),
                    branch_path: token.branch_path.clone(),
                    heads: token.heads.clone(),
                    facet_key,
                    rights: capabilities::FacetRights::READ | capabilities::FacetRights::UPDATE | capabilities::FacetRights::DELETE,
                })?;
                Ok(Ok(ftoken))
            }
            Err(crate::drawer::types::DrawerError::DocNotFound { .. }) => Ok(Err(capabilities::UpdateDocError::Other("doc not found".into()))),
            Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => Ok(Err(capabilities::UpdateDocError::Other("branch not found".into()))),
            Err(crate::drawer::types::DrawerError::InvalidKey { inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag } }) => Ok(Err(capabilities::UpdateDocError::InvalidKey(tag))),
            Err(crate::drawer::types::DrawerError::Other { inner }) => Err(wasmtime_err(format!("unexpected error: {inner}"))),
            Err(crate::drawer::types::DrawerError::BranchAlreadyExists { .. }) => Err(wasmtime_err("unexpected branch already exists")),
        }
    }

    async fn get_create_token(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTagToken>,
        key_id: String,
    ) -> wasmtime::Result<Result<wasmtime::component::Resource<capabilities::FacetCreateToken>, capabilities::UpdateDocError>> {
        let token = self.table.get(&handle).map_err(|err| wasmtime_err(format!("error locating facet tag token: {err}")))?;
        if !token.rights.contains(capabilities::FacetRights::CREATE) {
            return Ok(Err(capabilities::UpdateDocError::Other("create denied".into())));
        }
        let facet_key = daybook_types::doc::FacetKey {
            tag: daybook_types::doc::FacetTag::from(token.tag.as_str()),
            id: key_id,
        };
        let ctoken = self.table.push(FacetCreateToken {
            doc_id: token.doc_id.clone(),
            branch_path: token.branch_path.clone(),
            target_branch_path: token.branch_path.clone(),
            heads: token.heads.clone(),
            facet_key,
            facet_acl: token.facet_acl.clone(),
        })?;
        Ok(Ok(ctoken))
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::FacetTagToken>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

impl capabilities::Host for SharedWashCtx {
    async fn delete_facet(
        &mut self,
        token: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> wasmtime::Result<Result<(), capabilities::AccessError>> {
        let ft = self.table.get(&token).map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        if !ft.rights.contains(capabilities::FacetRights::DELETE) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        let plugin = DaybookPlugin::from_ctx(self);
        match plugin.drawer_repo.update_at_heads(
            daybook_types::doc::DocPatch {
                id: ft.doc_id.clone(),
                facets_set: default(),
                facets_remove: vec![ft.facet_key.clone()],
                user_path: None,
            },
            ft.branch_path.clone(),
            Some(ft.heads.clone()),
        ).await {
            Ok(_) => {
                let _ = self.table.delete(token);
                Ok(Ok(()))
            }
            Err(crate::drawer::types::DrawerError::DocNotFound { .. }) => Ok(Err(capabilities::AccessError::NotFound)),
            Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => Ok(Err(capabilities::AccessError::NotFound)),
            Err(crate::drawer::types::DrawerError::Other { inner }) => Err(wasmtime_err(format!("unexpected error: {inner}"))),
            Err(crate::drawer::types::DrawerError::InvalidKey { .. }) => Err(wasmtime_err("unexpected invalid key")),
            Err(crate::drawer::types::DrawerError::BranchAlreadyExists { .. }) => Err(wasmtime_err("unexpected branch already exists")),
        }
    }

    async fn delete_doc(
        &mut self,
        token: wasmtime::component::Resource<capabilities::DocToken>,
    ) -> wasmtime::Result<Result<(), capabilities::AccessError>> {
        let dt = self.table.get(&token).map_err(|err| wasmtime_err(format!("error locating doc token: {err}")))?;
        if !dt.rights.contains(capabilities::DocRights::DELETE) {
            return Ok(Err(capabilities::AccessError::Denied));
        }
        // FIXME: implement doc deletion
        Ok(Err(capabilities::AccessError::Other("doc deletion not yet implemented".into())))
    }
}

pub struct CommandInvokeToken {
    pub parent_wflow_job_id: Arc<str>,
    pub target_url: String,
}

impl capabilities::HostCommandInvokeToken for SharedWashCtx {
    async fn invoke(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::CommandInvokeToken>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, capabilities::InvokeCommandError>> {
        let plugin = DaybookPlugin::from_ctx(self);
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime_err(format!("error locating command invoke token: {err}")))?;
        let request: InvokeCommandRequest = match serde_json::from_str(&request_json) {
            Ok(value) => value,
            Err(err) => {
                return Ok(Err(capabilities::InvokeCommandError::BadRequest(
                    err.to_string(),
                )))
            }
        };

        let rt = match plugin.rt() {
            Ok(rt) => rt,
            Err(err) => {
                return Ok(Err(capabilities::InvokeCommandError::Other(
                    err.to_string(),
                )))
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

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::CommandInvokeToken>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

pub(super) async fn get_facet_raw_from_token(
    ctx: &mut SharedWashCtx,
    handle: &wasmtime::component::Resource<capabilities::FacetToken>,
) -> wasmtime::Result<Result<(daybook_types::doc::FacetKey, daybook_types::doc::FacetRaw), String>> {
    let plugin = DaybookPlugin::from_ctx(ctx);
    let (doc_id, branch_path, heads, facet_key) = {
        let token = ctx
            .table
            .get(handle)
            .map_err(|err| wasmtime_err(format!("error locating facet token: {err}")))?;
        (
            token.doc_id.clone(),
            token.branch_path.clone(),
            token.heads.clone(),
            token.facet_key.clone(),
        )
    };

    let Some(doc) = plugin
        .get_doc(&doc_id, &branch_path, &heads)
        .await
        .map_err(wasmtime_err)?
    else {
        return Ok(Err(format!("doc not found: {doc_id}")));
    };
    let Some(facet_raw) = doc.facets.get(&facet_key) else {
        return Ok(Err(format!("facet not found: {}", facet_key)));
    };
    Ok(Ok((facet_key, facet_raw.clone())))
}

pub(super) async fn get_blob_facet_from_token(
    ctx: &mut SharedWashCtx,
    handle: &wasmtime::component::Resource<capabilities::FacetToken>,
) -> wasmtime::Result<Result<daybook_types::doc::Blob, String>> {
    let (_facet_key, facet_raw) = match get_facet_raw_from_token(ctx, handle).await? {
        Ok(value) => value,
        Err(err) => return Ok(Err(err)),
    };
    let blob_facet_value = match daybook_types::doc::WellKnownFacet::from_json(
        facet_raw,
        daybook_types::doc::WellKnownFacetTag::Blob,
    ) {
        Ok(value) => value,
        Err(err) => return Ok(Err(err.to_string())),
    };
    let daybook_types::doc::WellKnownFacet::Blob(blob) = blob_facet_value else {
        return Ok(Err("facet is not a blob".to_string()));
    };
    Ok(Ok(blob))
}

pub(super) async fn resolve_blob_path_from_blob_facet(
    plugin: &DaybookPlugin,
    blob: &daybook_types::doc::Blob,
) -> Result<std::path::PathBuf, String> {
    let hash = blob_hash_from_blob_facet(blob)?;
    plugin
        .blobs_repo
        .get_path(&hash)
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
