use crate::interlude::*;

pub mod label_engine;
pub mod label_image;
pub mod label_note;
pub mod learn_image_label_candidates;
pub mod learn_note_label_candidates;

pub(crate) enum FacetWriteTarget<'a> {
    Update(&'a crate::wit::townframe::daybook::capabilities::FacetToken),
    Create(
        &'a crate::wit::townframe::daybook::capabilities::FacetTagToken,
        String,
    ),
}

impl FacetWriteTarget<'_> {
    pub(crate) async fn write(
        &self,
        data: &str,
        update_context: &str,
        create_context: &str,
    ) -> Result<(), wflow_sdk::JobErrorX> {
        match self {
            Self::Update(token) => token
                .update(data.to_string())
                .await
                .map_err(|err| wflow_sdk::JobErrorX::Terminal(ferr!("{update_context}: {err:?}")))?
                .map_err(|err| {
                    wflow_sdk::JobErrorX::Terminal(ferr!("{update_context}: {err:?}"))
                })?,
            Self::Create(token, key_id) => {
                token
                    .create(key_id.to_string(), data.to_string())
                    .await
                    .map_err(|err| {
                        wflow_sdk::JobErrorX::Terminal(ferr!("{create_context}: {err:?}"))
                    })?;
            }
        }
        Ok(())
    }
}

pub(crate) async fn find_facet_token<'a>(
    facets: &'a [crate::wit::townframe::daybook::capabilities::FacetToken],
    facet_key: &str,
    rights: crate::wit::townframe::daybook::capabilities::FacetRights,
) -> Option<&'a crate::wit::townframe::daybook::capabilities::FacetToken> {
    for token in facets {
        if token.key() == facet_key && token.rights().contains(rights) {
            return Some(token);
        }
    }
    None
}

pub(crate) async fn find_tag_token<'a>(
    tags: &'a [crate::wit::townframe::daybook::capabilities::FacetTagToken],
    facet_tag: &str,
    rights: crate::wit::townframe::daybook::capabilities::FacetRights,
) -> Option<&'a crate::wit::townframe::daybook::capabilities::FacetTagToken> {
    for token in tags {
        if token.tag() == facet_tag && token.rights().contains(rights) {
            return Some(token);
        }
    }
    None
}

pub(crate) async fn resolve_facet_write_target<'a>(
    facets: &'a [crate::wit::townframe::daybook::capabilities::FacetToken],
    tags: &'a [crate::wit::townframe::daybook::capabilities::FacetTagToken],
    facet_key: &str,
    facet_tag: &str,
    context: &str,
) -> Result<FacetWriteTarget<'a>, wflow_sdk::JobErrorX> {
    use crate::wit::townframe::daybook::capabilities::FacetRights;

    if let Some(token) = find_facet_token(facets, facet_key, FacetRights::UPDATE).await {
        return Ok(FacetWriteTarget::Update(token));
    }

    if let Some(token) = find_tag_token(tags, facet_tag, FacetRights::CREATE).await {
        return Ok(FacetWriteTarget::Create(
            token,
            plug_plabels_manifest::facet_key_id(facet_key),
        ));
    }

    Err(wflow_sdk::JobErrorX::Terminal(ferr!(
        "{context} facet token with update/create rights not found"
    )))
}
