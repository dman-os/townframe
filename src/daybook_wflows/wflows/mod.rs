use crate::interlude::*;

pub mod embed_image;
pub mod embed_text;
pub mod index_embedding;
pub mod ocr_image;
pub mod test_labeler;

pub(crate) enum FacetWriteTarget<'a> {
    Update(&'a crate::wit::townframe::daybook::capabilities::FacetToken),
    Create(
        &'a crate::wit::townframe::daybook::capabilities::FacetTagToken,
        String,
    ),
}

impl FacetWriteTarget<'_> {
    pub(crate) fn write(
        &self,
        data: &str,
        update_context: &str,
        create_context: &str,
    ) -> Result<(), wflow_sdk::JobErrorX> {
        match self {
            Self::Update(token) => token
                .update(data)
                .map_err(|err| wflow_sdk::JobErrorX::Terminal(ferr!("{update_context}: {err:?}")))?
                .map_err(|err| {
                    wflow_sdk::JobErrorX::Terminal(ferr!("{update_context}: {err:?}"))
                })?,
            Self::Create(token, key_id) => {
                token.create(key_id, data).map_err(|err| {
                    wflow_sdk::JobErrorX::Terminal(ferr!("{create_context}: {err:?}"))
                })?;
            }
        }
        Ok(())
    }
}

pub(crate) fn resolve_facet_write_target<'a>(
    facets: &'a [crate::wit::townframe::daybook::capabilities::FacetToken],
    tags: &'a [crate::wit::townframe::daybook::capabilities::FacetTagToken],
    facet_key: &str,
    facet_tag: &str,
    context: &str,
) -> Result<FacetWriteTarget<'a>, wflow_sdk::JobErrorX> {
    use crate::wit::townframe::daybook::capabilities::FacetRights;

    if let Some(token) = facets
        .iter()
        .find(|token| token.key() == facet_key && token.rights().contains(FacetRights::UPDATE))
    {
        return Ok(FacetWriteTarget::Update(token));
    }

    if let Some(token) = tags
        .iter()
        .find(|token| token.tag() == facet_tag && token.rights().contains(FacetRights::CREATE))
    {
        return Ok(FacetWriteTarget::Create(token, facet_key.to_string()));
    }

    Err(wflow_sdk::JobErrorX::Terminal(ferr!(
        "{context} facet token with update/create rights not found"
    )))
}
