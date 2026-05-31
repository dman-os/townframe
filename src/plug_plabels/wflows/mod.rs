use crate::interlude::*;

pub mod label_engine;
pub mod label_image;
pub mod label_note;
pub mod learn_algo;
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

fn facet_key_id(facet_key: &str) -> String {
    daybook_types::doc::FacetKey::from(facet_key).id
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
        return Ok(FacetWriteTarget::Create(token, facet_key_id(facet_key)));
    }

    Err(wflow_sdk::JobErrorX::Terminal(ferr!(
        "{context} facet token with update/create rights not found"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn facet_key_id_uses_the_key_suffix() {
        assert_eq!(facet_key_id("org.example.note/custom"), "custom");
        assert_eq!(facet_key_id("org.example.note"), "main");
    }
}
