use crate::interlude::*;

use crate::doc::{DocId, FacetKey};

pub const FACET_SCHEME: &str = "db+facet";
pub const FACET_SELF_DOC_ID: &str = "self";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetRef {
    pub doc_id: DocId,
    pub facet_key: FacetKey,
}

pub fn build_facet_ref(doc_id: &str, facet_key: &FacetKey) -> Res<Url> {
    if doc_id.contains('/') {
        eyre::bail!("facet-ref doc id cannot contain '/'");
    }
    if facet_key.id.contains('/') {
        eyre::bail!("facet-ref facet id cannot contain '/'");
    }
    let url = format!(
        "{FACET_SCHEME}:///{doc_id}/{tag}/{id}",
        tag = facet_key.tag,
        id = facet_key.id
    );
    Ok(url.parse()?)
}

pub fn parse_facet_ref(url: &Url) -> Res<FacetRef> {
    if url.scheme() != FACET_SCHEME {
        eyre::bail!("unsupported facet url scheme '{}'", url.scheme());
    }
    if url.host_str().is_some() {
        eyre::bail!("facet url authority must be empty");
    }

    let mut parts = url
        .path_segments()
        .ok_or_eyre("facet url path is malformed")?
        .filter(|segment| !segment.is_empty());

    let doc_id = parts.next().ok_or_eyre("facet url missing doc id")?;
    let tag = parts.next().ok_or_eyre("facet url missing facet tag")?;
    let id = parts.next().ok_or_eyre("facet url missing facet id")?;
    if parts.next().is_some() {
        eyre::bail!("facet url has unexpected extra path segments");
    }

    Ok(FacetRef {
        doc_id: doc_id.to_string(),
        facet_key: FacetKey::from(format!("{tag}/{id}")),
    })
}

pub fn parse_facet_ref_str(url: &str) -> Res<FacetRef> {
    parse_facet_ref(&Url::parse(url)?)
}

pub fn facet_ref_targets_tag(url: &Url, target_tag: &crate::doc::FacetTag) -> Res<bool> {
    Ok(parse_facet_ref(url)?.facet_key.tag == *target_tag)
}

pub fn facet_ref_str_targets_tag(url: &str, target_tag: &crate::doc::FacetTag) -> Res<bool> {
    Ok(parse_facet_ref_str(url)?.facet_key.tag == *target_tag)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doc::{FacetTag, WellKnownFacetTag};

    #[test]
    fn parses_facet_ref_str() {
        let parsed = parse_facet_ref_str("db+facet:///self/org.example.daybook.blob/main").unwrap();
        assert_eq!(parsed.doc_id, FACET_SELF_DOC_ID);
        assert_eq!(
            parsed.facet_key.tag,
            FacetTag::WellKnown(WellKnownFacetTag::Blob)
        );
        assert_eq!(parsed.facet_key.id, "main");
    }

    #[test]
    fn facet_ref_targets_tag_matches() {
        let is_blob = facet_ref_str_targets_tag(
            "db+facet:///self/org.example.daybook.blob/main",
            &FacetTag::WellKnown(WellKnownFacetTag::Blob),
        )
        .unwrap();
        assert!(is_blob);
    }

    #[test]
    fn facet_ref_targets_tag_mismatch() {
        let is_note = facet_ref_str_targets_tag(
            "db+facet:///self/org.example.daybook.blob/main",
            &FacetTag::WellKnown(WellKnownFacetTag::Note),
        )
        .unwrap();
        assert!(!is_note);
    }
}
