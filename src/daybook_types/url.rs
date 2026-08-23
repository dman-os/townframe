use crate::interlude::*;

use crate::doc::{DocId, FacetKey};

pub const FACET_SCHEME: &str = "db+facet";
pub const FACET_SELF_DOC_ID: &str = "self";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetRef {
    pub doc_id: DocId,
    pub facet_key: FacetKey,
    /// Optional branch pin (ADR 007 §3), e.g. `?branch=main`.
    pub branch: Option<String>,
    /// Optional head pins (ADR 007 §3), e.g. `?at=<head1>|<head2>` (pipe-separated).
    pub at: Option<Vec<String>>,
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

    // ADR 007 §3: pins ride in the query string; unknown params stay ignored.
    let mut branch = None;
    let mut at = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "branch" => branch = Some(value.into_owned()),
            "at" => {
                let heads = value
                    .split('|')
                    .filter(|segment| !segment.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                at = Some(heads);
            }
            _ => {}
        }
    }

    Ok(FacetRef {
        doc_id: doc_id.to_string(),
        facet_key: FacetKey::from(format!("{tag}/{id}")),
        branch,
        at,
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

    #[test]
    fn parses_facet_ref_branch_and_at_query_params() {
        let url = "db+facet:///abc123/org.example.daybook.plugManifest/main?branch=main&at=h1|h2";
        let parsed = parse_facet_ref_str(url).unwrap();
        assert_eq!(parsed.doc_id, "abc123");
        assert_eq!(parsed.facet_key.id, "main");
        assert_eq!(
            parsed.facet_key.tag.to_string(),
            "org.example.daybook.plugManifest"
        );
        assert_eq!(parsed.branch.as_deref(), Some("main"));
        assert_eq!(parsed.at, Some(vec!["h1".to_string(), "h2".to_string()]));
    }

    #[test]
    fn parses_facet_ref_without_query_params_has_no_pin() {
        let parsed = parse_facet_ref_str("db+facet:///self/org.example.daybook.note/main").unwrap();
        assert_eq!(parsed.branch, None);
        assert_eq!(parsed.at, None);
    }

    #[test]
    fn parses_facet_ref_with_unknown_query_params_ignores_them() {
        let parsed =
            parse_facet_ref_str("db+facet:///self/org.example.daybook.note/main?foo=bar").unwrap();
        assert_eq!(parsed.branch, None);
        assert_eq!(parsed.at, None);
    }

    #[test]
    fn plug_facet_tags_are_camelcase() {
        // ADR 007 §1: facet names follow the camelCase plug-name convention.
        assert_eq!(
            WellKnownFacetTag::PlugManifest.as_str(),
            "org.example.daybook.plugManifest"
        );
        assert_eq!(
            WellKnownFacetTag::PlugsConfig.as_str(),
            "org.example.daybook.plugsConfig"
        );
    }
}
