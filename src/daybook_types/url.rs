use crate::interlude::*;

use crate::doc::DocId;
use crate::doc::FacetKey;

/// The prefix of a datum reference URL: `dtxt:datum:<doc>/<tag>/<id>` (ADR 035).
///
/// This crate keeps its own parser rather than `big_cabinet_core::DatumReference`: a facet
/// reference here also carries the `branch` and `at` pins of ADR 007 §3, which ADR 035 replaces
/// with `?heads=` for the Cabinet. They go away with this crate's migration.
pub const FACET_URL_PREFIX: &str = "dtxt:datum:";
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
        "{FACET_URL_PREFIX}{doc_id}/{tag}/{id}",
        tag = facet_key.tag,
        id = facet_key.id
    );
    Ok(url.parse()?)
}

pub fn parse_facet_ref(url: &Url) -> Res<FacetRef> {
    // The path after `dtxt:datum:` is opaque, so the `url` crate hands it back verbatim.
    let path = (url.scheme() == "dtxt")
        .then(|| url.path().strip_prefix("datum:"))
        .flatten()
        .ok_or_else(|| eyre::eyre!("unsupported facet url '{url}', expected `dtxt:datum:`"))?;

    let mut parts = path.split('/');
    let doc_id = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_eyre("facet url missing doc id")?;
    let tag = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_eyre("facet url missing facet tag")?;
    let id = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_eyre("facet url missing facet id")?;
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
    use crate::doc::FacetTag;
    use crate::doc::WellKnownFacetTag;

    #[test]
    fn parses_facet_ref_str() {
        let parsed = parse_facet_ref_str("dtxt:datum:self/org.example.daybook.blob/main").unwrap();
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
            "dtxt:datum:self/org.example.daybook.blob/main",
            &FacetTag::WellKnown(WellKnownFacetTag::Blob),
        )
        .unwrap();
        assert!(is_blob);
    }

    #[test]
    fn facet_ref_targets_tag_mismatch() {
        let is_note = facet_ref_str_targets_tag(
            "dtxt:datum:self/org.example.daybook.blob/main",
            &FacetTag::WellKnown(WellKnownFacetTag::Note),
        )
        .unwrap();
        assert!(!is_note);
    }

    #[test]
    fn parses_facet_ref_branch_and_at_query_params() {
        let url = "dtxt:datum:abc123/org.example.daybook.plugManifest/main?branch=main&at=h1|h2";
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
        let parsed = parse_facet_ref_str("dtxt:datum:self/org.example.daybook.note/main").unwrap();
        assert_eq!(parsed.branch, None);
        assert_eq!(parsed.at, None);
    }

    #[test]
    fn parses_facet_ref_with_unknown_query_params_ignores_them() {
        let parsed =
            parse_facet_ref_str("dtxt:datum:self/org.example.daybook.note/main?foo=bar").unwrap();
        assert_eq!(parsed.branch, None);
        assert_eq!(parsed.at, None);
    }

    #[test]
    fn refuses_other_schemes_kinds_and_shapes() {
        for url in [
            "db+facet:///self/org.example.daybook.note/main",
            "dtxt:blob:zContent",
            "dtxt:datum:self/org.example.daybook.note",
            "dtxt:datum:self//main",
            "dtxt:datum://self/org.example.daybook.note/main",
            "dtxt:datum:self/org.example.daybook.note/main/extra",
        ] {
            assert!(parse_facet_ref_str(url).is_err(), "{url}");
        }
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
