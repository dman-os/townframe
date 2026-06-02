use crate::interlude::*;
use crate::types::Claim;
use wflow_sdk::WflowCtx;

pub fn run(_cx: &mut WflowCtx) -> Result<(), wflow_sdk::JobErrorX> {
    use crate::wit::townframe::daybook::capabilities::FacetRights;
    use crate::wit::townframe::daybook::facet_routine;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let args = facet_routine::get_args();

    let note_facet_key_str =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::Note).to_string();
    let note_token = args
        .primary_doc
        .facets
        .iter()
        .find(|tag| tag.key() == note_facet_key_str && tag.rights().contains(FacetRights::READ))
        .ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("note facet token with read rights not found"))
        })?;

    let note_raw = note_token.get().map_err(|err| {
        wflow_sdk::JobErrorX::Terminal(ferr!("error reading note facet: {err:?}"))
    })?;
    let note_json: daybook_types::doc::FacetRaw =
        serde_json::from_str(&note_raw).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("error parsing note facet json: {err}"))
        })?;
    let note = match WellKnownFacet::from_json(note_json, WellKnownFacetTag::Note)
        .map_err(|err| wflow_sdk::JobErrorX::Terminal(err.wrap_err("input facet is not note")))?
    {
        WellKnownFacet::Note(value) => value,
        _ => unreachable!("expected WellKnownFacet::Note here for parse-hledger"),
    };

    let transactions = crate::hledger::parse::journal::parse_journal(&note.content)
        .map_err(|err| wflow_sdk::JobErrorX::Terminal(ferr!("hledger parse error: {err:?}")))?;

    let note_heads = note_token.heads().map_err(|err| {
        wflow_sdk::JobErrorX::Terminal(ferr!("error reading note heads: {err:?}"))
    })?;
    let note_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Note);
    let note_url_str = format!(
        "db+facet:///{}/{}/{}",
        args.doc_id, note_facet_key.tag, note_facet_key.id
    );
    let note_url: url::Url = note_url_str
        .parse()
        .map_err(|err| wflow_sdk::JobErrorX::Terminal(ferr!("invalid facet url: {err}")))?;
    let src_ref = daybook_types::doc::FacetRef {
        r#ref: note_url,
        heads: note_heads,
    };

    let claim_tag_str = crate::types::DayledgerFacetTag::Claim.as_str();
    let claim_tag = daybook_types::doc::FacetKey::from(claim_tag_str).tag;

    let claim_tag_token = args
        .primary_doc
        .tags
        .iter()
        .find(|tag| tag.tag() == claim_tag_str && tag.rights().contains(FacetRights::CREATE))
        .ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("claim tag token with create rights not found"))
        })?;

    let mut existing_claims: HashMap<String, Claim> = HashMap::new();
    for claim_token in args.primary_doc.facets.iter() {
        let facet_key = daybook_types::doc::FacetKey::from(claim_token.key().as_str());
        if facet_key.tag != claim_tag || !claim_token.rights().contains(FacetRights::READ) {
            continue;
        }

        let claim_raw = claim_token.get().map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("error reading claim facet: {err:?}"))
        })?;
        let claim = serde_json::from_str(&claim_raw).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("error parsing claim facet json: {err}"))
        })?;
        let claim_id = facet_key.id;
        existing_claims.insert(claim_id, claim);
    }

    for (claim_id, claim) in
        crate::hledger::claim_matcher::match_claims(&transactions, &existing_claims, &src_ref)
    {
        let claim_json = serde_json::to_string(&claim).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("serde error serializing claim: {err}"))
        })?;

        claim_tag_token
            .create(&claim_id, &claim_json)
            .map_err(|err| {
                wflow_sdk::JobErrorX::Terminal(ferr!(
                    "error creating/updating claim facet: {err:?}"
                ))
            })?;
    }

    Ok(())
}
