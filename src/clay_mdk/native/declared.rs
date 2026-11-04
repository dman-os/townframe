//! A manifest's declared contributions as installable rows (ADR 025 §3, §4, §7). Installation
//! writes them into the package's manifest installation scope of each target collection, so
//! they are visible while the package is stopped; the package's runtime writes use another
//! scope key, so reinstalling the manifest never drops them.
use std::collections::BTreeMap;

use ciborium::Value;
use clay_guest_session::capabilities::PublicationEndpoint;
use clay_guest_session::capabilities::RowAttachment;
use clay_guest_session::collections::CAPABILITY_ATTACHMENT_TAG;
use clay_guest_session::collections::CollectionValue;
use clay_host::MicroId;

use crate::manifest::MicroManifest;

/// The installation scope that holds `micro`'s manifest-declared rows.
pub fn manifest_scope(micro: &MicroId) -> Vec<u8> {
    format!("{micro}/manifest").into_bytes()
}

/// The installation scope for `micro`'s own durable runtime writes, kept apart from
/// [`manifest_scope`] so a manifest reinstall replaces only the declared rows.
pub fn runtime_scope(micro: &MicroId) -> Vec<u8> {
    format!("{micro}/runtime").into_bytes()
}

/// Rows keyed by collection id, then by encoded key, ready for
/// `RegistrationCollection::install` into [`manifest_scope`].
pub type DeclaredRows = BTreeMap<String, BTreeMap<Vec<u8>, Option<CollectionValue>>>;

/// The rows `manifest` declares. A key is the CBOR text of the declared key. The payload is
/// the row body as JSON; the one-member object `{"$attachment": n}` stands for the reserved
/// attachment tag over index `n` (JSON has no tags). Each declared attachment becomes the
/// endpoint of one of the package's own publications, so the stored row never retains the
/// provider (ADR 025 §7).
pub fn declared_rows(manifest: &MicroManifest) -> Result<DeclaredRows, String> {
    manifest.validate()?;
    let micro = MicroId::new(manifest.id.as_str());
    let mut collections = DeclaredRows::new();
    for contribution in &manifest.contributions {
        let mut attachments = vec![None; contribution.attachments.len()];
        for attachment in &contribution.attachments {
            let publication = manifest
                .publications
                .iter()
                .find(|publication| publication.name == attachment.publication)
                .expect("validate checked that the attachment names a declared publication");
            let Some((_, contract)) = super::admitted_publication(publication) else {
                return Err(format!(
                    "contribution {:?} attaches {:?}, which is not a servable publication",
                    contribution.id, publication.name
                ));
            };
            let slot = usize::try_from(attachment.index)
                .ok()
                .and_then(|index| attachments.get_mut(index))
                .ok_or_else(|| {
                    format!(
                        "contribution {:?} attaches index {} but declares {} attachments",
                        contribution.id,
                        attachment.index,
                        contribution.attachments.len()
                    )
                })?;
            if slot
                .replace(RowAttachment::Endpoint(PublicationEndpoint {
                    micro: micro.clone(),
                    publication: attachment.publication.clone(),
                    contract,
                }))
                .is_some()
            {
                return Err(format!(
                    "contribution {:?} attaches index {} twice",
                    contribution.id, attachment.index
                ));
            }
        }
        let attachments: Vec<RowAttachment> = attachments
            .into_iter()
            .collect::<Option<_>>()
            .ok_or_else(|| {
                format!(
                    "contribution {:?} leaves an attachment index undeclared",
                    contribution.id
                )
            })?;
        let mut body = Vec::new();
        ciborium::into_writer(&cbor(&contribution.payload), &mut body)
            .expect("CBOR encoding of an in-memory value");
        let mut key = Vec::new();
        ciborium::into_writer(&contribution.key, &mut key)
            .expect("CBOR encoding of an in-memory value");
        let rows = collections
            .entry(contribution.collection.clone())
            .or_default();
        if rows
            .insert(key, Some(CollectionValue { body, attachments }))
            .is_some()
        {
            return Err(format!(
                "contribution {:?} duplicates key {:?} of {}",
                contribution.id, contribution.key, contribution.collection
            ));
        }
    }
    Ok(collections)
}

/// A JSON payload as CBOR, with `{"$attachment": n}` as the attachment tag.
fn cbor(json: &serde_json::Value) -> Value {
    use serde_json::Value as Json;
    match json {
        Json::Null => Value::Null,
        Json::Bool(flag) => Value::Bool(*flag),
        Json::Number(number) => match (number.as_u64(), number.as_i64()) {
            (Some(unsigned), _) => Value::Integer(unsigned.into()),
            (_, Some(signed)) => Value::Integer(signed.into()),
            _ => Value::Float(number.as_f64().expect("a JSON number is a finite float")),
        },
        Json::String(text) => Value::Text(text.clone()),
        Json::Array(items) => Value::Array(items.iter().map(cbor).collect()),
        Json::Object(members) => match members.get("$attachment").and_then(Json::as_u64) {
            Some(index) if members.len() == 1 => Value::Tag(
                CAPABILITY_ATTACHMENT_TAG,
                Box::new(Value::Integer(index.into())),
            ),
            _ => Value::Map(
                members
                    .iter()
                    .map(|(name, member)| (Value::Text(name.clone()), cbor(member)))
                    .collect(),
            ),
        },
    }
}
