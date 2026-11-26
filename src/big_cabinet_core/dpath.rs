//! The dpath claim: `org.example.daybook.dpath/<dpath>` on a document says "this document claims
//! this path" (FDR 001), so a file imported from disk keeps its path and a grant can later name
//! "documents under /projects".
//!
//! The slot is the dpath's bytes, and a slot that is not a valid [`Dpath`] is refused when the
//! datum is written. One slot per exact dpath makes one claim per exact dpath per document;
//! several dpath datums on one document are independent. Nothing is unique across documents.

use crate::DatumPin;
use datum_core::Dpath;
use datum_core::InvalidDpath;
use serde::Deserialize;
use serde::Serialize;

/// The value of one dpath claim. It is open like every datum object: a member a newer writer adds
/// survives a round trip through this build.
#[macros::datum(authority = "org.example.daybook", name = "dpath")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DpathClaim {
    /// The datums of the claiming document that carry the path. Absent means the document's
    /// primary content, whichever datum that is; present names them, each at the heads its pin
    /// carries or at the current frontier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub targets: Option<Vec<DatumPin>>,
    #[serde(flatten)]
    pub extra: crate::datum::Extra,
}

/// Why a dpath claim's slot is not a dpath.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DpathSlotError {
    #[error("a dpath claim's slot is UTF-8: a dpath is a validated UTF-8 label")]
    NotUtf8,
    #[error("a dpath claim's slot is not a dpath: {0}")]
    Invalid(#[from] InvalidDpath),
}

impl DpathClaim {
    /// A claim on the document's primary content.
    pub fn primary() -> Self {
        Self {
            targets: None,
            extra: crate::datum::Extra::new(),
        }
    }

    /// The dpath a claim's slot spells.
    pub fn slot_dpath(slot: &[u8]) -> Result<Dpath, DpathSlotError> {
        let text = std::str::from_utf8(slot).map_err(|_| DpathSlotError::NotUtf8)?;
        Ok(Dpath::parse(text)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DatumKey;
    use crate::DatumReference;
    use crate::Location;
    use crate::ReferenceDocId;
    use crate::datum::DatumMember;
    use serde_json::json;

    #[test]
    fn the_slot_is_the_dpath() {
        let key = DatumKey::from_text(DpathClaim::ID, "/inbox/hello.md").unwrap();
        assert_eq!(
            DpathClaim::slot_dpath(key.slot_bytes()).unwrap(),
            Dpath::literal("/inbox/hello.md")
        );
        assert_eq!(key.to_string(), "org.example.daybook.dpath//inbox/hello.md");
        let reparsed: DatumKey = key.to_string().parse().unwrap();
        assert_eq!(reparsed, key);

        assert_eq!(
            DpathClaim::slot_dpath(b"/caf\xe9"),
            Err(DpathSlotError::NotUtf8)
        );
        for bad in ["/", "a/b", "/a//b", "/a/", "/a/../b"] {
            assert!(
                matches!(
                    DpathClaim::slot_dpath(bad.as_bytes()),
                    Err(DpathSlotError::Invalid(_))
                ),
                "{bad:?} is not a dpath slot"
            );
        }
    }

    #[test]
    fn targets_are_optional_and_unknown_members_survive() {
        let primary = serde_json::to_value(DpathClaim::primary()).unwrap();
        assert_eq!(primary, json!({}), "absent targets is the primary content");

        let pin = DatumPin::new(
            Location::new(
                DatumReference::new(
                    ReferenceDocId::SelfDoc,
                    DatumKey::from_text("org.example.note", "main").unwrap(),
                ),
                None,
            )
            .unwrap(),
        );
        let claim = DpathClaim {
            targets: Some(vec![pin]),
            extra: crate::datum::Extra::new(),
        };
        let wire = serde_json::to_value(&claim).unwrap();
        assert!(wire["targets"][0]["ref"].is_string(), "{wire}");
        assert_eq!(serde_json::from_value::<DpathClaim>(wire).unwrap(), claim);

        let newer = json!({
            "targets": [{"ref": "dtxt:datum:self/org.example.note/main", "future": 1}],
            "future": {"member": true}
        });
        let decoded: DpathClaim = serde_json::from_value(newer.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), newer);
    }

    #[cfg(feature = "schemars")]
    mod declaration {
        use super::*;
        use crate::datum::Bundle;
        use crate::datum::Declaration;
        use crate::datum::Universe;

        fn universe() -> Universe {
            Universe::collect([Declaration::of::<DpathClaim>().unwrap()])
        }

        #[test]
        fn declares_publishes_and_admits() {
            assert_eq!(DpathClaim::ID, "org.example.daybook.dpath");
            let universe = universe();
            let bundle = Bundle::new(&universe, Vec::new());
            let lexicons = bundle.lexicons().unwrap();
            assert_eq!(lexicons.len(), 1);
            let lexicon = &lexicons[0];
            assert_eq!(lexicon.id, DpathClaim::ID);
            let properties = &lexicon.file["defs"]["main"]["properties"];
            assert_eq!(properties["targets"]["type"], "array", "{}", lexicon.file);
            assert_eq!(
                lexicon.file["defs"]["main"].get("required"),
                None,
                "targets is optional: {}",
                lexicon.file
            );
            let libraries: Vec<&serde_json::Value> =
                crate::reference::node_type_libraries().iter().collect();
            bundle
                .validate_lexicon(&lexicon.id, &lexicon.file, &libraries)
                .expect("the published dpath lexicon admits");
        }

        #[test]
        fn codec_round_trips_known_and_unknown_members() {
            let universe = universe();
            let bundle = Bundle::new(&universe, Vec::new());
            let lexicon = bundle.lexicons().unwrap().pop().unwrap();
            let validator = crate::datum::compile_position(
                &lexicon.file,
                &[],
                &lexicon.file["defs"]["main"],
                "$.defs.main",
            )
            .unwrap();
            for value in [
                json!({}),
                json!({"targets": []}),
                json!({"targets": [{"ref": "dtxt:datum:self/org.example.note/main"}]}),
                json!({"future": true}),
            ] {
                assert!(validator.is_valid(&value), "{value}");
                let decoded: DpathClaim = serde_json::from_value(value.clone()).unwrap();
                assert_eq!(serde_json::to_value(decoded).unwrap(), value);
            }
            assert!(!validator.is_valid(&json!({"targets": "main"})));
            assert!(!validator.is_valid(&json!({"targets": [7]})));
        }
    }
}
