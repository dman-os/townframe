use crate::{interlude::*, wit};

use crate::doc::{Blob, Doc, FacetKey, FacetRaw, ImageMetadata, WellKnownFacet, WellKnownFacetTag};
use std::collections::HashMap;

fn create_test_doc() -> Doc {
    let mut props = HashMap::new();

    props.insert(
        FacetKey::from(WellKnownFacetTag::RefGeneric),
        FacetRaw::from(WellKnownFacet::RefGeneric("ref-123".to_string())),
    );
    props.insert(
        FacetKey::from(WellKnownFacetTag::LabelGeneric),
        FacetRaw::from(WellKnownFacet::LabelGeneric("label-1".to_string())),
    );
    props.insert(
        FacetKey::from(WellKnownFacetTag::PseudoLabel),
        FacetRaw::from(WellKnownFacet::PseudoLabel(
            vec!["pseudo-label".to_string()],
        )),
    );
    props.insert(
        FacetKey::from(WellKnownFacetTag::Note),
        FacetRaw::from(WellKnownFacet::Note("Test note".into())),
    );

    Doc {
        id: "test-doc-id".to_string(),
        facets: props,
    }
}

#[test]
fn test_root_to_wit_conversion() {
    let root_doc = create_test_doc();
    let wit_doc: wit::doc::Doc = root_doc.clone().into();

    assert_eq!(wit_doc.id, root_doc.id);

    // Check if note exists in props
    let note_prop = wit_doc
        .facets
        .iter()
        .find(|(key, _)| key == &WellKnownFacetTag::Note.to_string());
    assert!(note_prop.is_some());

    assert_eq!(wit_doc.facets.len(), root_doc.facets.len());
}

#[test]
fn test_wit_to_root_conversion() -> Res<()> {
    let root_doc = create_test_doc();
    let wit_doc: wit::doc::Doc = root_doc.clone().into();
    let converted_back: Doc = wit_doc.try_into()?;

    assert_eq!(converted_back.id, root_doc.id);
    assert_eq!(converted_back.facets.len(), root_doc.facets.len());

    // Check specific prop
    let key = FacetKey::from(WellKnownFacetTag::Note);
    assert_eq!(converted_back.facets.get(&key), root_doc.facets.get(&key));

    Ok(())
}

#[test]
fn test_round_trip_root_wit_root() -> Res<()> {
    let original = create_test_doc();
    let wit: wit::doc::Doc = original.clone().into();
    let back: Doc = wit.try_into()?;

    assert_eq!(back.id, original.id);
    assert_eq!(back.facets.len(), original.facets.len());
    assert_eq!(back.facets, original.facets);

    Ok(())
}

#[test]
fn test_doc_with_blob() -> Res<()> {
    let mut props = HashMap::new();
    let blob = Blob {
        mime: "image/jpeg".to_string(),
        length_octets: 1024,
        digest: "hash123".to_string(),
        inline: Some(vec![1, 2, 3]),
        urls: Some(vec!["db+blob:///hash123".to_string()]),
    };
    props.insert(
        FacetKey::from(WellKnownFacetTag::Note),
        FacetRaw::from(WellKnownFacet::Blob(blob)),
    );

    let root_doc = Doc {
        id: "blob-doc".to_string(),
        facets: props,
    };

    let wit_doc: wit::doc::Doc = root_doc.clone().into();
    let back: Doc = wit_doc.try_into()?;

    assert_eq!(back.facets, root_doc.facets);
    Ok(())
}

#[test]
fn test_doc_with_all_prop_types() -> Res<()> {
    let mut props = HashMap::new();
    props.insert(
        FacetKey::from(WellKnownFacetTag::RefGeneric),
        FacetRaw::from(WellKnownFacet::RefGeneric("ref1".to_string())),
    );
    props.insert(
        FacetKey::from(WellKnownFacetTag::LabelGeneric),
        FacetRaw::from(WellKnownFacet::LabelGeneric("label1".to_string())),
    );
    props.insert(
        FacetKey::from(WellKnownFacetTag::ImageMetadata),
        FacetRaw::from(WellKnownFacet::ImageMetadata(ImageMetadata {
            mime: "image/png".to_string(),
            width_px: 1920,
            height_px: 1080,
        })),
    );
    props.insert(
        FacetKey::from(WellKnownFacetTag::PseudoLabel),
        FacetRaw::from(WellKnownFacet::PseudoLabel(vec!["p1".to_string()])),
    );
    props.insert(
        FacetKey::from(WellKnownFacetTag::PathGeneric),
        FacetRaw::from(WellKnownFacet::PathGeneric(PathBuf::from("/path/to/file"))),
    );
    props.insert(
        FacetKey::from(WellKnownFacetTag::TitleGeneric),
        FacetRaw::from(WellKnownFacet::TitleGeneric("Title".to_string())),
    );

    let root_doc = Doc {
        id: "all-props-doc".to_string(),
        facets: props,
    };

    let wit_doc: wit::doc::Doc = root_doc.clone().into();
    let back: Doc = wit_doc.try_into()?;

    assert_eq!(back.facets.len(), root_doc.facets.len());

    for (key, orig_val) in &root_doc.facets {
        let conv_val = back.facets.get(key).unwrap();
        assert_eq!(orig_val, conv_val);
    }
    Ok(())
}
