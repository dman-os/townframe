use crate::{interlude::*, wit};

use crate::doc::{
    Blob, Doc, DocContent, DocProp, DocPropKey, ImageMetadata, WellKnownProp, WellKnownPropTag,
};
use std::collections::HashMap;
use time::OffsetDateTime;

fn create_test_doc() -> Doc {
    let mut props = HashMap::new();

    props.insert(
        DocPropKey::from(WellKnownPropTag::RefGeneric),
        DocProp::from(WellKnownProp::RefGeneric("ref-123".to_string())),
    );
    props.insert(
        DocPropKey::from(WellKnownPropTag::LabelGeneric),
        DocProp::from(WellKnownProp::LabelGeneric("label-1".to_string())),
    );
    props.insert(
        DocPropKey::from(WellKnownPropTag::PseudoLabel),
        DocProp::from(WellKnownProp::PseudoLabel("pseudo-label".to_string())),
    );
    props.insert(
        DocPropKey::from(WellKnownPropTag::Content),
        DocProp::from(WellKnownProp::Content(DocContent::Text(
            "Test content".to_string(),
        ))),
    );

    Doc {
        id: "test-doc-id".to_string(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        props,
    }
}

#[test]
fn test_root_to_wit_conversion() {
    let root_doc = create_test_doc();
    let wit_doc: wit::doc::Doc = root_doc.clone().into();

    assert_eq!(wit_doc.id, root_doc.id);

    // Check if content exists in props
    let content_prop = wit_doc
        .props
        .iter()
        .find(|(k, _)| k == &WellKnownPropTag::Content.to_string());
    assert!(content_prop.is_some());

    assert_eq!(wit_doc.props.len(), root_doc.props.len());
}

#[test]
fn test_wit_to_root_conversion() -> Res<()> {
    let root_doc = create_test_doc();
    let wit_doc: wit::doc::Doc = root_doc.clone().into();
    let converted_back: Doc = wit_doc.try_into()?;

    assert_eq!(converted_back.id, root_doc.id);
    assert_eq!(converted_back.props.len(), root_doc.props.len());

    // Check specific prop
    let key = DocPropKey::from(WellKnownPropTag::Content);
    assert_eq!(converted_back.props.get(&key), root_doc.props.get(&key));

    Ok(())
}

#[test]
#[cfg(feature = "automerge")]
fn test_root_to_automerge_conversion() {
    let root_doc = create_test_doc();
    let am_doc: crate::automerge::doc::Doc = root_doc.clone().into();

    assert_eq!(am_doc.id, root_doc.id);
    assert_eq!(am_doc.created_at, root_doc.created_at);
    assert_eq!(am_doc.updated_at, root_doc.updated_at);
    assert_eq!(am_doc.props.len(), root_doc.props.len());
}

#[test]
#[cfg(feature = "automerge")]
fn test_automerge_to_root_conversion() {
    let root_doc = create_test_doc();
    let am_doc: crate::automerge::doc::Doc = root_doc.clone().into();
    let converted_back: Doc = am_doc.into();

    assert_eq!(converted_back.id, root_doc.id);
    assert_eq!(converted_back.props.len(), root_doc.props.len());
}

#[test]
#[cfg(feature = "automerge")]
fn test_automerge_to_wit_conversion() {
    let root_doc = create_test_doc();
    let am_doc: crate::automerge::doc::Doc = root_doc.clone().into();
    // Convert through root Doc
    let root_from_am: Doc = am_doc.clone().into();
    let wit_doc: wit::doc::Doc = root_from_am.into();

    assert_eq!(wit_doc.id, am_doc.id);
    assert_eq!(wit_doc.props.len(), am_doc.props.len());
}

#[test]
#[cfg(feature = "automerge")]
fn test_wit_to_automerge_conversion() -> Res<()> {
    let root_doc = create_test_doc();
    let wit_doc: wit::doc::Doc = root_doc.clone().into();
    // Convert through root Doc
    let root_from_wit: Doc = wit_doc.try_into()?;
    let am_doc: crate::automerge::doc::Doc = root_from_wit.into();

    assert_eq!(am_doc.id, root_doc.id);
    assert_eq!(am_doc.props.len(), root_doc.props.len());

    Ok(())
}

#[test]
fn test_round_trip_root_wit_root() -> Res<()> {
    let original = create_test_doc();
    let wit: wit::doc::Doc = original.clone().into();
    let back: Doc = wit.try_into()?;

    assert_eq!(back.id, original.id);
    assert_eq!(back.props.len(), original.props.len());
    assert_eq!(back.props, original.props);

    Ok(())
}

#[test]
#[cfg(feature = "automerge")]
fn test_round_trip_root_automerge_root() {
    let original = create_test_doc();
    let am: crate::automerge::doc::Doc = original.clone().into();
    let back: Doc = am.into();

    assert_eq!(back.id, original.id);
    assert_eq!(back.props.len(), original.props.len());
    assert_eq!(back.props, original.props);
}

#[test]
fn test_doc_with_blob_content() -> Res<()> {
    let mut props = HashMap::new();
    let blob = Blob {
        length_octets: 1024,
        hash: "hash123".to_string(),
    };
    props.insert(
        DocPropKey::from(WellKnownPropTag::Content),
        DocProp::from(WellKnownProp::Content(DocContent::Blob(blob))),
    );

    let root_doc = Doc {
        id: "blob-doc".to_string(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        props,
    };

    let wit_doc: wit::doc::Doc = root_doc.clone().into();
    let back: Doc = wit_doc.try_into()?;

    assert_eq!(back.props, root_doc.props);
    Ok(())
}

#[test]
fn test_doc_with_all_prop_types() -> Res<()> {
    let mut props = HashMap::new();
    props.insert(
        DocPropKey::from(WellKnownPropTag::RefGeneric),
        DocProp::from(WellKnownProp::RefGeneric("ref1".to_string())),
    );
    props.insert(
        DocPropKey::from(WellKnownPropTag::LabelGeneric),
        DocProp::from(WellKnownProp::LabelGeneric("label1".to_string())),
    );
    props.insert(
        DocPropKey::from(WellKnownPropTag::ImageMetadata),
        DocProp::from(WellKnownProp::ImageMetadata(ImageMetadata {
            mime: "image/png".to_string(),
            width_px: 1920,
            height_px: 1080,
        })),
    );
    props.insert(
        DocPropKey::from(WellKnownPropTag::PseudoLabel),
        DocProp::from(WellKnownProp::PseudoLabel("p1".to_string())),
    );
    props.insert(
        DocPropKey::from(WellKnownPropTag::PathGeneric),
        DocProp::from(WellKnownProp::PathGeneric(PathBuf::from("/path/to/file"))),
    );
    props.insert(
        DocPropKey::from(WellKnownPropTag::TitleGeneric),
        DocProp::from(WellKnownProp::TitleGeneric("Title".to_string())),
    );

    let root_doc = Doc {
        id: "all-props-doc".to_string(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        props,
    };

    let wit_doc: wit::doc::Doc = root_doc.clone().into();
    let back: Doc = wit_doc.try_into()?;

    assert_eq!(back.props.len(), root_doc.props.len());

    for (key, orig_val) in &root_doc.props {
        let conv_val = back.props.get(key).unwrap();
        assert_eq!(orig_val, conv_val);
    }
    Ok(())
}
