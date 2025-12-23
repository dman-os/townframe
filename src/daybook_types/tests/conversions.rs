//! Tests for From/Into conversions between root, automerge, and wit types

use daybook_types::automerge;
use daybook_types::doc::{
    Doc, DocBlob, DocContent, DocProp, DocPropKey, ImageMeta, MimeType, WellKnownDocPropKeys,
};
use daybook_types::wit;
use std::collections::HashMap;
use time::OffsetDateTime;

fn create_test_doc() -> Doc {
    let mut props = HashMap::new();
    props.insert(
        DocPropKey::WellKnown(WellKnownDocPropKeys::RefGeneric),
        DocProp::RefGeneric("ref-123".to_string()),
    );
    props.insert(
        DocPropKey::WellKnown(WellKnownDocPropKeys::LabelGeneric),
        DocProp::LabelGeneric("label-1".to_string()),
    );
    props.insert(
        DocPropKey::WellKnown(WellKnownDocPropKeys::PseudoLabel),
        DocProp::PseudoLabel(vec!["pseudo1".to_string(), "pseudo2".to_string()]),
    );
    Doc {
        id: "test-doc-id".to_string(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        content: DocContent::Text("Test content".to_string()),
        props,
    }
}

#[test]
fn test_root_to_wit_conversion() {
    let root_doc = create_test_doc();
    let wit_doc: wit::doc::Doc = root_doc.clone().into();

    assert_eq!(wit_doc.id, root_doc.id);
    // Compare content by checking the text directly
    match &root_doc.content {
        DocContent::Text(text) => {
            // For WIT, we need to check the generated type
            use daybook_types::wit::doc::DocContent as WitDocContent;
            match &wit_doc.content {
                WitDocContent::Text(wit_text) => assert_eq!(text, wit_text),
                _ => panic!(
                    "Expected Text content in WIT doc, got {:?}",
                    wit_doc.content
                ),
            }
        }
        _ => panic!("Unexpected content type: {:?}", root_doc.content),
    }
    assert_eq!(wit_doc.props.len(), root_doc.props.len());
}

#[test]
fn test_wit_to_root_conversion() {
    let root_doc = create_test_doc();
    let wit_doc: wit::doc::Doc = root_doc.clone().into();
    let converted_back: Doc = wit_doc.into();

    assert_eq!(converted_back.id, root_doc.id);
    assert_eq!(converted_back.content, root_doc.content);
    assert_eq!(converted_back.props.len(), root_doc.props.len());
}

#[test]
fn test_root_to_automerge_conversion() {
    // When automerge is enabled, Doc IS automerge::Doc, so this is a no-op
    let root_doc = create_test_doc();
    let am_doc: automerge::doc::Doc = root_doc.clone().into();

    assert_eq!(am_doc.id, root_doc.id);
    assert_eq!(am_doc.created_at, root_doc.created_at);
    assert_eq!(am_doc.updated_at, root_doc.updated_at);
    // Content types are different, compare by converting
    let am_content: daybook_types::doc::DocContent = am_doc.content.into();
    assert_eq!(am_content, root_doc.content);
    assert_eq!(am_doc.props.len(), root_doc.props.len());
}

#[test]
fn test_automerge_to_root_conversion() {
    // When automerge is enabled, Doc IS automerge::Doc, so this is a no-op
    let root_doc = create_test_doc();
    let am_doc: automerge::doc::Doc = root_doc.clone().into();
    let converted_back: Doc = am_doc.into();

    assert_eq!(converted_back.id, root_doc.id);
    // Content should match after round trip
    assert_eq!(converted_back.content, root_doc.content);
    assert_eq!(converted_back.props.len(), root_doc.props.len());
}

#[test]
fn test_automerge_to_wit_conversion() {
    let root_doc = create_test_doc();
    let am_doc: automerge::doc::Doc = root_doc.clone().into();
    // Convert through root Doc
    let root_from_am: Doc = am_doc.clone().into();
    let wit_doc: wit::doc::Doc = root_from_am.into();

    assert_eq!(wit_doc.id, am_doc.id);
    // Compare content by checking the text directly
    // Convert automerge content to root first
    let root_content: daybook_types::doc::DocContent = am_doc.content.into();
    match &root_content {
        DocContent::Text(text) => {
            // For WIT, we need to check the generated type
            use daybook_types::wit::doc::DocContent as WitDocContent;
            match &wit_doc.content {
                WitDocContent::Text(wit_text) => assert_eq!(text, wit_text),
                _ => panic!(
                    "Expected Text content in WIT doc, got {:?}",
                    wit_doc.content
                ),
            }
        }
        _ => panic!("Unexpected content type: {:?}", root_content),
    }
    assert_eq!(wit_doc.props.len(), am_doc.props.len());
}

#[test]
fn test_wit_to_automerge_conversion() {
    let root_doc = create_test_doc();
    let wit_doc: wit::doc::Doc = root_doc.clone().into();
    // Convert through root Doc since there's no direct wit -> automerge conversion
    let root_from_wit: Doc = wit_doc.into();
    let am_doc: automerge::doc::Doc = root_from_wit.into();

    assert_eq!(am_doc.id, root_doc.id);
    // Content types are different, compare by converting
    let am_content: daybook_types::doc::DocContent = am_doc.content.into();
    assert_eq!(am_content, root_doc.content);
    assert_eq!(am_doc.props.len(), root_doc.props.len());
}

#[test]
fn test_round_trip_root_wit_root() {
    let original = create_test_doc();
    let wit: wit::doc::Doc = original.clone().into();
    let back: Doc = wit.into();

    assert_eq!(back.id, original.id);
    assert_eq!(back.content, original.content);
    assert_eq!(back.props.len(), original.props.len());
}

#[test]
fn test_round_trip_root_automerge_root() {
    let original = create_test_doc();
    let am: automerge::doc::Doc = original.clone().into();
    let back: Doc = am.into();

    assert_eq!(back.id, original.id);
    assert_eq!(back.content, original.content);
    assert_eq!(back.props.len(), original.props.len());
}

#[test]
fn test_round_trip_automerge_wit_automerge() {
    let root_doc = create_test_doc();
    let am: automerge::doc::Doc = root_doc.clone().into();
    // Convert through root Doc
    let root_from_am: Doc = am.clone().into();
    let wit: wit::doc::Doc = root_from_am.into();
    // Convert back through root Doc
    let root_from_wit: Doc = wit.into();
    let back: automerge::doc::Doc = root_from_wit.into();

    assert_eq!(back.id, am.id);
    assert_eq!(back.content, am.content);
    assert_eq!(back.props.len(), am.props.len());
}

#[test]
fn test_doc_with_blob_content() {
    let root_doc = Doc {
        id: "blob-doc".to_string(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        content: DocContent::Blob(DocBlob {
            length_octets: 1024,
            hash: "hash123".to_string(),
        }),
        props: HashMap::new(),
    };

    let wit_doc: wit::doc::Doc = root_doc.clone().into();
    let back: Doc = wit_doc.into();

    match (&root_doc.content, &back.content) {
        (DocContent::Blob(orig), DocContent::Blob(conv)) => {
            assert_eq!(orig.length_octets, conv.length_octets);
            assert_eq!(orig.hash, conv.hash);
        }
        _ => panic!("Blob content not preserved"),
    }
}

#[test]
fn test_doc_with_all_prop_types() {
    use daybook_types::doc::{DocPropKey, WellKnownDocPropKeys};
    let mut props = HashMap::new();
    props.insert(
        DocPropKey::WellKnown(WellKnownDocPropKeys::RefGeneric),
        DocProp::RefGeneric("ref1".to_string()),
    );
    props.insert(
        DocPropKey::WellKnown(WellKnownDocPropKeys::LabelGeneric),
        DocProp::LabelGeneric("label1".to_string()),
    );
    props.insert(
        DocPropKey::WellKnown(WellKnownDocPropKeys::ImageMetadata),
        DocProp::ImageMetadata(ImageMeta {
            mime: MimeType::from("image/png".to_string()),
            width_px: 1920,
            height_px: 1080,
        }),
    );
    props.insert(
        DocPropKey::WellKnown(WellKnownDocPropKeys::PseudoLabel),
        DocProp::PseudoLabel(vec!["p1".to_string(), "p2".to_string()]),
    );
    props.insert(
        DocPropKey::WellKnown(WellKnownDocPropKeys::PathGeneric),
        DocProp::PathGeneric("/path/to/file".to_string()),
    );
    props.insert(
        DocPropKey::WellKnown(WellKnownDocPropKeys::TitleGeneric),
        DocProp::TitleGeneric("Title".to_string()),
    );

    let root_doc = Doc {
        id: "all-props-doc".to_string(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        content: DocContent::Text("Content".to_string()),
        props,
    };

    let wit_doc: wit::doc::Doc = root_doc.clone().into();
    let back: Doc = wit_doc.into();

    assert_eq!(back.props.len(), root_doc.props.len());
    // Compare props by values (keys might change during WIT conversion due to serialization)
    // Collect all props from both docs and compare them
    let mut orig_props: Vec<_> = root_doc.props.values().collect();
    let mut conv_props: Vec<_> = back.props.values().collect();
    orig_props.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
    conv_props.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));

    assert_eq!(orig_props.len(), conv_props.len());
    for (orig_prop, conv_prop) in orig_props.iter().zip(conv_props.iter()) {
        match (orig_prop, conv_prop) {
            (DocProp::RefGeneric(a), DocProp::RefGeneric(b)) => assert_eq!(a, b),
            (DocProp::LabelGeneric(a), DocProp::LabelGeneric(b)) => assert_eq!(a, b),
            (DocProp::ImageMetadata(a), DocProp::ImageMetadata(b)) => {
                assert_eq!(a.mime, b.mime);
                assert_eq!(a.width_px, b.width_px);
                assert_eq!(a.height_px, b.height_px);
            }
            (DocProp::PseudoLabel(a), DocProp::PseudoLabel(b)) => assert_eq!(a, b),
            (DocProp::PathGeneric(a), DocProp::PathGeneric(b)) => assert_eq!(a, b),
            (DocProp::TitleGeneric(a), DocProp::TitleGeneric(b)) => assert_eq!(a, b),
            _ => panic!("Prop type mismatch: {:?} vs {:?}", orig_prop, conv_prop),
        }
    }
}
