//! Tests for From/Into conversions between root, automerge, and wit types

use daybook_types::*;
use daybook_types::automerge;
use daybook_types::wit;
use time::OffsetDateTime;

fn create_test_doc() -> Doc {
    Doc {
        id: "test-doc-id".to_string(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        content: DocContent::Text("Test content".to_string()),
        props: vec![
            DocProp::RefGeneric("ref-123".to_string()),
            DocProp::LabelGeneric("label-1".to_string()),
            DocProp::PseudoLabel(vec!["pseudo1".to_string(), "pseudo2".to_string()]),
        ],
    }
}

#[test]
fn test_root_to_wit_conversion() {
    let root_doc = create_test_doc();
    let wit_doc: wit::Doc = root_doc.clone().into();
    
    assert_eq!(wit_doc.id, root_doc.id);
    // Compare content by checking the text directly
    match &root_doc.content {
        DocContent::Text(text) => {
            // For WIT, we need to check the generated type
            use daybook_types::gen::wit::doc::DocContent as WitDocContent;
            match &wit_doc.content {
                WitDocContent::Text(wit_text) => assert_eq!(text, wit_text),
                _ => panic!("Expected Text content in WIT doc, got {:?}", wit_doc.content),
            }
        }
        _ => panic!("Unexpected content type: {:?}", root_doc.content),
    }
    assert_eq!(wit_doc.props.len(), root_doc.props.len());
}

#[test]
fn test_wit_to_root_conversion() {
    let root_doc = create_test_doc();
    let wit_doc: wit::Doc = root_doc.clone().into();
    let converted_back: Doc = wit_doc.into();
    
    assert_eq!(converted_back.id, root_doc.id);
    assert_eq!(converted_back.content, root_doc.content);
    assert_eq!(converted_back.props.len(), root_doc.props.len());
}

#[test]
fn test_root_to_automerge_conversion() {
    // When automerge is enabled, Doc IS automerge::Doc, so this is a no-op
    let root_doc = create_test_doc();
    let am_doc: automerge::Doc = root_doc.clone().into();
    
    assert_eq!(am_doc.id, root_doc.id);
    assert_eq!(am_doc.created_at, root_doc.created_at);
    assert_eq!(am_doc.updated_at, root_doc.updated_at);
    assert_eq!(am_doc.content, root_doc.content);
    assert_eq!(am_doc.props.len(), root_doc.props.len());
}

#[test]
fn test_automerge_to_root_conversion() {
    // When automerge is enabled, Doc IS automerge::Doc, so this is a no-op
    let root_doc = create_test_doc();
    let am_doc: automerge::Doc = root_doc.clone().into();
    let converted_back: Doc = am_doc.into();
    
    assert_eq!(converted_back.id, root_doc.id);
    assert_eq!(converted_back.content, root_doc.content);
    assert_eq!(converted_back.props.len(), root_doc.props.len());
}

#[test]
fn test_automerge_to_wit_conversion() {
    let root_doc = create_test_doc();
    let am_doc: automerge::Doc = root_doc.clone().into();
    let wit_doc: wit::Doc = am_doc.clone().into();
    
    assert_eq!(wit_doc.id, am_doc.id);
    // Compare content by checking the text directly
    match &am_doc.content {
        DocContent::Text(text) => {
            // For WIT, we need to check the generated type
            use daybook_types::gen::wit::doc::DocContent as WitDocContent;
            match &wit_doc.content {
                WitDocContent::Text(wit_text) => assert_eq!(text, wit_text),
                _ => panic!("Expected Text content in WIT doc, got {:?}", wit_doc.content),
            }
        }
        _ => panic!("Unexpected content type: {:?}", am_doc.content),
    }
    assert_eq!(wit_doc.props.len(), am_doc.props.len());
}

#[test]
fn test_wit_to_automerge_conversion() {
    let root_doc = create_test_doc();
    let wit_doc: wit::Doc = root_doc.clone().into();
    let am_doc: automerge::Doc = wit_doc.into();
    
    assert_eq!(am_doc.id, root_doc.id);
    assert_eq!(am_doc.content, root_doc.content);
    assert_eq!(am_doc.props.len(), root_doc.props.len());
}

#[test]
fn test_round_trip_root_wit_root() {
    let original = create_test_doc();
    let wit: wit::Doc = original.clone().into();
    let back: Doc = wit.into();
    
    assert_eq!(back.id, original.id);
    assert_eq!(back.content, original.content);
    assert_eq!(back.props.len(), original.props.len());
}

#[test]
fn test_round_trip_root_automerge_root() {
    let original = create_test_doc();
    let am: automerge::Doc = original.clone().into();
    let back: Doc = am.into();
    
    assert_eq!(back.id, original.id);
    assert_eq!(back.content, original.content);
    assert_eq!(back.props.len(), original.props.len());
}

#[test]
fn test_round_trip_automerge_wit_automerge() {
    let root_doc = create_test_doc();
    let am: automerge::Doc = root_doc.clone().into();
    let wit: wit::Doc = am.clone().into();
    let back: automerge::Doc = wit.into();
    
    assert_eq!(back.id, am.id);
    assert_eq!(back.content, am.content);
    assert_eq!(back.props.len(), am.props.len());
}

#[test]
fn test_doc_with_blob_content() {
    use daybook_types::doc::DocBlob;
    
    let root_doc = Doc {
        id: "blob-doc".to_string(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        content: DocContent::Blob(DocBlob {
            length_octets: 1024,
            hash: "hash123".to_string(),
        }),
        props: vec![],
    };
    
    let wit_doc: wit::Doc = root_doc.clone().into();
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
    use daybook_types::doc::{ImageMeta, MimeType};
    
    let root_doc = Doc {
        id: "all-props-doc".to_string(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        content: DocContent::Text("Content".to_string()),
        props: vec![
            DocProp::RefGeneric("ref1".to_string()),
            DocProp::LabelGeneric("label1".to_string()),
            DocProp::ImageMetadata(ImageMeta {
                mime: MimeType::from("image/png".to_string()),
                width_px: 1920,
                height_px: 1080,
            }),
            DocProp::PseudoLabel(vec!["p1".to_string(), "p2".to_string()]),
            DocProp::PathGeneric("/path/to/file".to_string()),
            DocProp::TitleGeneric("Title".to_string()),
        ],
    };
    
    let wit_doc: wit::Doc = root_doc.clone().into();
    let back: Doc = wit_doc.into();
    
    assert_eq!(back.props.len(), root_doc.props.len());
    for (orig, conv) in root_doc.props.iter().zip(back.props.iter()) {
        match (orig, conv) {
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
            _ => panic!("Prop type mismatch: {:?} vs {:?}", orig, conv),
        }
    }
}
