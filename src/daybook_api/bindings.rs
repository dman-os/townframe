//! @generated
use super::*;   

pub mod wit {
    wit_bindgen::generate!({
        world: "feat-doc",
        async: true,
        additional_derives: [serde::Serialize, serde::Deserialize],
        with: {
            "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,
            "townframe:api-utils/utils": api_utils_rs::wit::utils,
            "townframe:daybook-api/doc/doc-tag": daybook_types::types::doc::DocTag,
            "townframe:daybook-api/doc-create/error": daybook_types::types::doc::doc_create::Error,
            "townframe:daybook-api/doc/doc-kind": daybook_types::types::doc::DocKind,
            "townframe:daybook-api/doc/doc-blob": daybook_types::types::doc::DocBlob,
            "townframe:daybook-api/doc/doc-image": daybook_types::types::doc::DocImage,
            "townframe:daybook-api/doc-create/input": daybook_types::types::doc::doc_create::Input,
            "townframe:daybook-api/doc/doc": daybook_types::types::doc::Doc,
            "townframe:daybook-api/doc/doc-tag-kind": daybook_types::types::doc::DocTagKind,
            "townframe:daybook-api/doc-create/error-id-occupied": daybook_types::types::doc::doc_create::ErrorIdOccupied,
            "townframe:daybook-api/doc/doc-content": daybook_types::types::doc::DocContent,
        }
    });
}
