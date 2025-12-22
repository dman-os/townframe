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
            "townframe:daybook-api/doc/doc-prop": daybook_types::gen::wit::doc::DocProp,
            "townframe:daybook-api/doc-create/error": daybook_types::gen::wit::doc::doc_create::Error,
            "townframe:daybook-api/doc/doc-kind": daybook_types::gen::wit::doc::DocKind,
            "townframe:daybook-api/doc/doc-blob": daybook_types::gen::wit::doc::DocBlob,
            "townframe:daybook-api/doc/doc-image": daybook_types::gen::wit::doc::DocImage,
            "townframe:daybook-api/doc-create/input": daybook_types::gen::wit::doc::doc_create::Input,
            "townframe:daybook-api/doc/doc": daybook_types::wit::Doc,
            "townframe:daybook-api/doc/doc-prop-kind": daybook_types::gen::wit::doc::DocPropKind,
            "townframe:daybook-api/doc-create/error-id-occupied": daybook_types::gen::wit::doc::doc_create::ErrorIdOccupied,
            "townframe:daybook-api/doc/doc-content": daybook_types::gen::wit::doc::DocContent,
        }
    });
}
