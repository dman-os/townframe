use super::*;   

pub mod doc {
    use super::*;

    pub const TAG: api::Tag = api::Tag {
        name: "doc",
        desc: "Doc mgmt.",
    };

    #[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct Doc {
        pub id: String,
        pub created_at: Datetime,
        pub updated_at: Datetime,
    }

    pub mod doc_create {
        use super::*;

        #[derive(Debug, Clone)]
        pub struct DocCreate;

        pub type Output = SchemaRef<Doc>;

        #[derive(Debug, Clone, Serialize, Deserialize, garde::Validate, utoipa::ToSchema)]
        #[serde(rename_all = "camelCase")]
        pub struct Input {
            #[schema(min_length = 1, max_length = 1024)]
            #[garde(length(min = 1, max = 1024))]
            pub id: String,
        }

        #[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error, displaydoc::Display, utoipa::ToSchema)]
        #[serde(rename_all = "camelCase", tag = "error")]
        /// Id occupied: {id}
        pub struct ErrorIdOccupied {
            pub id: String,
        }
        #[derive(
            Debug,
            Serialize,
            thiserror::Error,
            displaydoc::Display,
            macros::HttpError,
            utoipa::ToSchema,
        )]
        #[serde(rename_all = "camelCase", tag = "error")]
        pub enum Error {
            /// Id occupied {0}
            #[http(code(StatusCode::BAD_REQUEST), desc("Id occupied"))]
            IdOccupied(#[from] ErrorIdOccupied),
            /// Invalid input {0}
            #[http(code(StatusCode::BAD_REQUEST), desc("Invalid input"))]
            InvalidInput(#[from] ErrorsValidation),
            /// Internal server error {0}
            #[http(code(StatusCode::INTERNAL_SERVER_ERROR), desc("Internal server error"))]
            Internal(#[from] ErrorInternal),
        }
    }

    pub mod wit {
        wit_bindgen::generate!({
            world: "feat-doc",
            async: true,
            additional_derives: [serde::Serialize, serde::Deserialize],
            with: {
                "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,
                "townframe:api-utils/utils": api_utils_rs::wit::utils,
                "townframe:daybook-api/doc/doc": crate::gen::doc::Doc,
                "townframe:daybook-api/doc-create/error-id-occupied": crate::gen::doc::doc_create::ErrorIdOccupied,
                "townframe:daybook-api/doc-create/input": crate::gen::doc::doc_create::Input,
                "townframe:daybook-api/doc-create/error": crate::gen::doc::doc_create::Error,
            }
        });
    }
}
