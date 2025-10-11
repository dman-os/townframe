//! @generated
use super::*;   

pub mod user {
    use super::*;

    pub const TAG: api::Tag = api::Tag {
        name: "user",
        desc: "User mgmt.",
    };

    #[derive(Debug, Clone, utoipa::ToSchema, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct User {
        pub id: String,
        pub created_at: Datetime,
        pub updated_at: Datetime,
        pub email: Option<String>,
        pub username: String,
    }

    pub mod user_create {
        use super::*;

        #[derive(Debug, Clone)]
        pub struct UserCreate;

        pub type Output = SchemaRef<User>;

        #[derive(Debug, Clone, garde::Validate, utoipa::ToSchema, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct Input {
            #[schema(min_length = 3, max_length = 25, pattern = "USERNAME_REGEX")]
            #[garde(ascii, pattern(USERNAME_REGEX), length(min = 3, max = 25))]
            pub username: String,
            #[garde(email)]
            pub email: Option<String>,
            #[schema(min_length = 8, max_length = 1024)]
            #[garde(length(min = 8, max = 1024))]
            pub password: String,
        }

        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, utoipa::ToSchema, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", tag = "error")]
        /// Username occupied: {username}
        pub struct ErrorUsernameOccupied {
            pub username: String,
        }

        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, utoipa::ToSchema, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", tag = "error")]
        /// Email occupied: {email:?}
        pub struct ErrorEmailOccupied {
            /// example: alice@example.com
            pub email: Option<String>,
        }

        #[derive(Debug, thiserror::Error, displaydoc::Display, utoipa::ToSchema, macros::HttpError, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", tag = "error")]
        pub enum Error {
            /// Username occupied {0}
            #[http(code(StatusCode::BAD_REQUEST), desc("Username occupied"))]
            UsernameOccupied(#[from] ErrorUsernameOccupied),
            /// Email occupied {0}
            #[http(code(StatusCode::BAD_REQUEST), desc("Email occupied"))]
            EmailOccupied(#[from] ErrorEmailOccupied),
            /// Invalid input {0}
            #[http(code(StatusCode::BAD_REQUEST), desc("Invalid input"))]
            InvalidInput(#[from] ErrorsValidation),
            /// Internal server error {0}
            #[http(code(StatusCode::INTERNAL_SERVER_ERROR), desc("Internal server error"))]
            Internal(#[from] ErrorInternal),
        }
    }

}
