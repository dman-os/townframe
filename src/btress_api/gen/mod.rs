//! @generated
use super::*;   

pub mod user {
    use super::*;

    #[cfg(feature = "automerge")]
    pub type OffsetDateTime = time::OffsetDateTime;
    #[cfg(not(feature = "automerge"))]
    pub type OffsetDateTime = Datetime;

    #[cfg(feature = "utoipa")]
    pub const TAG: api::Tag = api::Tag {
        name: "user",
        desc: "User mgmt.",
    };

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    #[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
    pub struct User {
        pub id: String,
        pub created_at: OffsetDateTime,
        pub updated_at: OffsetDateTime,
        pub email: Option<String>,
        pub username: String,
    }

    pub mod user_create {
        use super::*;

        #[derive(Debug, Clone)]
        pub struct UserCreate;


        #[cfg(feature = "utoipa")]
        pub type Output = SchemaRef<User>;
        #[cfg(not(feature = "utoipa"))]
        pub type Output = User;


        #[derive(Debug, Clone, garde::Validate)]
        #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
        pub struct Input {
            #[cfg_attr(feature = "utoipa", schema(min_length = 3, max_length = 25, pattern = "USERNAME_REGEX"))]
            #[garde(ascii, pattern(USERNAME_REGEX), length(min = 3, max = 25))]
            pub username: String,
            #[garde(email)]
            pub email: Option<String>,
            #[cfg_attr(feature = "utoipa", schema(min_length = 8, max_length = 1024))]
            #[garde(length(min = 8, max = 1024))]
            pub password: String,
        }

        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, utoipa::ToSchema)]
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "serde", serde(rename_all = "camelCase", tag = "error"))]
        /// Username occupied: {username}
        pub struct ErrorUsernameOccupied {
            pub username: String,
        }
        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, utoipa::ToSchema)]
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "serde", serde(rename_all = "camelCase", tag = "error"))]
        /// Email occupied: {email:?}
        pub struct ErrorEmailOccupied {
            /// example: alice@example.com
            pub email: Option<String>,
        }
        #[derive(
            Debug,
            thiserror::Error,
            displaydoc::Display,
        )]
        #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema, macros::HttpError))]
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "serde", serde(rename_all = "camelCase", tag = "error"))]
        pub enum Error {
            /// Username occupied {0}
            #[cfg_attr(feature = "utoipa", http(code(StatusCode::BAD_REQUEST), desc("Username occupied")))]
            UsernameOccupied(#[from] ErrorUsernameOccupied),
            /// Email occupied {0}
            #[cfg_attr(feature = "utoipa", http(code(StatusCode::BAD_REQUEST), desc("Email occupied")))]
            EmailOccupied(#[from] ErrorEmailOccupied),
            /// Invalid input {0}
            #[cfg_attr(feature = "utoipa", http(code(StatusCode::BAD_REQUEST), desc("Invalid input")))]
            InvalidInput(#[from] ErrorsValidation),
            /// Internal server error {0}
            #[cfg_attr(feature = "utoipa", http(code(StatusCode::INTERNAL_SERVER_ERROR), desc("Internal server error")))]
            Internal(#[from] ErrorInternal),
        }
    }

}
