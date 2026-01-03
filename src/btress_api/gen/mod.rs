//! @generated
use super::*;   

pub mod user {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct User {
        pub id: Uuid,
        pub created_at: Timestamp,
        pub updated_at: Timestamp,
        pub email: Option<String>,
        pub username: String,
    }

    pub mod user_create {
        use super::*;

        #[derive(Debug, Clone)]
        pub struct UserCreate;

        pub type Output = User;

        #[derive(Debug, Clone, garde::Validate, Serialize, Deserialize)]
        pub struct Input {
            #[garde(ascii, pattern(USERNAME_REGEX), length(min = 3, max = 25))]
            pub username: String,
            #[garde(email)]
            pub email: Option<String>,
            #[garde(length(min = 8, max = 1024))]
            pub password: String,
        }

        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, Serialize, Deserialize)]
        /// Username occupied: {username}
        pub struct ErrorUsernameOccupied {
            pub username: String,
        }

        #[derive(Debug, Clone, thiserror::Error, displaydoc::Display, Serialize, Deserialize)]
        /// Email occupied: {email:?}
        pub struct ErrorEmailOccupied {
            /// example: alice@example.com
            pub email: Option<String>,
        }

        #[derive(Debug, thiserror::Error, displaydoc::Display, Serialize, Deserialize)]
        pub enum Error {
            /// Username occupied {0}
            UsernameOccupied(#[from] ErrorUsernameOccupied),
            /// Email occupied {0}
            EmailOccupied(#[from] ErrorEmailOccupied),
            /// Invalid input {0}
            InvalidInput(#[from] ErrorsValidation),
            /// Internal server error {0}
            Internal(#[from] ErrorInternal),
        }
    }

}
