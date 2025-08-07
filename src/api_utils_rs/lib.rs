pub mod api;
pub mod codecs;
pub mod macros;
pub mod testing;
pub mod validation_errs;
pub mod gen;

pub mod prelude {
    pub use utils_rs::prelude::*;

    pub use crate::api::*;
    pub use crate::interlude::*;
    pub use crate::validation_errs::ValidationErrors;

    pub use axum_extra;
    pub use dotenv_flow;
    pub use educe;
    pub use garde::Validate;
    pub use regex;
    pub use tokio;
    pub use tower;
}

mod interlude {
    pub use utils_rs::prelude::*;

    pub use crate::internal_err;
    pub use axum::{self, response::IntoResponse, Json};
    pub use utoipa::{self, openapi};
}
