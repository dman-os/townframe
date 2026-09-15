//! Backend-neutral trees, recorded observations, and backend-specific delivery.
//!
//! Checkout bindings, reconciliation, publication, and durable recovery are not
//! implemented here. Historical SQL migrations remain unchanged.

mod interlude {
    pub use std::error::Error;
    pub use utils_rs::prelude::*;
}

pub mod backends;
#[cfg(all(feature = "sqlite", unix))]
pub mod vtree;

#[cfg(test)]
mod e2e;
