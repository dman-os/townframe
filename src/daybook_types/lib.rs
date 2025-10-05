mod interlude {
    pub use api_utils_rs::{api, prelude::*};
    #[cfg(feature = "automerge")]
    pub use autosurgeon::{Hydrate, Reconcile};
    pub use time::OffsetDateTime;
}

use interlude::*;

pub mod types;

pub use types::doc;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();


#[cfg(feature = "uniffi")]
uniffi::custom_type!(OffsetDateTime, i64, {
    remote,
    lower: |dt| dt.unix_timestamp(),
    try_lift: |int| OffsetDateTime::from_unix_timestamp(int)
        .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
});

// uniffi::custom_type!(Uuid, Vec<u8>, {
//     remote,
//     lower: |uuid| uuid.as_bytes().to_vec(),
//     try_lift: |bytes: Vec<u8>| {
//         uuid::Uuid::from_slice(&bytes)
//             .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
//     }
// });