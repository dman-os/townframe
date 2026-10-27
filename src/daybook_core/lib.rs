// FIXME: investigate why this is being flagged
// by rustc
#![recursion_limit = "256"]

#[expect(unused)]
mod interlude {
    pub use crate::app::Database;
    pub use crate::stores::AmStore;
    pub use crate::stores::VersionTag;
    pub use crate::stores::Versioned;

    pub use daybook_types::doc::BranchPath;
    pub use daybook_types::doc::BranchPathBuf;
    pub use daybook_types::doc::ChangeHashSet;
    pub use daybook_types::doc::UserPath;
    pub use daybook_types::doc::UserPathBuf;

    pub use am_utils_rs::prelude::*;
    pub use automerge::ActorId;
    pub use autosurgeon::Hydrate;
    pub use autosurgeon::Reconcile;
    pub use big_repo::DocumentId;
    pub use big_repo::SharedBigRepo;
    pub use big_sync_core::ObjKey;
    pub use big_sync_core::PartKey;
    pub use big_sync_core::PeerKey;
    pub use garde::Validate;
    pub use std::borrow::Cow;
    pub use std::collections::HashMap;
    pub use std::collections::HashSet;
    pub use std::path::Path;
    pub use std::path::PathBuf;
    pub use std::rc::Rc;
    pub use std::sync::Arc;
    pub use std::sync::LazyLock;
    pub use std::sync::RwLock;
    pub use struct_patch::Patch;
    pub use tokio;
    pub use utils_rs::CHeapStr;
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

use sqlite_rs as sqlite_rs_alias;
/// Repo-wide and app-wide key/value globals (`kvstore`). Both the app
/// globals database and each repository database carry this unit as their
/// primary migration list; see `app::open_database`.
mod sqlite {
    pub(crate) mod queries {
        include!("sqlite/gen.rs");
    }
}

pub(crate) mod authority;
pub mod blobs;
pub mod config;
// pub mod crypto;
pub mod drawer;
pub mod event_origin;
pub mod imgtools;
pub mod index;
pub mod keyhive;
pub mod local_state;
pub mod plugs;
pub mod progress;
pub mod repo;
#[expect(unused)]
pub mod repos;
pub mod rt;
pub mod secrets;
pub mod stores;
pub mod sync;
pub mod tables;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

#[cfg(feature = "uniffi")]
daybook_types::custom_type_set!();

#[cfg(feature = "uniffi")]
use crate::stores::VersionTag;

#[cfg(feature = "uniffi")]
uniffi::custom_type!(VersionTag, String, {
    remote,
    lower: |tag| format!(
        "{}_{}",
        utils_rs::hash::encode_base58_multibase(tag.actor_id.to_bytes()),
        utils_rs::hash::encode_base58_multibase(tag.version.as_bytes())
    ),
    try_lift: |str| {
        let (actor_id, version)  = str.split_once('_')
            .ok_or_else(|| uniffi::deps::anyhow::anyhow!("unable to parse VersionTag from {str:?}"))?;

        let mut buf = [0_u8; 16];
        utils_rs::hash::decode_base58_multibase_onto(actor_id, &mut buf).to_anyhow()?;
        let actor_id = buf.into();

        let mut buf = [0_u8; 16];
        utils_rs::hash::decode_base58_multibase_onto(version, &mut buf).to_anyhow()?;
        let version = Uuid::from_bytes(buf);

        Ok(VersionTag{
            actor_id,
            version
        })
    }
});

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn peer_id_from_label(label: &str) -> PeerKey {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(label.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(&digest[..32]);
    PeerKey::new(bytes)
}

pub fn part_id_from_label(label: &str) -> PartKey {
    let digest = blake3::hash(label.as_bytes());
    PartKey::new(*digest.as_bytes())
}

pub use blobs::blob_inventory_part_id;
pub use blobs::blob_inventory_part_id_from_doc_id;

pub mod app;
