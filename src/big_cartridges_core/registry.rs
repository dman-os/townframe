//! The registry drawer's document layout (ADR 028 §1).
//!
//! The registry is a grouped drawer holding one document per cartridge id. Everything about a cartridge
//! lives in that document as datums, so any node that syncs the drawer discovers the same cartridges
//! by reading it, and no index outside the drawer is authoritative:
//!
//! - `org.example.daybook.cartridge.identity/main` ([`CartridgeIdentity`]) names the cartridge id the document
//!   is for, and is what a scan of the drawer selects documents by;
//! - `org.example.daybook.cartridge/<version>` holds the immutable envelope ([`Cartridge`]) of one version,
//!   keyed by the semver label ([`version_key`]);
//! - `org.example.daybook.blob/cartridge.artifact/<version>/<artifactKey>` ([`artifact_key`]) is a
//!   [`Blob`] reference per artifact of that version, which is what makes big_blobs retain,
//!   inventory and fetch the bytes.

use crate::Cartridge;
use crate::CartridgeId;
use crate::Version;
use big_blobs_core::Blob;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumPin;
use big_cabinet_core::DatumReference;
use big_cabinet_core::Location;
use big_cabinet_core::ReferenceDocId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::Extra;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::doc::ChangeHash;
use serde::Deserialize;
use serde::Serialize;

/// `org.example.daybook.cartridge.identity/main`: the cartridge id a registry document is for. Written once
/// when the document is created and removed with the cartridge, so a document without it is not a
/// cartridge.
#[macros::datum(authority = Cartridge::ID, name = "identity")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeIdentity {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub id: CartridgeId,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The slot [`CartridgeIdentity`] is written at.
pub fn identity_key() -> DatumKey {
    DatumKey::from_text(CartridgeIdentity::ID, "main").expect("`main` is a valid datum key slot")
}

/// The slot prefix of the [`Blob`] datums a registry document keeps for artifacts. A distinct
/// prefix from the nodule's `nodule.artifact/`, so the two projections never claim each other's
/// blobs.
pub const ARTIFACT_SLOT_PREFIX: &str = "cartridge.artifact/";

/// `org.example.daybook.cartridge/<version>`: where the envelope of one version is stored. The slot is
/// the version's canonical text, which is also what a reader checks the envelope's own version
/// against.
pub fn version_key(version: &Version) -> DatumKey {
    DatumKey::from_text(Cartridge::ID, version.to_string())
        .expect("a semver label is a valid datum key slot")
}

/// The pin of `version`'s envelope in the registry document `doc_id`, at the document `heads` it
/// was read at: what an install takes (ADR 044 §2). Its heads select the revision, and holding
/// the pin is what keeps that version.
pub fn version_pin(
    doc_id: DocId,
    version: &Version,
    heads: impl IntoIterator<Item = ChangeHash>,
) -> DatumPin {
    let reference =
        DatumReference::new(ReferenceDocId::Concrete(doc_id), version_key(version)).pinned(heads);
    DatumPin::new(Location::new(reference, None).expect("a location without a fragment is valid"))
}

/// `org.example.daybook.blob/cartridge.artifact/<version>/<artifactKey>`: the [`Blob`] reference that
/// pins one artifact of one version. The artifact key may itself contain `/`; a version label
/// cannot, so the first `/` after the prefix ends the label.
pub fn artifact_key(version: &Version, artifact: &str) -> DatumKey {
    DatumKey::from_text(
        Blob::ID,
        format!("{ARTIFACT_SLOT_PREFIX}{version}/{artifact}"),
    )
    .expect("a version label and a non-empty artifact key are a valid datum key slot")
}

/// The version an [`artifact_key`] slot belongs to, `None` for any other `Blob` slot.
pub fn artifact_slot_version(key: &DatumKey) -> Option<String> {
    if key.tag().as_str() != Blob::ID {
        return None;
    }
    let slot = std::str::from_utf8(key.slot_bytes()).ok()?;
    let rest = slot.strip_prefix(ARTIFACT_SLOT_PREFIX)?;
    rest.split_once('/').map(|(version, _)| version.to_owned())
}

/// The datums the registry declares: the envelope, its two contribution members, and the
/// identity. The `Blob` Lexicon is registered by big_blobs.
#[cfg(feature = "schemars")]
pub fn registry_universe() -> big_cabinet_core::datum::Universe {
    use crate::MicroContribution;
    use crate::NoduleContribution;
    use big_cabinet_core::datum::Declaration;
    big_cabinet_core::datum::Universe::collect([
        Declaration::of::<Cartridge>().expect("Cartridge declaration is valid"),
        Declaration::of::<NoduleContribution>().expect("NoduleContribution declaration is valid"),
        Declaration::of::<MicroContribution>().expect("MicroContribution declaration is valid"),
        Declaration::of::<CartridgeIdentity>().expect("CartridgeIdentity declaration is valid"),
    ])
}
