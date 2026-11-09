//! What a contract crate needs to describe row values without linking a backend: a value names
//! one of its row's attachments by index under a reserved CBOR tag. Manifest crates (a
//! micro's declared rows) use this, and so do both backends when they mint the attachment into
//! a recipient-local [`crate::Capability`].
use serde::Deserialize;
use serde::Serialize;

/// The CBOR tag a value uses to name one of its row's attachments by index. Equal to
/// `clay_guest_session::collections::CAPABILITY_ATTACHMENT_TAG`, which the in-process backend
/// asserts at compile time.
pub const CAPABILITY_ATTACHMENT_TAG: u64 = 43000;

/// A value's reference to one of its row's attachments, written under the reserved CBOR tag.
/// The body alone conveys no authority: the index names an entry of the row's attachment list,
/// which the host mints into a recipient-local capability on read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Attachment(ciborium::tag::Required<u64, CAPABILITY_ATTACHMENT_TAG>);

impl Attachment {
    pub fn at(index: u64) -> Self {
        Self(ciborium::tag::Required(index))
    }

    pub fn index(self) -> usize {
        usize::try_from(self.0.0).expect("attachment index fits in memory")
    }
}
