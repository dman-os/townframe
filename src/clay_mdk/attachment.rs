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
///
/// On the wire a row is CBOR and the attachment is the tagged index. In a human-readable
/// format (the JSON payload of a manifest's declared row) there are no tags, so it is written
/// as the one-member object `{"$attachment": n}`, which `declared_rows` turns back into the tag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Attachment(ciborium::tag::Required<u64, CAPABILITY_ATTACHMENT_TAG>);

/// The human-readable form of an [`Attachment`].
#[derive(Serialize, Deserialize)]
struct HumanForm {
    #[serde(rename = "$attachment")]
    index: u64,
}

impl Serialize for Attachment {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanForm { index: self.0.0 }.serialize(serializer)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Attachment {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            Ok(Self::at(HumanForm::deserialize(deserializer)?.index))
        } else {
            Ok(Self(Deserialize::deserialize(deserializer)?))
        }
    }
}

impl Attachment {
    pub fn at(index: u64) -> Self {
        Self(ciborium::tag::Required(index))
    }

    pub fn index(self) -> usize {
        usize::try_from(self.0.0).expect("attachment index fits in memory")
    }
}
