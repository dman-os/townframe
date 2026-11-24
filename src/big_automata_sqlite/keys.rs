//! Keys as SQLite blobs.
//!
//! `big_automata`'s keys order by their derived `Ord`, which is **not** byte order: a key of up
//! to 32 bytes is stored inline and compares length first, then bytes; a longer key compares
//! after every short one, bytewise. The log's page order, a part's `ObjState` order and every
//! `ORDER BY` below must agree with that order, so a key is stored as a blob whose memcmp order
//! is the key's order:
//!
//! - up to 32 bytes: `0x00`, the length, the bytes zero-padded to 32;
//! - longer: `0x01`, the bytes.

use crate::interlude::*;

/// The memcmp-ordered encoding of a key's bytes.
fn encode(bytes: &[u8]) -> Vec<u8> {
    if bytes.len() <= 32 {
        let mut blob = Vec::with_capacity(34);
        blob.push(0);
        blob.push(bytes.len() as u8);
        blob.extend_from_slice(bytes);
        blob.resize(34, 0);
        blob
    } else {
        let mut blob = Vec::with_capacity(1 + bytes.len());
        blob.push(1);
        blob.extend_from_slice(bytes);
        blob
    }
}

fn decode(blob: &[u8]) -> &[u8] {
    match blob[0] {
        0 => &blob[2..2 + usize::from(blob[1])],
        1 => &blob[1..],
        tag => unreachable!("key blob with tag {tag}"),
    }
}

/// A key type stored as an ordered blob.
pub trait BlobKey: Sized {
    fn blob(&self) -> Vec<u8>;
    /// Keys in the database were written by [`blob`](Self::blob); anything else is corruption.
    fn from_blob(blob: &[u8]) -> Self;
}

macro_rules! blob_key {
    ($($ty:ty),+) => {$(
        impl BlobKey for $ty {
            fn blob(&self) -> Vec<u8> {
                encode(self.as_bytes())
            }
            fn from_blob(blob: &[u8]) -> Self {
                Self::new(decode(blob))
            }
        }
    )+};
}
blob_key!(big_automata::ObjKey, big_automata::PartKey, big_automata::PeerKey);

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use big_automata::ObjKey;

    proptest! {
        /// The blob order is the key order, across the inline/heap boundary and for
        /// length-first inline keys; and a blob decodes to its key.
        #[test]
        fn blob_order_is_key_order(
            left in proptest::collection::vec(any::<u8>(), 0..40),
            right in proptest::collection::vec(any::<u8>(), 0..40),
        ) {
            let (left, right) = (ObjKey::new(&left), ObjKey::new(&right));
            prop_assert_eq!(left.blob().cmp(&right.blob()), left.cmp(&right));
            prop_assert_eq!(ObjKey::from_blob(&left.blob()), left);
        }
    }

    #[test]
    fn short_keys_order_length_first() {
        assert!(ObjKey::new(b"b").blob() < ObjKey::new(b"aa").blob());
        assert!(ObjKey::new(&[9; 32]).blob() < ObjKey::new(&[0; 33]).blob());
    }
}
