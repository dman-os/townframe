//! How drawer state is named in the docs that carry it: the datum keys of a drawer doc's
//! descriptor, roster and entries, and the [`GroupKey`] a descriptor names a Keyhive group by.
//!
//! A doc, child or known entry key's slot is its target's `DocId` text, so a roster read recovers
//! the target from the key alone.

use big_cabinet_core::DatumKey;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::drawer::Drawer;
use big_cabinet_core::drawer::DrawerChildEntry;
use big_cabinet_core::drawer::DrawerDocEntry;
use big_cabinet_core::drawer::DrawerMetaDoc;
use big_cabinet_core::drawer::DrawerRoster;
use big_cabinet_core::drawer::DrawerSuccessor;
use big_cabinet_core::drawer::GroupKey;
use big_cabinet_core::drawer::KnownDrawerEntry;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawersError;
use big_repo::BigKeyhiveGroup;

pub(crate) fn drawer_key() -> DatumKey {
    DatumKey::from_text(Drawer::ID, "main").expect("the drawer key is well formed")
}

pub(crate) fn known_entry_key(drawer_id: &DrawerId) -> DatumKey {
    DatumKey::from_text(KnownDrawerEntry::ID, drawer_id.to_string())
        .expect("a known entry key is well formed")
}

pub(crate) fn roster_key() -> DatumKey {
    DatumKey::from_text(DrawerRoster::ID, "main").expect("the roster key is well formed")
}

/// The fixed singleton successor slot on a descriptor-bearing doc.
pub(crate) fn successor_key() -> DatumKey {
    DatumKey::from_text(DrawerSuccessor::ID, "main").expect("the successor key is well formed")
}

/// The slot a grouped drawer's metadata doc carries one `(kind, name)` reference under:
/// `metaDoc/<kind>/<name>`. Both parts come from outside, so an empty part, a `/` inside one,
/// or a dot segment is a refusal rather than a panic — those spellings would make the slot
/// ambiguous when a projection reads it back.
pub(crate) fn meta_doc_key(kind: &str, name: &str) -> Result<DatumKey, DrawersError> {
    let slot = format!("{kind}/{name}");
    if kind.is_empty() || kind.contains('/') || kind == "." || kind == ".." {
        return Err(DrawersError::InvalidMetaDocSlot(slot.clone()));
    }
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        return Err(DrawersError::InvalidMetaDocSlot(slot.clone()));
    }
    DatumKey::from_text(DrawerMetaDoc::ID, slot.clone())
        .map_err(|_| DrawersError::InvalidMetaDocSlot(slot))
}

/// The `(kind, name)` a `metaDoc` key slot names, `None` when the slot is not one this build
/// can read back into its two parts.
pub(crate) fn meta_doc_slot(key: &DatumKey) -> Option<(String, String)> {
    let slot = std::str::from_utf8(key.slot_bytes()).ok()?;
    let (kind, name) = slot.split_once('/')?;
    if kind.is_empty() || name.is_empty() || kind.contains('/') || name.contains('/') {
        return None;
    }
    Some((kind.to_owned(), name.to_owned()))
}

pub(crate) fn doc_entry_key(doc: &DocId) -> DatumKey {
    DatumKey::from_text(DrawerDocEntry::ID, doc.to_string())
        .expect("a doc entry key is well formed")
}

pub(crate) fn child_entry_key(child: &DrawerId) -> DatumKey {
    DatumKey::from_text(DrawerChildEntry::ID, child.to_string())
        .expect("a child entry key is well formed")
}

/// The target of an entry key: `docEntry/<id>` or `childEntry/<id>` names it in the key slot.
pub(crate) fn entry_target(key: &DatumKey) -> Option<DocId> {
    std::str::from_utf8(key.slot_bytes()).ok()?.parse().ok()
}

pub(crate) fn group_key(group: &BigKeyhiveGroup) -> GroupKey {
    GroupKey(group.id().to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_keys_name_their_target() {
        let doc = DocId::new([9; 32]);
        assert_eq!(entry_target(&doc_entry_key(&doc)), Some(doc.clone()));
        assert_eq!(entry_target(&child_entry_key(&doc)), Some(doc));
        assert_eq!(entry_target(&drawer_key()), None);
    }
}
