use crate::interlude::*;

use automerge::{transaction::Transactable, ActorId, AutoCommit, ROOT};
use autosurgeon::reconcile_prop;

pub mod app {
    use super::*;

    use daybook_core::tables::TablesStore;

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        // annotate schema for app document
        doc.put(ROOT, "$schema", "daybook.app")?;
        reconcile_prop(&mut doc, ROOT, TablesStore::PROP, TablesStore::default())?;
        Ok(doc.save_nocompress())
    }
}

pub mod drawer {
    use super::*;

    use daybook_core::drawer::DrawerStore;

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        // indicate schema type for this document
        doc.put(ROOT, "$schema", "daybook.drawer")?;
        reconcile_prop(&mut doc, ROOT, DrawerStore::PROP, DrawerStore::default())?;
        Ok(doc.save_nocompress())
    }
}
