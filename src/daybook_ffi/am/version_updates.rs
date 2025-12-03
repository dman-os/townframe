use crate::interlude::*;

use automerge::{transaction::Transactable, ActorId, AutoCommit, ROOT};
use autosurgeon::reconcile_prop;

pub mod app {
    use super::*;

    use daybook_core::config::ConfigStore;
    use daybook_core::tables::TablesStore;

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        // annotate schema for app document
        doc.put(ROOT, "$schema", "daybook.app")?;
        reconcile_prop(&mut doc, ROOT, TablesStore::PROP, TablesStore::default())?;
        reconcile_prop(&mut doc, ROOT, ConfigStore::PROP, ConfigStore::default())?;
        Ok(doc.save_nocompress())
    }
}
