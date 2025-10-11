use crate::interlude::*;

use automerge::{transaction::Transactable, ActorId, AutoCommit, ROOT};
use autosurgeon::reconcile_prop;

pub mod app {
    use super::*;

    use crate::tables::TablesAm;

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        reconcile_prop(&mut doc, ROOT, TablesAm::PROP, TablesAm::default())?;
        Ok(doc.save_nocompress())
    }
}

pub mod drawer {
    use crate::docs::DrawerAm;

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        reconcile_prop(&mut doc, ROOT, DrawerAm::PROP, DrawerAm::default())?;
        Ok(doc.save_nocompress())
    }
    use super::*;
}
