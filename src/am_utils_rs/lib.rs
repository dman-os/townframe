mod interlude {
    pub use utils_rs::prelude::*;
}

pub mod prelude {
    pub use crate::codecs::ThroughJson;
    pub use crate::ids::{DocId32, PeerId32};

    #[cfg(feature = "repo")]
    pub use crate::repo::{BigRepo, SharedBigRepo};
    pub use automerge;
    pub use autosurgeon;
}

use crate::interlude::*;

pub mod codecs;
pub mod ids;
#[cfg(feature = "repo")]
pub mod partition;
#[cfg(feature = "repo")]
pub mod repo;
#[cfg(feature = "repo")]
pub mod sync;
#[cfg(feature = "repo")]
pub use repo::{BigDocHandle, BigRepo, BigRepoStopToken, DocumentId, SharedBigRepo};

use automerge::ChangeHash;

pub fn parse_commit_heads<S: AsRef<str>>(heads: &[S]) -> Res<Arc<[ChangeHash]>> {
    heads
        .iter()
        .map(|commit| {
            let mut buf = [0u8; 32];
            utils_rs::hash::decode_base58_multibase_onto(commit.as_ref(), &mut buf)?;
            eyre::Ok(automerge::ChangeHash(buf))
        })
        .collect()
}

pub fn serialize_commit_heads(heads: &[ChangeHash]) -> Vec<String> {
    heads
        .iter()
        .map(|commit| utils_rs::hash::encode_base58_multibase(commit.0))
        .collect()
}

#[test]
fn play() -> Res<()> {
    use automerge::transaction::Transactable;
    use automerge::ReadDoc;

    let mut doc = automerge::AutoCommit::new();
    let map = doc.put_object(automerge::ROOT, "map", automerge::ObjType::Map)?;
    let obj1 = doc.put_object(map.clone(), "foo", automerge::ObjType::Map)?;
    doc.put(obj1.clone(), "key1", 1)?;
    doc.commit();
    let commit1 = doc.get_heads();
    doc.put(obj1.clone(), "key2", 2)?;
    doc.put(obj1.clone(), "key3", 3)?;
    let obj2 = doc.put_object(map.clone(), "bar", automerge::ObjType::Map)?;
    doc.put(obj2.clone(), "key1", 1)?;
    doc.commit();
    let commit2 = doc.get_heads();

    let _patches = doc.diff(&commit1, &commit2);

    let _obj1 = doc.put_object(map.clone(), "foo", automerge::ObjType::Map)?;
    doc.commit();
    let commit3 = doc.get_heads();
    let patches = doc.diff(&commit2, &commit3);
    let json = doc.hydrate(automerge::ROOT, None)?;

    println!("{patches:#?} {json:#?}");

    Ok(())
}

#[test]
fn patch_obj_actor_is_object_creator_not_latest_change_author() -> Res<()> {
    use automerge::transaction::Transactable;

    let actor_a = automerge::ActorId::from([1_u8]);
    let actor_b = automerge::ActorId::from([2_u8]);

    let mut doc = automerge::AutoCommit::new();

    doc.set_actor(actor_a.clone());
    let map_obj = doc.put_object(automerge::ROOT, "map", automerge::ObjType::Map)?;
    doc.commit();
    let heads_after_a = doc.get_heads();

    doc.set_actor(actor_b.clone());
    doc.put(&map_obj, "k", "v")?;
    doc.commit();
    let heads_after_b = doc.get_heads();

    let patches = doc.diff(&heads_after_a, &heads_after_b);
    let patch = patches
        .iter()
        .find(|p| matches!(p.action, automerge::PatchAction::PutMap { ref key, .. } if key == "k"))
        .ok_or_else(|| eyre::eyre!("missing PutMap patch for key 'k'"))?;

    let actor_from_patch_obj = match &patch.obj {
        automerge::ObjId::Id(_, actor_id, _) => actor_id.clone(),
        automerge::ObjId::Root => eyre::bail!("missing patch actor"),
    };
    assert_eq!(actor_from_patch_obj, actor_a);
    assert_ne!(actor_from_patch_obj, actor_b);

    Ok(())
}

#[test]
fn patch_conflict_still_uses_object_lineage_actor() -> Res<()> {
    use automerge::transaction::Transactable;

    let actor_a = automerge::ActorId::from([11_u8]);
    let actor_b = automerge::ActorId::from([22_u8]);

    let mut base = automerge::AutoCommit::new();
    base.set_actor(actor_a.clone());
    let map_obj = base.put_object(automerge::ROOT, "map", automerge::ObjType::Map)?;
    base.commit();
    let old_heads = base.get_heads();

    let mut doc_a = base.fork();
    let mut doc_b = base.fork();

    doc_a.set_actor(actor_a.clone());
    doc_a.put(&map_obj, "k", "from-a")?;
    doc_a.commit();

    doc_b.set_actor(actor_b.clone());
    doc_b.put(&map_obj, "k", "from-b")?;
    doc_b.commit();

    doc_a.merge(&mut doc_b)?;

    let new_heads = doc_a.get_heads();
    let patches = doc_a.diff(&old_heads, &new_heads);

    let put_map_patch = patches
        .iter()
        .find(|p| matches!(p.action, automerge::PatchAction::PutMap { ref key, .. } if key == "k"))
        .ok_or_else(|| eyre::eyre!("missing PutMap patch for key 'k'"))?;

    let actor_from_patch_obj = match &put_map_patch.obj {
        automerge::ObjId::Id(_, actor_id, _) => actor_id.clone(),
        automerge::ObjId::Root => eyre::bail!("missing patch actor"),
    };
    assert_eq!(actor_from_patch_obj, actor_a);
    assert_ne!(actor_from_patch_obj, actor_b);

    Ok(())
}

#[test]
fn patches_are_not_one_to_one_with_changes() -> Res<()> {
    use automerge::transaction::Transactable;

    let mut doc = automerge::AutoCommit::new();
    let map_obj = doc.put_object(automerge::ROOT, "map", automerge::ObjType::Map)?;
    doc.commit();
    let old_heads = doc.get_heads();

    doc.put(&map_obj, "k", "v1")?;
    doc.commit();
    doc.put(&map_obj, "k", "v2")?;
    doc.commit();
    let new_heads = doc.get_heads();

    let changes = doc.get_changes(&old_heads);
    let patches = doc.diff(&old_heads, &new_heads);
    let put_count = patches
        .iter()
        .filter(
            |p| matches!(p.action, automerge::PatchAction::PutMap { ref key, .. } if key == "k"),
        )
        .count();

    assert_eq!(changes.len(), 2, "expected two committed changes");
    assert_eq!(
        put_count, 1,
        "diff collapsed to one resulting patch for key k"
    );

    Ok(())
}
