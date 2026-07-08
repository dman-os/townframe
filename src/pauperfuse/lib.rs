#![expect(unused)]
/*

Rough ideas
- ppfuse manages bidirection change application across file trees
- use a in store only vrtual tree to track the managed application of state
    - diff it with the other trees to detect changes
    - store full diff history for a jj op log like experience
- watch mode
    - fast track tree changes resolution across trees
- late materilization
    - abstractions that allow trees to defer materilization of a previous version on demand
- git livetree
    - trees
        - daybook tree
        - last applied tree
        - real file tree
- wasi plugin tree
    - trees
        - daybook tree
        - wasi tree
- operations
    - get_diff
    - pull changes from tree to store
    - apply changes to tree
- usecases
    - git checkout
        - pull changes from daybook
        - pull changes from fs
        - assert fs empty
        - create empty vtree
        - apply change from daybook to (fs, vtree)
    - git commit
        - pull changes from fs
        - apply change from fs to (daybook, vtree)
        - apply change from daybook to vtree
     - obsidian sync
        - pull changes from fs
        - apply changes from fs to (daybook, vtree)
        - apply change from daybook to vtree
*/

mod interlude {
    pub use std::collections::{BTreeMap, VecDeque};
    pub use std::path::{Path, PathBuf};
    pub use utils_rs::prelude::*;
}

mod livetree;

use futures::future::BoxFuture;
use surelock::{key::lock_scope, mutex::Mutex};

use crate::interlude::*;

#[tokio::test]
async fn smoke() -> Res<()> {
    let dir = tempfile::tempdir()?;
    let config = Config {
        managed_dir: dir.path().to_path_buf(),
        metastore_dir_path: dir.path().join(".db"),
    };
    let store = ErasedVtreeStore::new(Arc::new(MemVtreeStore {
        trees: Arc::new(surelock::mutex::Mutex::new(default())),
    }));
    let ctx = Ctx {
        store: Arc::clone(&store),
        trees: default(),
    };

    struct TestSource {}
    struct FsTarget {}
    let src = TestSource {};
    let tar = FsTarget {};
    pull_changes_to_store(&store).await;

    Ok(())
}

pub struct Config {
    managed_dir: PathBuf,
    metastore_dir_path: PathBuf,
}

pub struct Ctx {
    store: Arc<ErasedVtreeStore>,
    trees: HashMap<TreeId, Arc<dyn Tree>>,
}

impl Ctx {
    fn new() {}
}

trait VtreeStore: Send + Sync {
    type Txn: Send + Sync + 'static;
    async fn start_txn(&self) -> Self::Txn;
    async fn commit_txn(&self, txn: Self::Txn);
    async fn update_file(&self, txn: &Self::Txn, tid: TreeId, fid: FileId, meta: FileMeta);
    async fn remove_file(&self, txn: &Self::Txn, tid: TreeId, fid: FileId);
    async fn diff(&self, from: TreeId, to: TreeId);
}

struct ErasedVtreeStore {
    start_txn_cb: Box<dyn Fn() -> BoxFuture<'static, ErasedTxn> + Send + Sync>,
    commit_txn_cb: Box<dyn Fn(ErasedTxn) -> BoxFuture<'static, ()> + Send + Sync>,
    update_file_cb:
        Box<dyn Fn(&ErasedTxn, TreeId, FileId, FileMeta) -> BoxFuture<'static, ()> + Send + Sync>,
    remove_file_cb: Box<dyn Fn(&ErasedTxn, TreeId, FileId) -> BoxFuture<'static, ()> + Send + Sync>,
    diff_cb: Box<dyn Fn(TreeId, TreeId) -> BoxFuture<'static, ()> + Send + Sync>,
}
struct ErasedTxn(Arc<dyn std::any::Any + Send + Sync>);

impl ErasedVtreeStore {
    fn new<S, T>(store: Arc<S>) -> Arc<Self>
    where
        S: VtreeStore<Txn = T>,
        T: std::any::Any + Send + Sync,
    {
        Arc::new(Self {
            start_txn_cb: {
                let store = store.clone();
                Box::new(|| {
                    async move {
                        let txn = store.start_txn().await;
                        ErasedTxn(Arc::new(txn) as _)
                    }
                    .boxed()
                })
            },
            commit_txn_cb: {
                let store = store.clone();
                Box::new(|txn| {
                    async move {
                        let txn = Arc::try_unwrap(txn.0).expect(ERROR_IMPOSSIBLE);
                        store.commit_txn(txn).await;
                    }
                    .boxed()
                })
            },
            update_file_cb: {
                let store = store.clone();
                Box::new(|txn, tid, fid, meta| {
                    async move {
                        let txn = Arc::downcast(Arc::clone(&txn.0)).expect("wrong txn");
                        store.update_file(&txn, tid, fid, meta).await;
                    }
                    .boxed()
                })
            },
            remove_file_cb: {
                let store = store.clone();
                Box::new(|txn, tid, fid| {
                    async move {
                        let txn = Arc::downcast(Arc::clone(&txn.0)).expect("wrong txn");
                        store.remove_file(&txn, tid, fid).await;
                    }
                    .boxed()
                })
            },
            diff_cb: {
                let store = store.clone();
                Box::new(|from, to| {
                    async move {
                        store.diff(from, to).await;
                    }
                    .boxed()
                })
            },
        })
    }
    async fn start_txn(&self) -> ErasedTxn {
        (self.start_txn_cb)().await
    }
    async fn commit_txn(&self, txn: ErasedTxn) {
        (self.commit_txn_cb)(txn).await
    }
    async fn update_file(&self, txn: &ErasedTxn, tid: TreeId, fid: FileId, meta: FileMeta) {
        (self.update_file_cb)(txn, tid, fid, meta).await
    }
    async fn remove_file(&self, txn: &ErasedTxn, tid: TreeId, fid: FileId) {
        (self.remove_file_cb)(txn, tid, fid).await
    }
    async fn diff(&self, from: TreeId, to: TreeId) {
        (self.diff_cb)(from, to).await
    }
}

strike! {
    pub struct TreeEvent {
        fid: FileId,
        cursor: CursorIndex,
        deets: enum TreeEventDeets {
            FileCreated { meta: FileMeta },
            FileChanged { meta: FileMeta },
            FileRemoved
        }
    }
}

type ArcTree = Arc<dyn Tree>;
#[async_trait]
pub trait Tree: Send + Sync {
    async fn get_updates(&self) -> Vec<TreeEvent>;
}

async fn pull_changes_to_store(store: &ErasedVtreeStore, tid: TreeId, tree: ArcTree) {
    let txn = store.start_txn().await;
    for evt in tree.get_updates().await {
        match evt.deets {
            TreeEventDeets::FileChanged { meta } | TreeEventDeets::FileCreated { meta } => {
                store.update_file(&txn, tid, evt.fid, meta).await
            }
            TreeEventDeets::FileRemoved => store.remove_file(&txn, tid, evt.fid).await,
        }
    }
}

// async fn update_stores(ctx: &Ctx) {
//     use futures_buffered::BufferedStreamExt;
//     futures::stream::iter(ctx.trees.iter().map({
//         |(&tid, tree)| {
//             let store = Arc::clone(&ctx.store);
//             async move {}
//         }
//     }))
//     .buffered_unordered(16)
//     .collect::<Vec<()>>()
//     .await;
// }

strike! {
    struct MemVtreeStore {
        trees: Arc<Mutex<
            HashMap<
                TreeId,
                Arc<Mutex<
                    struct MemVtreeState {
                        #![derive(Default)]

                        files: HashMap<
                            FileId,
                            FileMeta,
                        >
                    }
                >>
            >
        >>
    }
}

impl MemVtreeStore {
    fn get_tree(&self, tid: TreeId) -> Arc<Mutex<MemVtreeState>> {
        lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.trees);
            guard
                .entry(tid)
                .or_insert_with(|| Arc::new(Mutex::new(default())))
                .clone()
        })
    }
}

impl VtreeStore for MemVtreeStore {
    type Txn = ();
    async fn start_txn(&self) -> Self::Txn {
        // no op
    }
    async fn commit_txn(&self, _txn: Self::Txn) {
        // no op
    }
    async fn update_file(&self, _txn: &Self::Txn, tid: TreeId, fid: FileId, meta: FileMeta) {
        let tree = self.get_tree(tid);
        lock_scope(|key| {
            let (mut guard, _key) = key.lock(&tree);
            guard.files.insert(fid, meta);
        })
    }

    async fn remove_file(&self, _txn: &Self::Txn, tid: TreeId, fid: FileId) {
        let tree = self.get_tree(tid);
        lock_scope(|key| {
            let (mut guard, _key) = key.lock(&tree);
            guard.files.remove(&fid);
        })
    }

    async fn diff(&self, from: TreeId, to: TreeId) {
        let from = self.get_tree(from);
        let to = self.get_tree(to);
        let lock_set = surelock::set::LockSet::new((&from, &to));
        lock_scope(|key| {
            let ((mut from, mut to), _key) = key.lock(&lock_set);
            let mut diff = HashMap::new();
            for (fid, from_meta) in &from.files {
                let Some(to_meta) = to.files.get(&fid) else {
                    diff.insert(
                        fid,
                        VtreeFileDiff {
                            from: Some(from_meta.clone()),
                            to: None,
                        },
                    );
                    continue;
                };
                diff.insert(
                    fid,
                    VtreeFileDiff {
                        from: Some(from_meta.clone()),
                        to: Some(to_meta.clone()),
                    },
                );
            }
            for (fid, to_meta) in &to.files {
                if diff.contains_key(&fid) {
                    continue;
                }
                diff.insert(
                    fid,
                    VtreeFileDiff {
                        from: None,
                        to: Some(to_meta.clone()),
                    },
                );
            }
        })
    }
}

struct VtreeFileDiff {
    from: Option<FileMeta>,
    to: Option<FileMeta>,
}

pub async fn get_diff(ctx: &Ctx) {}
pub async fn update_source() {}
pub async fn update_target() {}

async fn tick() {
    let src_store = MemorySourceStore::default();
    let sid = 0;
    let src_prov = TestSourceProvider {};
    let tgt_backend = FsTargetBackend {};
    // collect the changes from the provider and store them
    // in the index
    {
        let mut last_cursor = src_store.get_cursor(sid).await;
        loop {
            let src_events = src_prov.get_new_events(last_cursor.clone()).await;
            for evt in src_events {
                match evt {
                    SourceProviderEvent::FileCreated { id, file }
                    | SourceProviderEvent::FileChanged { id, file } => {
                        src_store.update_vfile(sid, id, file).await
                    }
                    SourceProviderEvent::FileRemoved { id } => {
                        src_store.delmark_vfile(sid, id).await
                    }
                }
            }
        }
    }
    {}
}

type FileId = Uuid;

#[derive(Clone)]
struct FileMeta {}
type TreeId = Uuid;

type CursorIndex = u64;

trait IndexStore {}

use source::*;
mod source {
    use crate::interlude::*;

    use crate::*;

    pub type SourceId = u32;

    pub enum SourceProviderEvent {
        FileCreated { id: FileId, file: FileMeta },
        FileChanged { id: FileId, file: FileMeta },
        FileRemoved { id: FileId },
    }

    pub trait SourceProvider {
        async fn get_new_events(&self, cursor: Option<CursorIndex>) -> Vec<SourceProviderEvent>;
    }

    #[derive(Default)]
    pub struct TestSourceProvider {}

    pub trait SourceStore {
        async fn get_cursor(&self, id: SourceId) -> Option<CursorIndex>;
        async fn update_vfile(&self, sid: SourceId, fid: FileId, file: FileMeta);
        async fn delmark_vfile(&self, sid: SourceId, fid: FileId);
        async fn remove_provider_vfile(&self, sid: SourceId, fid: FileId);
    }

    #[derive(Default)]
    pub struct MemorySourceStore {
        provider_cursors: HashMap<SourceId, CursorIndex>,
    }

    impl SourceStore for MemorySourceStore {
        async fn get_cursor(&self, id: SourceId) -> Option<CursorIndex> {
            self.provider_cursors.get(&id).cloned()
        }
    }

    impl SourceProvider for TestSourceProvider {
        async fn get_new_events(&self, cursor: Option<CursorIndex>) -> Vec<SourceProviderEvent> {
            todo!()
        }
    }
}

use target::*;
mod target {
    use crate::interlude::*;

    use crate::*;

    enum TargetBackendEvent {
        FileCreated { id: FileId, file: FileMeta },
        FileChanged { id: FileId, file: FileMeta },
        FileRemoved { id: FileId },
    }

    pub trait TargetBackend {
        fn get_backend_changes() -> Vec<TargetBackendEvent>;
    }

    pub struct FsTargetBackend {}
    impl TargetBackend for FsTargetBackend {}
}
