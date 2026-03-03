use std::fs::FileType;

use crate::interlude::*;

use crate::*;

use filetime::FileTime;

pub struct LivetreeBackend {
    root_path: PathBuf,
    state: LivetreeBackendState,
}

impl LivetreeBackend {
    async fn load(root_path: PathBuf, state: LivetreeBackendState) -> Res<Self> {
        Ok(Self { root_path, state })
    }
}

#[async_trait]
impl Backend for LivetreeBackend {
    async fn reconcile(
        &self,
        cx: &Ctx,
        effects: &[BackendEffect],
        report: &mut BackendReconcileReport,
    ) -> Res<()> {
        for fx in effects {
            match fx {
                BackendEffect::SetFile {
                    id,
                    provider_id,
                    relative_path,
                } => {
                    //
                    match tokio::fs::metadata(self.root_path.join(relative_path)).await {
                        Ok(meta) => {}
                        Err(err) => todo!(),
                    }
                }
                BackendEffect::RemoveFile { id } => {}
            }
        }
        Ok(())
    }
}

async fn diff(root_path: PathBuf, state: Arc<LivetreeBackendState>) -> Res<()> {
    struct Entry {
        file_name: String,
        parent_path: Arc<Path>,
        meta: EntryMeta,
    }
    let (diff_tx, mut diff_rx) = tokio::sync::mpsc::channel(512);

    let mut entry_txes = vec![];
    let mut diff_worker_handles = vec![];
    for ii in 0..8 {
        let (entry_tx, mut entry_rx) = tokio::sync::mpsc::unbounded_channel::<Entry>();
        let fut = {
            let state = state.clone();
            let diff_tx = diff_tx.clone();
            async move {
                while let Some(entry) = entry_rx.recv().await {
                    let diff = if let Some(old) = state
                        .get_vfile(&entry.parent_path, &entry.file_name)
                        .await?
                    {
                        let diff = EntryMetaDiff::diff(&old.meta, &entry.meta);
                        if !diff.is_empty() {
                            FileDiff::FileChanged { id: old.id, diff }
                        } else {
                            FileDiff::NoChange { id: old.id }
                        }
                    } else {
                        FileDiff::NewFile
                    };
                    diff_tx.send((entry, diff)).await.wrap_err(ERROR_ACTOR);
                }
                eyre::Ok(())
            }
        };
        let handle = tokio::spawn(fut);
        entry_txes.push(entry_tx);
        diff_worker_handles.push(handle);
    }

    let walker = tokio::task::spawn_blocking(move || {
        let mut last_worker_ii = 0;
        for entry in jwalk::WalkDir::new(root_path.clone()).sort(true) {
            let entry = match entry {
                Ok(val) => val,
                Err(err) => {
                    warn!(?err, "entry error walking through livetree");
                    continue;
                }
            };
            // FIXME: I wonder if we can improve the perf using
            // async/io for the metadata syscalls.
            // Or was it true that tokio is actually using spawn_blocking
            // for it's fs impl?
            let meta = match entry.metadata() {
                Ok(val) => val,
                Err(err) => {
                    warn!(?err, path = ?entry.parent_path, name = ?entry.file_name, "metadata error walking through livetree");
                    continue;
                }
            };
            let entry = Entry {
                file_name: entry.file_name.to_string_lossy().to_string(),
                parent_path: entry.parent_path.clone(),
                meta: EntryMeta {
                    ftype: meta.file_type(),
                    len: meta.len(),
                    mtime: FileTime::from_last_modification_time(&meta),
                    atime: FileTime::from_last_access_time(&meta),
                    ctime: FileTime::from_creation_time(&meta),
                },
            };
            entry_txes[last_worker_ii % entry_txes.len()]
                .send(entry)
                .expect(ERROR_ACTOR);
            last_worker_ii += 1;
        }
        Ok(())
    });

    let mut seen_entries = vec![];
    while let Some((entry, diff)) = diff_rx.recv().await {
        match diff {
            FileDiff::NewFile => todo!(),
            FileDiff::NoChange { id } => {
                //
                seen_entries.push(id);
            }
            FileDiff::FileChanged { id, diff } => {
                //
                seen_entries.push(id);
            }
        }
    }
    // #[derive(Default)]
    // struct Dir {
    //     /// NOTE: is sorted
    //     children: Vec<FsEntry>,
    // }
    // let mut dirs: HashMap<Arc<Path>, Dir> = default();

    walker.await.wrap_err("error walking tree")?
}

struct EntryMeta {
    // meta
    len: u64,
    mtime: FileTime,
    ctime: Option<FileTime>,
    atime: FileTime,
    ftype: FileType,
}

struct EntryMetaDiff {
    // meta
    len: Option<(u64, u64)>,
    mtime: Option<(FileTime, FileTime)>,
    ctime: Option<(Option<FileTime>, Option<FileTime>)>,
    atime: Option<(FileTime, FileTime)>,
    ftype: Option<(FileType, FileType)>,
}

impl EntryMetaDiff {
    fn is_empty(&self) -> bool {
        !(self.len.is_some()
            || self.mtime.is_some()
            || self.ctime.is_some()
            || self.atime.is_some()
            || self.ftype.is_some())
    }
    fn diff(from: &EntryMeta, to: &EntryMeta) -> EntryMetaDiff {
        EntryMetaDiff {
            len: if from.len != to.len {
                Some((from.len, to.len))
            } else {
                None
            },
            mtime: if from.mtime != to.mtime {
                Some((from.mtime, to.mtime))
            } else {
                None
            },
            ctime: if from.ctime != to.ctime {
                Some((from.ctime, to.ctime))
            } else {
                None
            },
            atime: if from.atime != to.atime {
                Some((from.atime, to.atime))
            } else {
                None
            },
            ftype: if from.ftype != to.ftype {
                Some((from.ftype, to.ftype))
            } else {
                None
            },
        }
    }
}

enum FileDiff {
    NewFile,
    NoChange { id: VFileId },
    FileChanged { id: VFileId, diff: EntryMetaDiff },
}
struct VFileSnap {
    id: VFileId,
    meta: EntryMeta,
}

pub enum LivetreeBackendState {
    Memory {
        vfiles: HashMap<PathBuf, Arc<VFileSnap>>,
    },
}

impl LivetreeBackendState {
    async fn get_vfile(&self, parent_path: &Path, file_name: &str) -> Res<Option<Arc<VFileSnap>>> {
        Ok(match self {
            LivetreeBackendState::Memory { vfiles } => {
                vfiles.get(&parent_path.join(file_name)).cloned()
            }
        })
    }
}
