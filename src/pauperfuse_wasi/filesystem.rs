use crate::interlude::*;
use wasmtime::component::{HasSelf, Linker, Resource};

use crate::bindings::wasi::filesystem::{preopens, types::*};

/// Descriptor reads may return at most this many bytes, even for larger guest requests.
pub const MAX_READ_BYTES: usize = 65_536;
const MAX_DESCRIPTORS: usize = 4096;

pub struct DescriptorToken;
pub struct DirectoryToken;

enum OpenDescriptor<R> {
    Directory(RelPath),
    File {
        path: RelPath,
        handle: FileHandle<R>,
        size: Option<u64>,
    },
}

/// Most recent producer failure, with exact selected source and original cause.
#[derive(Debug)]
pub struct SourceFailure {
    pub source: Source,
    pub cause: Box<dyn StdError + Send + Sync>,
}

/// Owns a mounted namespace and live readers for an isolated component store.
/// The mount must explicitly describe its directory root and ancestors. Paths
/// use canonical escaped components, without `.`/`..` or symlink traversal.
/// Up to 4096 descriptors are retained; source diagnostics keep only the latest failure.
pub struct Filesystem<A: ByteAccess> {
    view: VirtualFs,
    access: A,
    descriptors: BTreeMap<u32, OpenDescriptor<A::Reader>>,
    next: u32,
    failure: Option<SourceFailure>,
}

impl<A: ByteAccess> Filesystem<A> {
    pub fn new(view: VirtualFs, access: A) -> Self {
        Self {
            view,
            access,
            descriptors: BTreeMap::new(),
            next: 0,
            failure: None,
        }
    }

    pub fn take_source_failure(&mut self) -> Option<SourceFailure> {
        self.failure.take()
    }

    pub fn live_descriptors(&self) -> usize {
        self.descriptors.len()
    }

    fn insert(
        &mut self,
        descriptor: OpenDescriptor<A::Reader>,
    ) -> Result<Resource<DescriptorToken>, ErrorCode> {
        if self.descriptors.len() >= MAX_DESCRIPTORS {
            return Err(ErrorCode::InsufficientMemory);
        }
        let id = self.next;
        self.next = id.checked_add(1).ok_or(ErrorCode::InsufficientMemory)?;
        assert!(self.descriptors.insert(id, descriptor).is_none());
        Ok(Resource::new_own(id))
    }

    fn resolve(
        &self,
        descriptor: &Resource<DescriptorToken>,
        key: &str,
    ) -> Result<RelPath, ErrorCode> {
        let base = match self.descriptors.get(&descriptor.rep()) {
            Some(OpenDescriptor::Directory(path)) => path,
            Some(OpenDescriptor::File { .. }) => return Err(ErrorCode::NotDirectory),
            None => return Err(ErrorCode::BadDescriptor),
        };
        let relative = RelPath::parse(key).map_err(|_| ErrorCode::NotPermitted)?;
        let path = RelPath::try_new(
            base.components()
                .iter()
                .chain(relative.components())
                .cloned()
                .collect(),
        )
        .expect("base and relative components are validated");
        for depth in 0..path.len() {
            if !matches!(
                self.view.metadata(&path.ancestor(depth).unwrap()),
                Some(Description::Directory)
            ) {
                return Err(ErrorCode::NotDirectory);
            }
        }
        Ok(path)
    }
}

impl<A> Filesystem<A>
where
    A: ByteAccess + Send + Sync + 'static,
    A::Reader: Send + 'static,
    A::Error: Send + Sync + 'static,
    <A::Reader as ByteReader>::Error: Send + Sync + 'static,
{
    pub fn add_to_linker(linker: &mut Linker<Self>) -> wasmtime::Result<()> {
        crate::bindings::wasi::filesystem::types::add_to_linker::<_, HasSelf<_>>(
            linker,
            |state| state,
        )?;
        preopens::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
    }
}

impl<A> HostDescriptor for Filesystem<A>
where
    A: ByteAccess + Send + Sync + 'static,
    A::Reader: Send + 'static,
    A::Error: Send + Sync + 'static,
    <A::Reader as ByteReader>::Error: Send + Sync + 'static,
{
    async fn read_via_stream(
        &mut self,
        _self_: Resource<Descriptor>,
        _offset: Filesize,
    ) -> wasmtime::Result<Result<Resource<InputStream>, ErrorCode>> {
        Ok(Err(ErrorCode::Unsupported))
    }
    async fn write_via_stream(
        &mut self,
        _self_: Resource<Descriptor>,
        _offset: Filesize,
    ) -> wasmtime::Result<Result<Resource<OutputStream>, ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn append_via_stream(
        &mut self,
        _self_: Resource<Descriptor>,
    ) -> wasmtime::Result<Result<Resource<OutputStream>, ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn advise(
        &mut self,
        _self_: Resource<Descriptor>,
        _offset: Filesize,
        _length: Filesize,
        _advice: Advice,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::Unsupported))
    }
    async fn sync_data(
        &mut self,
        _self_: Resource<Descriptor>,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::Unsupported))
    }
    async fn get_flags(
        &mut self,
        descriptor: Resource<Descriptor>,
    ) -> wasmtime::Result<Result<DescriptorFlags, ErrorCode>> {
        Ok(if self.descriptors.contains_key(&descriptor.rep()) {
            Ok(DescriptorFlags::READ)
        } else {
            Err(ErrorCode::BadDescriptor)
        })
    }
    async fn get_type(
        &mut self,
        descriptor: Resource<Descriptor>,
    ) -> wasmtime::Result<Result<DescriptorType, ErrorCode>> {
        Ok(match self.descriptors.get(&descriptor.rep()) {
            Some(OpenDescriptor::Directory(_)) => Ok(DescriptorType::Directory),
            Some(OpenDescriptor::File { .. }) => Ok(DescriptorType::RegularFile),
            None => Err(ErrorCode::BadDescriptor),
        })
    }
    async fn set_size(
        &mut self,
        _self_: Resource<Descriptor>,
        _size: Filesize,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn set_times(
        &mut self,
        _self_: Resource<Descriptor>,
        _data_access_timestamp: NewTimestamp,
        _data_modification_timestamp: NewTimestamp,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn read(
        &mut self,
        descriptor: Resource<Descriptor>,
        length: Filesize,
        offset: Filesize,
    ) -> wasmtime::Result<Result<(Vec<u8>, bool), ErrorCode>> {
        let (handle, size) = match self.descriptors.get_mut(&descriptor.rep()) {
            Some(OpenDescriptor::File { handle, size, .. }) => (handle, *size),
            Some(OpenDescriptor::Directory(_)) => return Ok(Err(ErrorCode::IsDirectory)),
            None => return Ok(Err(ErrorCode::BadDescriptor)),
        };
        let count = usize::try_from(length.min(MAX_READ_BYTES as u64)).unwrap();
        let mut bytes = vec![0; count];
        let count = match handle.read_at(offset, &mut bytes).await {
            Ok(count) => count,
            Err(cause) => {
                self.failure = Some(SourceFailure {
                    source: handle.source().clone(),
                    cause: Box::new(cause),
                });
                return Ok(Err(ErrorCode::Io));
            }
        };
        bytes.truncate(count);
        // Short nonempty reads are not EOF. Known metadata can prove the endpoint;
        // otherwise only a zero result from a nonempty request establishes EOF.
        let eof = size.is_some_and(|size| offset.saturating_add(count as u64) >= size)
            || (length != 0 && count == 0);
        Ok(Ok((bytes, eof)))
    }
    async fn write(
        &mut self,
        _self_: Resource<Descriptor>,
        _buffer: Vec<u8>,
        _offset: Filesize,
    ) -> wasmtime::Result<Result<Filesize, ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn read_directory(
        &mut self,
        _self_: Resource<Descriptor>,
    ) -> wasmtime::Result<Result<Resource<DirectoryEntryStream>, ErrorCode>> {
        Ok(Err(ErrorCode::Unsupported))
    }
    async fn sync(
        &mut self,
        _self_: Resource<Descriptor>,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::Unsupported))
    }
    async fn create_directory_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _path: String,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn stat(
        &mut self,
        descriptor: Resource<Descriptor>,
    ) -> wasmtime::Result<Result<DescriptorStat, ErrorCode>> {
        Ok(match self.descriptors.get(&descriptor.rep()) {
            Some(OpenDescriptor::Directory(_)) => Ok(stat(DescriptorType::Directory, 0)),
            Some(OpenDescriptor::File {
                size: Some(size), ..
            }) => Ok(stat(DescriptorType::RegularFile, *size)),
            Some(OpenDescriptor::File { size: None, .. }) => Err(ErrorCode::Unsupported),
            None => Err(ErrorCode::BadDescriptor),
        })
    }
    async fn stat_at(
        &mut self,
        descriptor: Resource<Descriptor>,
        path_flags: PathFlags,
        key: String,
    ) -> wasmtime::Result<Result<DescriptorStat, ErrorCode>> {
        if path_flags != PathFlags::empty() {
            return Ok(Err(ErrorCode::Unsupported));
        }
        let path = match self.resolve(&descriptor, &key) {
            Ok(path) => path,
            Err(error) => return Ok(Err(error)),
        };
        Ok(match self.view.metadata(&path) {
            Some(Description::Directory) => Ok(stat(DescriptorType::Directory, 0)),
            Some(Description::File {
                size: Some(size), ..
            }) => Ok(stat(DescriptorType::RegularFile, *size)),
            Some(_) => Err(ErrorCode::Unsupported),
            None => Err(ErrorCode::NoEntry),
        })
    }
    async fn set_times_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _path_flags: PathFlags,
        _path: String,
        _data_access_timestamp: NewTimestamp,
        _data_modification_timestamp: NewTimestamp,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn link_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _old_path_flags: PathFlags,
        _old_path: String,
        _new_descriptor: Resource<Descriptor>,
        _new_path: String,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn open_at(
        &mut self,
        descriptor: Resource<Descriptor>,
        path_flags: PathFlags,
        key: String,
        open_flags: OpenFlags,
        flags: DescriptorFlags,
    ) -> wasmtime::Result<Result<Resource<Descriptor>, ErrorCode>> {
        if open_flags.intersects(OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::EXCLUSIVE)
            || flags.intersects(DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY)
        {
            return Ok(Err(ErrorCode::ReadOnly));
        }
        if path_flags != PathFlags::empty()
            || (flags & !DescriptorFlags::READ) != DescriptorFlags::empty()
        {
            return Ok(Err(ErrorCode::Unsupported));
        }
        let path = match self.resolve(&descriptor, &key) {
            Ok(path) => path,
            Err(error) => return Ok(Err(error)),
        };
        let opened = match self.view.metadata(&path) {
            Some(Description::Directory) => OpenDescriptor::Directory(path),
            Some(Description::File { size, .. }) => {
                if open_flags.contains(OpenFlags::DIRECTORY) {
                    return Ok(Err(ErrorCode::NotDirectory));
                }
                if !flags.contains(DescriptorFlags::READ) {
                    return Ok(Err(ErrorCode::Access));
                }
                let size = *size;
                let handle = match self.view.open(&path, &self.access).await {
                    Ok(handle) => handle,
                    Err(pauperfuse_virtual_fs::OpenError::Access { source, cause }) => {
                        self.failure = Some(SourceFailure {
                            source,
                            cause: Box::new(cause),
                        });
                        return Ok(Err(ErrorCode::Io));
                    }
                    Err(error) => return Err(wasmtime::Error::msg(error.to_string())),
                };
                OpenDescriptor::File { path, handle, size }
            }
            Some(Description::Symlink { .. }) => return Ok(Err(ErrorCode::Unsupported)),
            None => return Ok(Err(ErrorCode::NoEntry)),
        };
        Ok(self.insert(opened))
    }
    async fn readlink_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _path: String,
    ) -> wasmtime::Result<Result<String, ErrorCode>> {
        Ok(Err(ErrorCode::Unsupported))
    }
    async fn remove_directory_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _path: String,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn rename_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _old_path: String,
        _new_descriptor: Resource<Descriptor>,
        _new_path: String,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn symlink_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _old_path: String,
        _new_path: String,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn unlink_file_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _path: String,
    ) -> wasmtime::Result<Result<(), ErrorCode>> {
        Ok(Err(ErrorCode::ReadOnly))
    }
    async fn is_same_object(
        &mut self,
        first: Resource<Descriptor>,
        second: Resource<Descriptor>,
    ) -> wasmtime::Result<bool> {
        let path = |id| match self.descriptors.get(&id) {
            Some(OpenDescriptor::Directory(path)) | Some(OpenDescriptor::File { path, .. }) => {
                Some(path)
            }
            None => None,
        };
        Ok(path(first.rep())
            .zip(path(second.rep()))
            .is_some_and(|(first, second)| first == second))
    }
    async fn metadata_hash(
        &mut self,
        _self_: Resource<Descriptor>,
    ) -> wasmtime::Result<Result<MetadataHashValue, ErrorCode>> {
        Ok(Err(ErrorCode::Unsupported))
    }
    async fn metadata_hash_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _path_flags: PathFlags,
        _path: String,
    ) -> wasmtime::Result<Result<MetadataHashValue, ErrorCode>> {
        Ok(Err(ErrorCode::Unsupported))
    }
    async fn drop(&mut self, _rep: Resource<Descriptor>) -> wasmtime::Result<()> {
        self.descriptors
            .remove(&_rep.rep())
            .ok_or_else(|| wasmtime::Error::msg("invalid descriptor drop"))?;
        Ok(())
    }
}

impl<A> HostDirectoryEntryStream for Filesystem<A>
where
    A: ByteAccess + Send + Sync + 'static,
    A::Reader: Send + 'static,
    A::Error: Send + Sync + 'static,
    <A::Reader as ByteReader>::Error: Send + Sync + 'static,
{
    async fn read_directory_entry(
        &mut self,
        _resource: Resource<DirectoryEntryStream>,
    ) -> wasmtime::Result<Result<Option<DirectoryEntry>, ErrorCode>> {
        Ok(Err(ErrorCode::Unsupported))
    }
    async fn drop(&mut self, _resource: Resource<DirectoryEntryStream>) -> wasmtime::Result<()> {
        Err(wasmtime::Error::msg(
            "no directory-entry streams are issued",
        ))
    }
}
impl<A> crate::bindings::wasi::filesystem::types::Host for Filesystem<A>
where
    A: ByteAccess + Send + Sync + 'static,
    A::Reader: Send + 'static,
    A::Error: Send + Sync + 'static,
    <A::Reader as ByteReader>::Error: Send + Sync + 'static,
{
    async fn filesystem_error_code(
        &mut self,
        _resource: Resource<crate::bindings::wasi::filesystem::types::Error>,
    ) -> wasmtime::Result<Option<ErrorCode>> {
        Ok(None)
    }
}
impl<A> preopens::Host for Filesystem<A>
where
    A: ByteAccess + Send + Sync + 'static,
    A::Reader: Send + 'static,
    A::Error: Send + Sync + 'static,
    <A::Reader as ByteReader>::Error: Send + Sync + 'static,
{
    async fn get_directories(
        &mut self,
    ) -> wasmtime::Result<Vec<(Resource<DescriptorToken>, String)>> {
        if !matches!(
            self.view.metadata(&RelPath::root()),
            Some(Description::Directory)
        ) {
            return Err(wasmtime::Error::msg(
                "mount must explicitly describe its directory root",
            ));
        }
        let root = self.insert(OpenDescriptor::Directory(RelPath::root()))?;
        Ok(vec![(root, "/".into())])
    }
}

fn stat(type_: DescriptorType, size: u64) -> DescriptorStat {
    DescriptorStat {
        type_,
        link_count: 1,
        size,
        data_access_timestamp: None,
        data_modification_timestamp: None,
        status_change_timestamp: None,
    }
}
