use crate::encrypt::CipherBlobProvider;
use crate::ownership::ByteOwnership;
use crate::pair_roots::PairRoots;
use big_blobs_core::BlobId;
use big_blobs_core::protocol::*;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::InspectCapability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::Rights;
use big_sync::keyed_frontier::SqliteFrontierCodec;
use big_sync::keyed_frontier::SqliteFrontierSelector;
use big_sync::keyed_frontier::SqliteKeyedFrontier;
use big_sync_core::keyed_frontier::KeyedFrontier;
use big_sync_core::keyed_frontier::KeyedFrontierTransaction;
use big_sync_core::revisioned_store::KeyedFrontierRevisionReader;
use bytes::Bytes;
use fs4::fs_std::FileExt;
use futures::Stream;
use iroh_blobs::Hash;
use iroh_blobs::api::Store;
use iroh_blobs::api::proto::BlobStatus;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::Handler;
use service_utils_rs::Host;
use service_utils_rs::RequestContext;
use service_utils_rs::ResponseEnvelope;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::sync::Notify;
use utils_rs::prelude::Res;

#[derive(Clone)]
pub struct ImportedBlob {
    pub info: BlobInfo,
    staging_id: [u8; 32],
}

impl ImportedBlob {
    pub(crate) fn new(info: BlobInfo) -> Self {
        Self {
            info,
            staging_id: rand::random(),
        }
    }

    pub(crate) fn owner(&self) -> String {
        format!(
            "upload:{}",
            data_encoding::HEXLOWER.encode(&self.staging_id)
        )
    }
}

impl std::fmt::Debug for ImportedBlob {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ImportedBlob")
            .field("info", &self.info)
            .finish_non_exhaustive()
    }
}

/// The blob store a node is handed. The service opens no location itself: the composition root
/// opens the store ([`Storage::memory`] or [`Storage::filesystem`]) and passes it in.
pub struct Storage {
    store: Store,
    provider: Arc<CipherBlobProvider>,
    /// Holds the exclusive owner lock of a filesystem store for as long as the store lives.
    owner: Option<std::fs::File>,
}

impl Storage {
    /// A store that lives and dies with the process. Must be called inside a tokio runtime.
    pub fn memory() -> Res<Self> {
        let (memory, providers) =
            iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register(&providers)?;
        Ok(Self {
            store: Store::from(memory),
            provider,
            owner: None,
        })
    }

    /// Opens (creating if needed) the store rooted at `root` and takes its exclusive owner lock.
    pub async fn filesystem(root: PathBuf) -> Res<Self> {
        tokio::fs::create_dir_all(&root).await?;
        let owner = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("owner.lock"))?;
        // fs4 reports contention as `Ok(false)`, not an error: only real IO
        // failures arrive in `Err`. Proceeding on `false` would reopen the
        // already-owned `blobs.db` and block forever inside redb's own
        // exclusive lock, so the second open must fail here instead.
        let locked = owner.try_lock_exclusive().map_err(|error| {
            utils_rs::prelude::eyre::eyre!("could not take the blob store owner lock: {error}")
        })?;
        if !locked {
            return Err(utils_rs::prelude::eyre::eyre!(
                "blob store already has an owner; connect through its service"
            ));
        }
        let (filesystem, providers) = iroh_blobs::store::fs::FsStore::load_with_virtuals(
            root.join("blobs.db"),
            iroh_blobs::store::fs::options::Options::new(&root),
        )
        .await?;
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register(&providers)?;
        Ok(Self {
            store: Store::from(filesystem),
            provider,
            owner: Some(owner),
        })
    }
}

pub struct Config {
    pub storage: Storage,
    pub sql: Database,
    pub authority: Client<AuthorityProtocol>,
    pub namespace: Namespace,
}

#[derive(Clone)]
struct AvailabilityCodec;

impl SqliteFrontierCodec for AvailabilityCodec {
    type Key = BlobId;
    type Value = BlobInfo;

    fn encode_key(&self, key: &BlobId) -> Vec<u8> {
        key.to_bytes32().to_vec()
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<BlobId, Box<dyn std::error::Error + Send + Sync>> {
        Ok(BlobId::new(bytes.try_into()?))
    }

    fn encode_value(&self, value: &BlobInfo) -> Vec<u8> {
        serde_json::to_vec(value).expect("blob info serializes")
    }

    fn decode_value(
        &self,
        bytes: &[u8],
    ) -> Result<BlobInfo, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

pub struct BlobService {
    store: Store,
    pub provider: Arc<CipherBlobProvider>,
    pub roots: PairRoots,
    pub(crate) ownership: ByteOwnership,
    authority: Client<AuthorityProtocol>,
    namespace: Namespace,
    availability: SqliteKeyedFrontier<AvailabilityCodec>,
    _owner: Option<std::fs::File>,
}

service_utils_rs::bind_service_protocol! {
    pub struct BlobsBinding for BlobsProtocol => BlobsMessage {
        unary {
            PutBlob(PutBlob),
            ReadBlob(ReadBlob),
            StatBlob(StatBlob)
        }
        stream {}
    }
}

pub struct Blobs {
    pub service: Arc<BlobService>,
    pub host: Arc<Host<Arc<BlobService>>>,
    pub client: Client<BlobsProtocol>,
}

pub async fn boot(config: Config) -> Res<(Blobs, StopToken)> {
    let Storage {
        store,
        provider,
        owner,
    } = config.storage;
    let prepared = async {
        let roots = PairRoots::boot(config.sql.clone()).await?;
        provider.restore(&store, &roots).await?;
        let availability = SqliteKeyedFrontier::new(
            config.sql.clone(),
            "big-blobs-availability-v1",
            AvailabilityCodec,
            Arc::new(Notify::new()),
        )
        .await?;
        let (ownership, ownership_stop) =
            ByteOwnership::boot(config.sql, store.clone(), Arc::clone(&provider)).await?;
        Ok::<_, utils_rs::prelude::eyre::Report>((roots, ownership, availability, ownership_stop))
    }
    .await;
    let (roots, ownership, availability, ownership_stop) = match prepared {
        Ok(value) => value,
        Err(error) => {
            store.shutdown().await?;
            return Err(error);
        }
    };
    let service = Arc::new(BlobService {
        store,
        provider,
        roots,
        ownership,
        authority: config.authority,
        namespace: config.namespace,
        availability,
        _owner: owner,
    });
    let (store_stop, tasks) = StopToken::new();
    let shutdown = Arc::clone(&service);
    let cancellation = tasks.clone();
    tasks
        .spawn(async move {
            cancellation.cancelled().await;
            shutdown
                .store
                .shutdown()
                .await
                .expect("blob store shutdown failed");
        })
        .expect("store shutdown task starts before shutdown");
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let client = Client::new(Transport::direct(BlobsBinding::new(Arc::clone(&host))));
    Ok((
        Blobs {
            service,
            host,
            client,
        },
        host_stop.then(ownership_stop).then(store_stop),
    ))
}

impl BlobService {
    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn put_object() -> &'static str {
        "big_blobs/put/v1"
    }

    pub fn read_object(blob_id: &BlobId) -> String {
        format!("big_blobs/read/v1/{blob_id}")
    }

    async fn authorize(
        &self,
        context: &RequestContext,
        capability: CapId,
        object: &str,
    ) -> Result<(), BlobsError> {
        let cap = self
            .authority
            .call(context.child(), InspectCapability { capability })
            .await
            .map_err(|error| match error {
                ClientError::Operation(AuthorityServiceError::Context(error)) => error.into(),
                ClientError::Operation(
                    AuthorityServiceError::InvalidCapability | AuthorityServiceError::Unauthorized,
                ) => BlobsError::Denied,
                other => storage_error(other),
            })?;
        if cap.issuer_id != self.namespace.namespace_id
            || !cap.rights.contains(Rights::USE)
            || cap.object != object
        {
            return Err(BlobsError::Denied);
        }
        Ok(())
    }

    pub async fn stat(&self, blob_id: &BlobId) -> Res<Option<BlobInfo>> {
        Ok(
            match self
                .store
                .blobs()
                .status(Hash::from(blob_id.to_bytes32()))
                .await?
            {
                BlobStatus::Complete { size } => {
                    let hash = Hash::from(blob_id.to_bytes32());
                    use iroh_blobs::store::virtual_blob::Provider as _;
                    if self.store.blobs().sync_reader(hash).await?.is_some()
                        || self.provider.reader_for(&hash).is_some()
                    {
                        Some(BlobInfo {
                            blob_id: *blob_id,
                            length_octets: size,
                        })
                    } else {
                        None
                    }
                }
                BlobStatus::NotFound | BlobStatus::Partial { .. } => None,
            },
        )
    }

    /// The whole of a complete local blob, or `None` while it is not (or not readable) here.
    /// In-process only, for hosts that need the bytes themselves: the `ReadBlob` wire operation
    /// is range-capped. A blob longer than `max_octets` is an error rather than a truncation, so
    /// a caller never holds a partial blob and the allocation is bounded by the caller's cap.
    pub async fn read_all(&self, blob_id: &BlobId, max_octets: u64) -> Res<Option<Vec<u8>>> {
        let Some(info) = self.stat(blob_id).await? else {
            return Ok(None);
        };
        utils_rs::prelude::eyre::ensure!(
            info.length_octets <= max_octets,
            "blob {blob_id} is {} octets, over the {max_octets} octet cap",
            info.length_octets
        );
        let mut bytes = Vec::with_capacity(info.length_octets as usize);
        self.store
            .blobs()
            .reader(Hash::from(blob_id.to_bytes32()))
            .read_to_end(&mut bytes)
            .await?;
        Ok(Some(bytes))
    }

    pub async fn announce(&self, blob_id: BlobId) -> Res<()> {
        let info = self
            .stat(&blob_id)
            .await?
            .ok_or_else(|| utils_rs::prelude::eyre::eyre!("cannot announce an incomplete blob"))?;
        let mut tx = self.availability.begin().await?;
        if tx.get(&blob_id).await?.as_ref() != Some(&info) {
            tx.put(blob_id, info).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// The availability feed's current revision: a reader opened `after` it sees only blobs
    /// announced from now on.
    pub async fn availability_revision(&self) -> Res<u64> {
        Ok(self.availability.latest_revision().await?)
    }

    pub async fn open_availability(
        &self,
        after: u64,
    ) -> Res<KeyedFrontierRevisionReader<'_, BlobId, BlobInfo>> {
        Ok(KeyedFrontierRevisionReader::new(
            self.availability
                .open(SqliteFrontierSelector::All { after })
                .await?,
        ))
    }

    pub async fn import_stream<S>(&self, stream: S) -> Res<ImportedBlob>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
    {
        let (tag, hash) = crate::encrypt::ensure_stored(&self.store, stream).await?;
        let blob_id = BlobId::new(*hash.as_bytes());
        let imported = ImportedBlob::new(
            self.stat(&blob_id)
                .await?
                .expect("an imported blob is complete"),
        );
        self.ownership.stage(imported.clone(), tag).await?;
        self.announce(blob_id).await?;
        Ok(imported)
    }

    pub async fn adopt_import(
        &self,
        imported: &ImportedBlob,
        document: &big_cabinet_core::DocId,
    ) -> Res<()> {
        self.ownership
            .adopt(imported, &format!("document:retention:{document}"))
            .await
    }

    pub async fn release_import(&self, imported: &ImportedBlob) -> Res<()> {
        self.ownership.release_import(imported).await
    }
}

fn storage_error(error: impl std::fmt::Display) -> BlobsError {
    BlobsError::Storage {
        reason: error.to_string(),
    }
}

impl Handler<PutBlob> for BlobService {
    async fn call(
        &self,
        request: Envelope<PutBlob>,
    ) -> Result<ResponseEnvelope<BlobInfo>, BlobsError> {
        self.authorize(
            &request.context,
            request.params.capability,
            Self::put_object(),
        )
        .await?;
        if request.params.bytes.len() > MAX_PUT_BYTES {
            return Err(BlobsError::InvalidInput {
                reason: format!(
                    "put exceeds {MAX_PUT_BYTES} bytes; use the native streaming import for large content"
                ),
            });
        }
        let stream = futures::stream::iter([Ok(Bytes::from(request.params.bytes))]);
        Ok(self
            .import_stream(stream)
            .await
            .map_err(storage_error)?
            .info
            .into())
    }
}

impl Handler<ReadBlob> for BlobService {
    async fn call(
        &self,
        request: Envelope<ReadBlob>,
    ) -> Result<ResponseEnvelope<BlobRange>, BlobsError> {
        let params = request.params;
        self.authorize(
            &request.context,
            params.capability,
            &Self::read_object(&params.blob_id),
        )
        .await?;
        if params.length > MAX_READ_BYTES {
            return Err(BlobsError::InvalidInput {
                reason: format!("read exceeds {MAX_READ_BYTES} bytes"),
            });
        }
        let info = self
            .stat(&params.blob_id)
            .await
            .map_err(storage_error)?
            .ok_or(BlobsError::NotFound)?;
        if params.offset > info.length_octets {
            return Err(BlobsError::InvalidInput {
                reason: "offset exceeds blob length".into(),
            });
        }
        let length = u64::from(params.length).min(info.length_octets - params.offset) as usize;
        let mut bytes = vec![0; length];
        let mut reader = self
            .store
            .blobs()
            .reader(Hash::from(params.blob_id.to_bytes32()));
        reader
            .seek(std::io::SeekFrom::Start(params.offset))
            .await
            .map_err(storage_error)?;
        reader.read_exact(&mut bytes).await.map_err(storage_error)?;
        Ok(BlobRange {
            bytes,
            length_octets: info.length_octets,
        }
        .into())
    }
}

impl Handler<StatBlob> for BlobService {
    async fn call(
        &self,
        request: Envelope<StatBlob>,
    ) -> Result<ResponseEnvelope<Option<BlobInfo>>, BlobsError> {
        self.authorize(
            &request.context,
            request.params.capability,
            &Self::read_object(&request.params.blob_id),
        )
        .await?;
        Ok(self
            .stat(&request.params.blob_id)
            .await
            .map_err(storage_error)?
            .into())
    }
}

#[cfg(test)]
mod tests;
