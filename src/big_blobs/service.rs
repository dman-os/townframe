use crate::encrypt::CipherBlobProvider;
use crate::ownership::ByteOwnership;
use crate::pair_roots::PairRoots;
use big_blobs_core::BlobId;
use big_blobs_core::protocol::*;
use big_cabinet::authority::DomainAuthority;
use big_cabinet::capability::DomainCapabilities;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::Uuid7;
use big_sync::keyed_frontier::SqliteFrontierCodec;
use big_sync::keyed_frontier::SqliteFrontierSelector;
use big_sync::keyed_frontier::SqliteKeyedFrontier;
use big_sync_core::keyed_frontier::KeyedFrontier;
use big_sync_core::keyed_frontier::KeyedFrontierTransaction;
use big_sync_core::revisioned_store::KeyedFrontierRevisionReader;
use bytes::Bytes;
use fs4::fs_std::FileExt;
use futures::Stream;
use futures::StreamExt;
use iroh_blobs::Hash;
use iroh_blobs::api::Store;
use iroh_blobs::api::TempTag;
use iroh_blobs::api::proto::BlobStatus;
use method_core::ByteBody;
use method_core::ByteSpan;
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
    /// The node operator: the one identity that may mint a blobs handle of any scope.
    pub operator: Uuid7,
}

/// The prefix of every capability object this domain mints; the scope follows it as JSON.
const SCOPE_PREFIX: &str = "big_blobs/scope/v1/";

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
    capabilities: DomainCapabilities,
    operator: Uuid7,
    availability: SqliteKeyedFrontier<AvailabilityCodec>,
    _owner: Option<std::fs::File>,
}

service_utils_rs::bind_service_protocol! {
    pub struct BlobsBinding for BlobsProtocol => BlobsMessage {
        unary {
            MintBlobsCapability(MintBlobsCapability),
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

impl Blobs {
    /// [`PutBlob`] through the in-process client, whose envelope carries the byte `body`.
    pub async fn put(
        &self,
        context: RequestContext,
        capability: CapId,
        body: ByteBody,
    ) -> Result<BlobInfo, ClientError<BlobsError>> {
        self.client
            .call_with_metadata(Envelope::new(context, PutBlob { capability }).with_body(body))
            .await
            .map(|response| response.output)
    }

    /// [`ReadBlob`] through the in-process client: the blob's bytes as a stream, or the part of
    /// them `range` selects.
    pub async fn read(
        &self,
        context: RequestContext,
        capability: CapId,
        blob_id: BlobId,
        range: Option<method_core::ByteRange>,
    ) -> Result<ByteBody, ClientError<BlobsError>> {
        self.client
            .call_with_metadata(Envelope::with_metadata(
                context,
                service_utils_rs::HttpMetadata {
                    range,
                    ..Default::default()
                },
                ReadBlob {
                    capability,
                    blob_id,
                },
            ))
            .await
            .map(|response| response.output)
    }
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
        capabilities: DomainCapabilities::new(config.authority, config.namespace, SCOPE_PREFIX),
        operator: config.operator,
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

    /// Resolves `capability` to the scope its handle was minted with and requires `permits` to
    /// hold of it. Only a handle this domain minted resolves: an identity, or a capability from
    /// any other namespace, is `Denied`.
    async fn authorize(
        &self,
        context: &RequestContext,
        capability: CapId,
        permits: impl FnOnce(&BlobsScope) -> bool,
    ) -> Result<(), BlobsError> {
        let invocation = self
            .capabilities
            .resolve::<BlobsScope>(context, capability)
            .await
            .map_err(authority_error)?;
        let DomainAuthority::Scoped(scope) = invocation.authority else {
            unreachable!("resolving a bare handle yields its scope, never an identity")
        };
        if permits(&scope) {
            Ok(())
        } else {
            Err(BlobsError::Denied)
        }
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
    /// In-process only, for hosts that need the bytes themselves in one buffer: `ReadBlob`
    /// streams them. A blob longer than `max_octets` is an error rather than a truncation, so a
    /// caller never holds a partial blob and the allocation is bounded by the caller's cap.
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
        self.stage_stored(tag, hash).await
    }

    /// Takes the upload hold on bytes `ensure_stored` just stored, and announces them.
    async fn stage_stored(&self, tag: TempTag, hash: Hash) -> Res<ImportedBlob> {
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

    /// Reads `body` into the store, whatever it is, and returns what the store now holds: the id
    /// is the one this hashes, never one the caller says, and putting what the store already
    /// holds is a cheap no-op that returns the same [`BlobInfo`] and adds an upload hold (the
    /// previous ones, if any, expire on their own). An item over `limit` octets is
    /// [`BlobsError::TooLarge`], counted as it streams in so a body of unknown length is refused
    /// as soon as it is seen to be too long.
    async fn put(&self, body: ByteBody, limit: u64) -> Result<BlobInfo, BlobsError> {
        let too_large = BlobsError::TooLarge {
            limit_octets: limit,
        };
        if body.length.is_some_and(|length| length > limit) {
            return Err(too_large);
        }
        // What stopped the body, if it stopped early. The store's importer never finishes when
        // its source yields an error (it keeps waiting for the end of a stream that has already
        // failed), so a body that fails or runs over the limit ends the stream instead, and what
        // was stored of it is dropped here without ever being held or announced.
        let stopped = Arc::new(std::sync::Mutex::new(None));
        let counted = body.scan((0u64, Arc::clone(&stopped)), move |(seen, cause), chunk| {
            std::future::ready(match chunk {
                Ok(chunk) => {
                    *seen += chunk.len() as u64;
                    if *seen > limit {
                        *cause.lock().expect("cause lock") = Some(too_large.clone());
                        None
                    } else {
                        Some(Ok(chunk))
                    }
                }
                Err(error) => {
                    *cause.lock().expect("cause lock") = Some(BlobsError::Unreadable {
                        reason: error.to_string(),
                    });
                    None
                }
            })
        });
        let (tag, hash) = crate::encrypt::ensure_stored(&self.store, counted)
            .await
            .map_err(storage_error)?;
        if let Some(cause) = stopped.lock().expect("cause lock").take() {
            drop(tag);
            return Err(cause);
        }
        Ok(self
            .stage_stored(tag, hash)
            .await
            .map_err(storage_error)?
            .info)
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

fn authority_error(error: ClientError<AuthorityServiceError>) -> BlobsError {
    match error {
        ClientError::Method(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Method(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor
            | AuthorityServiceError::Escalation,
        ) => BlobsError::Denied,
        other => storage_error(other),
    }
}

fn storage_error(error: impl std::fmt::Display) -> BlobsError {
    BlobsError::Storage {
        reason: error.to_string(),
    }
}

impl Handler<MintBlobsCapability> for BlobService {
    async fn call(
        &self,
        request: Envelope<MintBlobsCapability>,
    ) -> Result<ResponseEnvelope<Capability>, BlobsError> {
        let params = request.params;
        let authorization = Authorization::from_metadata(&request.metadata)?;
        let caller = self
            .capabilities
            .source::<BlobsScope>(&request.context, authorization)
            .await
            .map_err(authority_error)?;
        if !caller.rights.contains(Rights::USE) {
            return Err(BlobsError::Denied);
        }
        // The operator's identity mints any scope; a scoped handle only a subset of its own. Any
        // other identity is nobody this service knows. The rights subset and `NARROW` checks
        // are `DomainCapabilities::mint`'s.
        match &caller.authority {
            DomainAuthority::Identity(actor) if *actor == self.operator => {}
            DomainAuthority::Identity(_) => return Err(BlobsError::Denied),
            DomainAuthority::Scoped(scope) if !scope.covers(&params.scope) => {
                return Err(BlobsError::Denied);
            }
            DomainAuthority::Scoped(_) => {}
        }
        Ok(self
            .capabilities
            .mint(
                &request.context,
                &caller,
                params.subject,
                params.scope,
                params.rights,
                Vec::new(),
            )
            .await
            .map_err(authority_error)?
            .into())
    }
}

impl Handler<PutBlob> for BlobService {
    async fn call(
        &self,
        mut request: Envelope<PutBlob>,
    ) -> Result<ResponseEnvelope<BlobInfo>, BlobsError> {
        self.authorize(&request.context, request.params.capability, |scope| {
            scope.put
        })
        .await?;
        let body = request
            .take_body()
            .expect("a method with a byte input is called with its body");
        Ok(self.put(body, MAX_PUT_OCTETS).await?.into())
    }
}

/// How many octets a read pulls from the store at a time, which is the chunk a reader sees.
const READ_CHUNK_OCTETS: usize = 64 * 1024;

impl Handler<ReadBlob> for BlobService {
    async fn call(
        &self,
        request: Envelope<ReadBlob>,
    ) -> Result<ResponseEnvelope<ByteBody>, BlobsError> {
        let params = request.params;
        self.authorize(&request.context, params.capability, |scope| {
            scope.read.contains(&params.blob_id)
        })
        .await?;
        let info = self
            .stat(&params.blob_id)
            .await
            .map_err(storage_error)?
            .ok_or(BlobsError::NotFound)?;
        // Any byte range of a local blob, which the store's reader seeks to (ADR 042 §2). A
        // range reaching past the end is cut at it, and one that selects nothing is refused.
        let (start, length, span) = match request.metadata.range {
            None => (0, info.length_octets, None),
            Some(range) => {
                let (start, length) =
                    range
                        .resolve(info.length_octets)
                        .ok_or(BlobsError::RangeNotSatisfiable {
                            total_octets: info.length_octets,
                        })?;
                (
                    start,
                    length,
                    Some(ByteSpan {
                        start,
                        total: info.length_octets,
                    }),
                )
            }
        };
        let mut reader = self
            .store
            .blobs()
            .reader(Hash::from(params.blob_id.to_bytes32()));
        reader
            .seek(std::io::SeekFrom::Start(start))
            .await
            .map_err(storage_error)?;
        // Pulled chunk by chunk as the consumer reads, never the blob in one buffer. A blob
        // that ends before the length `stat` promised is an error mid-stream, which a transport
        // reports as a broken response, not a short success.
        let chunks =
            futures::stream::try_unfold((reader, length), |(mut reader, remaining)| async move {
                if remaining == 0 {
                    return Ok::<_, std::io::Error>(None);
                }
                let mut chunk = vec![0; remaining.min(READ_CHUNK_OCTETS as u64) as usize];
                reader.read_exact(&mut chunk).await?;
                let read = chunk.len() as u64;
                Ok(Some((Bytes::from(chunk), (reader, remaining - read))))
            });
        let body = ByteBody::new(chunks).with_length(length);
        Ok(match span {
            Some(span) => body.with_span(span),
            None => body,
        }
        .into())
    }
}

impl Handler<StatBlob> for BlobService {
    async fn call(
        &self,
        request: Envelope<StatBlob>,
    ) -> Result<ResponseEnvelope<BlobStat>, BlobsError> {
        self.authorize(&request.context, request.params.capability, |scope| {
            scope.read.contains(&request.params.blob_id)
        })
        .await?;
        Ok(BlobStat {
            info: self
                .stat(&request.params.blob_id)
                .await
                .map_err(storage_error)?,
        }
        .into())
    }
}

#[cfg(test)]
mod tests;
