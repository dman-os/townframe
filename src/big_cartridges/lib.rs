//! The cartridge registry service (ADR 028 §1): the node-side catalog of cartridges.
//!
//! The registry is a grouped drawer holding one document per cartridge id, with each version as an
//! immutable datum and a `Blob` datum per artifact (see [`big_cartridges_core::registry`]). The
//! service derives what it serves from that drawer ([`registry`]), so another node syncing the
//! drawer discovers the same cartridges; SQLite holds only the drawer's id and this node's first-seen
//! record of each version ([`store`]). It does no enablement: each runtime owns that.
//!
//! Boot is two steps because the blob workers are configured with the drawers they maintain
//! blobs for: [`ensure_registry_drawer`] runs before them and yields the drawer id, and [`boot`]
//! runs after the blob service exists.

#![recursion_limit = "256"]

use sqlite_rs as sqlite_rs_alias;

mod sqlite {
    include!("sqlite/gen.rs");
}
pub mod import;
mod registry;
mod store;

use big_blobs::BlobService;
use big_cabinet::DrawerAccess;
use big_cabinet::Drawers;
use big_cabinet::SchemaRegistration;
use big_cabinet::SchemaRegistrationGroup;
use big_cabinet::authority::DomainAuthority;
use big_cabinet::capability::DomainCapabilities;
use big_cabinet::capability::Invocation;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::Universe;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::CREATE_DRAWER_OBJECT;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::CreateMetaDoc;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::DrawersProtocol;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cartridges_core::Cartridge;
use big_cartridges_core::ResolvedCartridge;
use big_cartridges_core::protocol::CartridgeChanges;
use big_cartridges_core::protocol::CartridgeList;
use big_cartridges_core::protocol::CartridgePublisher;
use big_cartridges_core::protocol::CartridgeRecord;
use big_cartridges_core::protocol::CartridgeVersionRecord;
use big_cartridges_core::protocol::CartridgesError;
use big_cartridges_core::protocol::CartridgesMessage;
use big_cartridges_core::protocol::CartridgesProtocol;
use big_cartridges_core::protocol::CartridgesScope;
use big_cartridges_core::protocol::GetCartridge;
use big_cartridges_core::protocol::GetCartridgeVersion;
use big_cartridges_core::protocol::GetPublisher;
use big_cartridges_core::protocol::ImportCartridgeDocument;
use big_cartridges_core::protocol::ListCartridges;
use big_cartridges_core::protocol::MintCartridgesCapability;
use big_cartridges_core::protocol::PublishCartridgeVersion;
use big_cartridges_core::protocol::PublishedCartridgeVersion;
use big_cartridges_core::protocol::RemoveCartridge;
use big_cartridges_core::protocol::RemoveCartridgeVersion;
use big_cartridges_core::protocol::ResolveCartridgeVersion;
use big_cartridges_core::protocol::StageCartridgeVersion;
use big_cartridges_core::protocol::WatchCartridges;
use big_cartridges_core::registry::registry_universe;
use big_cartridges_core::signature::did_key;
use registry::Registry;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::Handler;
use service_utils_rs::Host;
use service_utils_rs::RequestContext;
use service_utils_rs::ResponseEnvelope;
use service_utils_rs::Spawner;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::Instant;

/// The prefix of every capability object this domain mints; the scope follows it as JSON.
const SCOPE_PREFIX: &str = "CartridgeService/scope/v1/";

static REGISTRY_UNIVERSE: LazyLock<Universe> = LazyLock::new(registry_universe);

/// The schema registration groups for the registry's datums: the envelope and the cartridge
/// identity (declared by the registry's own universe), and the payload tags a version's pins name
/// (Lexicons, sidecars and typed components). The payload tags are open objects: a Lexicon is
/// validated by Lexicon admission and a sidecar or component by the typed view the registry
/// resolves it through, so the datum schema only has to admit the write. The `Blob` Lexicon the
/// artifact references use is registered beside them by big_blobs.
pub fn schema_groups() -> [SchemaRegistrationGroup<'static>; 2] {
    use big_cartridges_core::registry::COMPONENT_TAG;
    use big_cartridges_core::registry::LEXICON_TAG;
    use big_cartridges_core::registry::SIDECAR_TAG;
    let bundle = Bundle::new(&REGISTRY_UNIVERSE, Vec::new());
    let lexicons = bundle.lexicons().expect("registry lexicons publish");
    let registrations = lexicons
        .into_iter()
        .map(|lexicon| SchemaRegistration::new(lexicon.id, lexicon.file));
    let payloads = [LEXICON_TAG, SIDECAR_TAG, COMPONENT_TAG].map(|tag| {
        SchemaRegistration::new(
            tag,
            serde_json::json!({
                "lexicon": 1,
                "id": tag,
                "defs": { "main": { "type": "object", "properties": {} } }
            }),
        )
    });
    [
        SchemaRegistrationGroup::new(Some(bundle), registrations, Vec::new()),
        SchemaRegistrationGroup::new(None, payloads, Vec::new()),
    ]
}

type Res<T> = std::result::Result<T, CartridgesError>;

pub(crate) fn authority_error(error: ClientError<AuthorityServiceError>) -> CartridgesError {
    match error {
        ClientError::Method(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Method(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor,
        ) => CartridgesError::Denied,
        ClientError::Method(error) => CartridgesError::Authority(error),
        ClientError::Transport { message, .. } => CartridgesError::AuthorityUnavailable(message),
    }
}

pub(crate) fn docs_error(error: ClientError<DocsError>) -> CartridgesError {
    match error {
        ClientError::Method(DocsError::Context(error)) => error.into(),
        ClientError::Method(DocsError::Authority(error)) => CartridgesError::Authority(error),
        ClientError::Method(DocsError::AuthorityUnavailable(message)) => {
            CartridgesError::AuthorityUnavailable(message)
        }
        ClientError::Method(DocsError::Denied) => CartridgesError::Denied,
        ClientError::Method(other) => CartridgesError::Storage(other.to_string()),
        ClientError::Transport { message, .. } => CartridgesError::Storage(message),
    }
}

/// What [`ensure_registry_drawer`] needs: the drawers service, the actor that creates the drawer
/// (it holds Admin on it afterwards, and is the identity the blob workers sign as), the drawer
/// access policy it is granted creation through, and the node's database.
pub struct RegistryDrawerConfig<'a> {
    pub drawers: &'a Drawers,
    pub actor: &'a OpenedActor,
    pub access: &'a DrawerAccess,
    pub sql: Database,
}

/// The registry drawer's id, created on the first boot and read back from SQLite on every later
/// one. It is a grouped drawer rather than a self-doc drawer because it syncs, and a grouped
/// drawer is what has the blob inventories (ADR 026) that carry artifact bytes to peers; the
/// conventional public `ciphertext` and sensitive `plaintext` inventories are created with it.
///
/// A crash between the drawer's creation and the stored id replays the creation under its fixed
/// command, which the drawer ledger answers with the same drawer while its row is retained; past
/// that, the first drawer is leaked as an unreferenced empty drawer and a second one made.
pub async fn ensure_registry_drawer(config: RegistryDrawerConfig<'_>) -> Res<DrawerId> {
    store::migrate(&config.sql).await?;
    if let Some(drawer) = store::registry_drawer(&config.sql).await? {
        return Ok(drawer);
    }
    let storage = |error: &dyn std::fmt::Display| CartridgesError::Storage(error.to_string());
    let command = |text: &str| {
        CommandId::try_from(text.to_owned()).expect("a registry drawer command is within bounds")
    };
    let context = RequestContext::root();
    let identity = Authorization::identity(config.actor.capability);
    config
        .access
        .grants()
        .grant(
            &context,
            config.actor.actor.actor_id,
            CREATE_DRAWER_OBJECT.into(),
            Rights::USE,
            None,
        )
        .await
        .map_err(|error| storage(&error))?;
    let create = config
        .drawers
        .client
        .call_with(
            context.child(),
            identity.metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    drawers: BTreeMap::new(),
                    create: true,
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .map_err(|error| storage(&error))?;
    let drawer = config
        .drawers
        .client
        .call(
            context.child(),
            CreateDrawer {
                capability: create.capability_id,
                command: command("cartridge registry drawer"),
                layout: CreateLayout::SingleGroup,
            },
        )
        .await
        .map_err(|error| storage(&error))?
        .drawer_id;
    let admin = config
        .drawers
        .mint_drawer_capability(&context, identity, &drawer, Access::Admin, Rights::USE)
        .await
        .map_err(|error| storage(&error))?;
    for (root, name) in [
        (MetaRoot::Public, "ciphertext"),
        (MetaRoot::Sensitive, "plaintext"),
    ] {
        config
            .drawers
            .client
            .call(
                context.child(),
                CreateMetaDoc {
                    capability: admin.capability_id,
                    command: command(&format!("cartridge registry inventory {root} {name}")),
                    drawer_id: drawer.clone(),
                    kind: big_blobs::workers::INVENTORY_KIND.into(),
                    name: name.into(),
                    root,
                },
            )
            .await
            .map_err(|error| storage(&error))?;
    }
    store::set_registry_drawer(&config.sql, &drawer).await?;
    // Creating the one registry drawer is the only use of this right, so it is revoked once the
    // drawer is recorded rather than left standing (least privilege). Admin on the drawer comes
    // from being its creator and is unaffected.
    config
        .access
        .grants()
        .revoke(
            &context,
            config.actor.actor.actor_id,
            &[CREATE_DRAWER_OBJECT.into()],
        )
        .await
        .map_err(|error| storage(&error))?;
    Ok(drawer)
}

/// The node's publisher identity: its Ed25519 public key and a function that signs a message with
/// the matching secret. The secret never leaves its owner; the registry only ever asks for
/// signatures over envelopes (domain-separated, see [`big_cartridges_core::signature`]).
pub struct NodeKey {
    pub public_key: [u8; 32],
    pub sign: NodeSigner,
}

/// Signs a message with the node's secret key, returning the 64 Ed25519 signature bytes.
pub type NodeSigner = Arc<dyn Fn(&[u8]) -> [u8; 64] + Send + Sync>;

/// What the embedder hands the service: the authority it mints handles from, the docs service it
/// reads and writes the registry drawer through, the drawer (from [`ensure_registry_drawer`]) and
/// the actor holding Admin on it, the namespace it mints handles in, the operator whose identity
/// administers the registry, the blob service it checks artifact bytes against, the node's
/// publisher key, and the database.
pub struct CartridgesConfig {
    pub authority: Client<AuthorityProtocol>,
    pub docs: Client<DocsProtocol>,
    pub registry_drawer: DrawerId,
    pub actor: OpenedActor,
    pub namespace: Namespace,
    pub operator: Uuid7,
    pub blobs: Arc<BlobService>,
    pub drawers: Client<DrawersProtocol>,
    pub node_key: NodeKey,
    pub sql: Database,
}

/// The hosted surface: it settles who the caller is, then hands the work to the registry.
pub struct CartridgeService {
    capabilities: DomainCapabilities,
    operator: Uuid7,
    registry: Arc<Registry>,
    /// The `did:key` of the node's key, and the key's signer: an unsigned envelope in this
    /// namespace is signed by the node on publish.
    publisher: String,
    node_key: NodeKey,
    /// Ends a held watch when the service stops, rather than at its hold's expiry.
    tasks: Spawner,
}

impl CartridgeService {
    async fn caller(
        &self,
        context: &RequestContext,
        authorization: Authorization,
    ) -> Res<Invocation<CartridgesScope>> {
        self.capabilities
            .source::<CartridgesScope>(context, authorization)
            .await
            .map_err(authority_error)
    }

    /// What the caller may do: everything for the operator's identity, the scope of a handle this
    /// domain minted, nothing for any other identity.
    fn scope_of<'a>(
        &self,
        caller: &'a Invocation<CartridgesScope>,
    ) -> Res<Option<&'a CartridgesScope>> {
        if !caller.rights.contains(Rights::USE) {
            return Err(CartridgesError::Denied);
        }
        match &caller.authority {
            DomainAuthority::Identity(actor) if *actor == self.operator => Ok(None),
            DomainAuthority::Identity(_) => Err(CartridgesError::Denied),
            DomainAuthority::Scoped(scope) => Ok(Some(scope)),
        }
    }

    /// Publishing and removing need the handle's `publish` scope, and the cartridge's namespace must be
    /// one the handle may publish under.
    fn require_publish(&self, caller: &Invocation<CartridgesScope>, namespace: &str) -> Res<()> {
        match self.scope_of(caller)? {
            Some(scope) if !scope.publish => Err(CartridgesError::Denied),
            Some(scope) if !scope.may_publish_under(namespace) => {
                Err(CartridgesError::NamespaceNotPermitted {
                    namespace: namespace.to_owned(),
                })
            }
            _ => Ok(()),
        }
    }

    async fn read_caller(&self, context: &RequestContext, authorization: Authorization) -> Res<()> {
        let caller = self.caller(context, authorization).await?;
        self.scope_of(&caller).map(drop)
    }

    async fn write_caller(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        namespace: &str,
    ) -> Res<()> {
        let caller = self.caller(context, authorization).await?;
        self.require_publish(&caller, namespace)
    }

    async fn mint(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: MintCartridgesCapability,
    ) -> Res<Capability> {
        let caller = self.caller(context, authorization).await?;
        if let Some(scope) = self.scope_of(&caller)?
            && !scope.covers(&params.scope)
        {
            return Err(CartridgesError::Denied);
        }
        self.capabilities
            .mint(
                context,
                &caller,
                params.subject,
                params.scope,
                params.rights,
                Vec::new(),
            )
            .await
            .map_err(authority_error)
    }

    async fn stage(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: StageCartridgeVersion,
    ) -> Res<Cartridge> {
        self.write_caller(context, authorization, &params.id.namespace)
            .await?;
        self.registry.stage(params).await
    }

    async fn publish(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: PublishCartridgeVersion,
    ) -> Res<PublishedCartridgeVersion> {
        let mut envelope = params.envelope;
        self.write_caller(context, authorization, &envelope.id.namespace)
            .await?;
        if envelope.signature.is_none() {
            // The node signs only for its own DID: signing for any other namespace would let a
            // local publisher pass an envelope off as someone else's.
            if envelope.id.namespace != self.publisher {
                return Err(CartridgesError::UnsignedForeignNamespace {
                    namespace: envelope.id.namespace,
                });
            }
            envelope.sign_with(|message| (self.node_key.sign)(message));
        }
        self.registry.publish(envelope).await
    }

    async fn remove_version(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: RemoveCartridgeVersion,
    ) -> Res<()> {
        self.write_caller(context, authorization, &params.id.namespace)
            .await?;
        self.registry
            .remove_version(&params.id, &params.version)
            .await
    }

    async fn remove_cartridge(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: RemoveCartridge,
    ) -> Res<()> {
        self.write_caller(context, authorization, &params.id.namespace)
            .await?;
        self.registry.remove_cartridge(&params.id).await
    }

    async fn import_document(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: ImportCartridgeDocument,
    ) -> Res<CartridgeRecord> {
        let caller = self.caller(context, authorization).await?;
        // The document's namespace is known only after it is read, so the scope is checked
        // against it by the registry once it has.
        self.registry
            .import_document(&params.doc_id, params.read, params.reach, |id| {
                self.require_publish(&caller, &id.namespace)
            })
            .await
    }

    async fn get_publisher(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        _: GetPublisher,
    ) -> Res<CartridgePublisher> {
        self.read_caller(context, authorization).await?;
        Ok(CartridgePublisher {
            publisher: self.publisher.clone(),
        })
    }

    async fn list(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        _: ListCartridges,
    ) -> Res<CartridgeList> {
        self.read_caller(context, authorization).await?;
        Ok(CartridgeList {
            cartridges: self.registry.records().await,
        })
    }

    async fn get(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: GetCartridge,
    ) -> Res<CartridgeRecord> {
        self.read_caller(context, authorization).await?;
        self.registry
            .record(&params.id)
            .await
            .ok_or(CartridgesError::UnknownCartridge(params.id))
    }

    async fn get_version(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: GetCartridgeVersion,
    ) -> Res<CartridgeVersionRecord> {
        self.read_caller(context, authorization).await?;
        self.registry.version(&params.id, &params.version).await
    }

    async fn resolve_version(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: ResolveCartridgeVersion,
    ) -> Res<ResolvedCartridge> {
        self.read_caller(context, authorization).await?;
        self.registry.resolved(&params.id, &params.version).await
    }

    /// The long poll of ADR 023: answers at once when there is a change past the cursor (or the
    /// caller must reset), otherwise holds until one lands, the hold expires, or the service
    /// stops. An expired hold is an ordinary empty page.
    async fn watch(
        &self,
        context: &RequestContext,
        authorization: Authorization,
        params: WatchCartridges,
    ) -> Res<CartridgeChanges> {
        self.read_caller(context, authorization).await?;
        let hold =
            Duration::from_millis(u64::from(params.hold_ms.min(registry::WATCH_MAX_HOLD_MS)));
        let deadline = Instant::now() + hold;
        // Subscribed before the first look, so a change between the look and the wait wakes it.
        let mut changed = self.registry.subscribe().await;
        loop {
            let page = self
                .registry
                .changes_after(params.after, params.limit)
                .await;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !page.changes.is_empty() || page.reset || remaining.is_zero() {
                return Ok(page);
            }
            tokio::select! {
                biased;
                _ = self.tasks.cancelled() => return Ok(page),
                woken = tokio::time::timeout(remaining, changed.changed()) => {
                    if let Ok(result) = woken {
                        result.expect("the registry outlives its watchers");
                    }
                }
            }
        }
    }
}

macro_rules! handlers {
    ($($request:ident => $method:ident : $output:ty;)*) => {$(
        impl Handler<$request> for CartridgeService {
            async fn call(
                &self,
                request: Envelope<$request>,
            ) -> Res<ResponseEnvelope<$output>> {
                let authorization = Authorization::from_metadata(&request.metadata)?;
                Ok(self
                    .$method(&request.context, authorization, request.params)
                    .await?
                    .into())
            }
        }
    )*};
}
handlers! {
    StageCartridgeVersion => stage: Cartridge;
    MintCartridgesCapability => mint: Capability;
    PublishCartridgeVersion => publish: PublishedCartridgeVersion;
    RemoveCartridgeVersion => remove_version: ();
    RemoveCartridge => remove_cartridge: ();
    ImportCartridgeDocument => import_document: CartridgeRecord;
    GetPublisher => get_publisher: CartridgePublisher;
    ListCartridges => list: CartridgeList;
    GetCartridge => get: CartridgeRecord;
    GetCartridgeVersion => get_version: CartridgeVersionRecord;
    ResolveCartridgeVersion => resolve_version: ResolvedCartridge;
    WatchCartridges => watch: CartridgeChanges;
}

service_utils_rs::bind_service_protocol! {
    pub struct CartridgesBinding for CartridgesProtocol => CartridgesMessage {
        unary {
            MintCartridgesCapability(MintCartridgesCapability),
            StageCartridgeVersion(StageCartridgeVersion),
            PublishCartridgeVersion(PublishCartridgeVersion),
            RemoveCartridgeVersion(RemoveCartridgeVersion),
            RemoveCartridge(RemoveCartridge),
            ImportCartridgeDocument(ImportCartridgeDocument),
            GetPublisher(GetPublisher),
            ListCartridges(ListCartridges),
            GetCartridge(GetCartridge),
            GetCartridgeVersion(GetCartridgeVersion),
            ResolveCartridgeVersion(ResolveCartridgeVersion),
            WatchCartridges(WatchCartridges)
        }
        stream {}
    }
}

pub type CartridgesHandler = Arc<CartridgeService>;

/// A booted registry service: the host that fronts it and the client its consumers use.
pub struct Cartridges {
    pub host: Arc<Host<CartridgesHandler>>,
    pub client: Client<CartridgesProtocol>,
    /// The node's publisher DID, the namespace unsigned envelopes are signed for.
    pub publisher: String,
    /// The drawer the registry's documents live in, which installs pin versions in.
    pub registry_drawer: DrawerId,
}

/// Boot the registry on `config.registry_drawer`: scan the drawer into the served state, wait for
/// the scan to catch up (so a restart serves what was published before it), and host the service.
/// The returned token stops the host, then the drawer follower, then retires the namespace.
pub async fn boot(config: CartridgesConfig) -> Res<(Cartridges, StopToken)> {
    store::migrate(&config.sql).await?;
    let capability = config
        .docs
        .call_with(
            RequestContext::root(),
            Authorization::identity(config.actor.capability).metadata(),
            MintDocCapability {
                scope: DocScope {
                    drawers: BTreeMap::from([(config.registry_drawer.clone(), Access::Write)]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .map_err(docs_error)?
        .capability_id;
    let capabilities =
        DomainCapabilities::new(config.authority.clone(), config.namespace, SCOPE_PREFIX);
    let domain_stop = capabilities.stop_token();
    let (follow_stop, tasks) = StopToken::new();
    let registry = Arc::new(
        Registry::load(
            &tasks,
            config.docs,
            capability,
            config.registry_drawer.clone(),
            registry::DocImport {
                authority: config.authority.clone(),
                drawers: config.drawers,
                actor: config.actor.capability,
            },
            config.blobs,
            config.sql,
        )
        .await?,
    );
    let (ready, scanned) = tokio::sync::oneshot::channel();
    let cancelled = tasks.clone();
    // The follower runs for the node's life: it either never returns or returns an error, and a
    // dead follower would leave a registry that silently stops tracking its drawer.
    let following = Arc::clone(&registry);
    tasks
        .spawn(async move {
            following
                .follow(ready, cancelled.cancelled())
                .await
                .expect("the registry follower tracks its drawer");
        })
        .expect("a fresh stop token accepts tasks");
    scanned
        .await
        .expect("the registry follower reports its first scan");
    let publisher = did_key(&config.node_key.public_key);
    let service = Arc::new(CartridgeService {
        capabilities,
        operator: config.operator,
        registry,
        publisher: publisher.clone(),
        node_key: config.node_key,
        tasks,
    });
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let client = Client::new(Transport::direct(CartridgesBinding::new(Arc::clone(&host))));
    Ok((
        Cartridges {
            host,
            client,
            publisher,
            registry_drawer: config.registry_drawer,
        },
        host_stop.then(follow_stop).then(domain_stop),
    ))
}
