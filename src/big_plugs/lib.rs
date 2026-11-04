//! The plug registry service (ADR 028 §1): the node-side catalog of plugs.
//!
//! The registry is a grouped drawer holding one document per plug id, with each version as an
//! immutable datum and a `Blob` datum per artifact (see [`big_plugs_core::registry`]). The
//! service derives what it serves from that drawer ([`registry`]), so another node syncing the
//! drawer discovers the same plugs; SQLite holds only the drawer's id and this node's first-seen
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
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Namespace;
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
use big_plugs_core::protocol::GetPlug;
use big_plugs_core::protocol::GetPlugVersion;
use big_plugs_core::protocol::GetPublisher;
use big_plugs_core::protocol::ImportPlugDocument;
use big_plugs_core::protocol::ListPlugs;
use big_plugs_core::protocol::MintPlugsCapability;
use big_plugs_core::protocol::PlugChanges;
use big_plugs_core::protocol::PlugRecord;
use big_plugs_core::protocol::PlugVersionRecord;
use big_plugs_core::protocol::PlugsError;
use big_plugs_core::protocol::PlugsMessage;
use big_plugs_core::protocol::PlugsProtocol;
use big_plugs_core::protocol::PlugsScope;
use big_plugs_core::protocol::PublishPlugVersion;
use big_plugs_core::protocol::PublishedPlugVersion;
use big_plugs_core::protocol::RemovePlug;
use big_plugs_core::protocol::RemovePlugVersion;
use big_plugs_core::protocol::WatchPlugs;
use big_plugs_core::registry::registry_universe;
use big_plugs_core::signature::did_key;
use registry::Registry;
use service_utils_rs::Client;
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
const SCOPE_PREFIX: &str = "PlugService/scope/v1/";

static REGISTRY_UNIVERSE: LazyLock<Universe> = LazyLock::new(registry_universe);

/// The schema registration group for the registry's datums (the envelope and the plug identity).
/// The `Blob` Lexicon the artifact references use is registered beside it by big_blobs.
pub fn schema_group() -> SchemaRegistrationGroup<'static> {
    let bundle = Bundle::new(&REGISTRY_UNIVERSE, Vec::new());
    let lexicons = bundle.lexicons().expect("registry lexicons publish");
    let registrations = lexicons
        .into_iter()
        .map(|lexicon| SchemaRegistration::new(lexicon.id, lexicon.file));
    SchemaRegistrationGroup::new(Some(bundle), registrations, Vec::new())
}

type Res<T> = std::result::Result<T, PlugsError>;

pub(crate) fn authority_error(error: ClientError<AuthorityServiceError>) -> PlugsError {
    match error {
        ClientError::Operation(AuthorityServiceError::Context(error)) => error.into(),
        ClientError::Operation(
            AuthorityServiceError::InvalidCapability
            | AuthorityServiceError::Unauthorized
            | AuthorityServiceError::UnrecognizedActor,
        ) => PlugsError::Denied,
        ClientError::Operation(error) => PlugsError::Authority(error),
        ClientError::Transport { message, .. } => PlugsError::AuthorityUnavailable(message),
    }
}

pub(crate) fn docs_error(error: ClientError<DocsError>) -> PlugsError {
    match error {
        ClientError::Operation(DocsError::Context(error)) => error.into(),
        ClientError::Operation(DocsError::Authority(error)) => PlugsError::Authority(error),
        ClientError::Operation(DocsError::AuthorityUnavailable(message)) => {
            PlugsError::AuthorityUnavailable(message)
        }
        ClientError::Operation(DocsError::Denied) => PlugsError::Denied,
        ClientError::Operation(other) => PlugsError::Storage(other.to_string()),
        ClientError::Transport { message, .. } => PlugsError::Storage(message),
    }
}

/// What [`ensure_registry_drawer`] needs: the drawers service, the actor that creates the drawer
/// (it holds Admin on it afterwards, and is the identity the blob workers sign as), the drawer
/// access policy it is granted creation through, and the node's database.
pub struct RegistryDrawerConfig<'a> {
    pub drawers: &'a Drawers,
    pub actor: &'a Actor,
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
    let storage = |error: &dyn std::fmt::Display| PlugsError::Storage(error.to_string());
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
            config.actor.actor_id,
            CREATE_DRAWER_OBJECT.into(),
            Rights::USE,
            None,
        )
        .await
        .map_err(|error| storage(&error))?;
    let create = config
        .drawers
        .client
        .call(
            context.child(),
            MintDrawerCapability {
                authorization: identity,
                scope: DrawerScope {
                    drawers: BTreeMap::new(),
                    create: true,
                },
                rights: Rights::USE,
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
                command: command("plug registry drawer"),
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
                    command: command(&format!("plug registry inventory {root} {name}")),
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
            config.actor.actor_id,
            &[CREATE_DRAWER_OBJECT.into()],
        )
        .await
        .map_err(|error| storage(&error))?;
    Ok(drawer)
}

/// The node's publisher identity: its Ed25519 public key and a function that signs a message with
/// the matching secret. The secret never leaves its owner; the registry only ever asks for
/// signatures over envelopes (domain-separated, see [`big_plugs_core::signature`]).
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
pub struct PlugsConfig {
    pub authority: Client<AuthorityProtocol>,
    pub docs: Client<DocsProtocol>,
    pub registry_drawer: DrawerId,
    pub actor: Actor,
    pub namespace: Namespace,
    pub operator: Uuid7,
    pub blobs: Arc<BlobService>,
    pub drawers: Client<DrawersProtocol>,
    pub node_key: NodeKey,
    pub sql: Database,
}

/// The hosted surface: it settles who the caller is, then hands the work to the registry.
pub struct PlugService {
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

impl PlugService {
    async fn caller(
        &self,
        context: &RequestContext,
        authorization: Authorization,
    ) -> Res<Invocation<PlugsScope>> {
        self.capabilities
            .source::<PlugsScope>(context, authorization)
            .await
            .map_err(authority_error)
    }

    /// What the caller may do: everything for the operator's identity, the scope of a handle this
    /// domain minted, nothing for any other identity.
    fn scope_of<'a>(&self, caller: &'a Invocation<PlugsScope>) -> Res<Option<&'a PlugsScope>> {
        if !caller.rights.contains(Rights::USE) {
            return Err(PlugsError::Denied);
        }
        match &caller.authority {
            DomainAuthority::Identity(actor) if *actor == self.operator => Ok(None),
            DomainAuthority::Identity(_) => Err(PlugsError::Denied),
            DomainAuthority::Scoped(scope) => Ok(Some(scope)),
        }
    }

    /// Publishing and removing need the handle's `publish` scope, and the plug's namespace must be
    /// one the handle may publish under.
    fn require_publish(&self, caller: &Invocation<PlugsScope>, namespace: &str) -> Res<()> {
        match self.scope_of(caller)? {
            Some(scope) if !scope.publish => Err(PlugsError::Denied),
            Some(scope) if !scope.may_publish_under(namespace) => {
                Err(PlugsError::NamespaceNotPermitted {
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

    async fn mint(&self, context: &RequestContext, params: MintPlugsCapability) -> Res<Capability> {
        let caller = self.caller(context, params.authorization).await?;
        if let Some(scope) = self.scope_of(&caller)?
            && !scope.covers(&params.scope)
        {
            return Err(PlugsError::Denied);
        }
        self.capabilities
            .mint(
                context,
                params.authorization.authority,
                &caller,
                params.scope,
                params.rights,
                Vec::new(),
            )
            .await
            .map_err(authority_error)
    }

    async fn publish(
        &self,
        context: &RequestContext,
        params: PublishPlugVersion,
    ) -> Res<PublishedPlugVersion> {
        let mut envelope = params.envelope;
        self.write_caller(context, params.authorization, &envelope.id.namespace)
            .await?;
        if envelope.signature.is_none() {
            // The node signs only for its own DID: signing for any other namespace would let a
            // local publisher pass an envelope off as someone else's.
            if envelope.id.namespace != self.publisher {
                return Err(PlugsError::UnsignedForeignNamespace {
                    namespace: envelope.id.namespace,
                });
            }
            envelope.sign_with(|message| (self.node_key.sign)(message));
        }
        self.registry.publish(envelope).await
    }

    async fn remove_version(&self, context: &RequestContext, params: RemovePlugVersion) -> Res<()> {
        self.write_caller(context, params.authorization, &params.id.namespace)
            .await?;
        self.registry
            .remove_version(&params.id, &params.version)
            .await
    }

    async fn remove_plug(&self, context: &RequestContext, params: RemovePlug) -> Res<()> {
        self.write_caller(context, params.authorization, &params.id.namespace)
            .await?;
        self.registry.remove_plug(&params.id).await
    }

    async fn import_document(
        &self,
        context: &RequestContext,
        params: ImportPlugDocument,
    ) -> Res<PlugRecord> {
        let caller = self.caller(context, params.authorization).await?;
        // The document's namespace is known only after it is read, so the scope is checked
        // against it by the registry once it has.
        self.registry
            .import_document(&params.doc_id, &params.source_drawer, |id| {
                self.require_publish(&caller, &id.namespace)
            })
            .await
    }

    async fn get_publisher(&self, context: &RequestContext, params: GetPublisher) -> Res<String> {
        self.read_caller(context, params.authorization).await?;
        Ok(self.publisher.clone())
    }

    async fn list(&self, context: &RequestContext, params: ListPlugs) -> Res<Vec<PlugRecord>> {
        self.read_caller(context, params.authorization).await?;
        Ok(self.registry.records().await)
    }

    async fn get(&self, context: &RequestContext, params: GetPlug) -> Res<PlugRecord> {
        self.read_caller(context, params.authorization).await?;
        self.registry
            .record(&params.id)
            .await
            .ok_or(PlugsError::UnknownPlug(params.id))
    }

    async fn get_version(
        &self,
        context: &RequestContext,
        params: GetPlugVersion,
    ) -> Res<PlugVersionRecord> {
        self.read_caller(context, params.authorization).await?;
        self.registry.version(&params.id, &params.version).await
    }

    /// The long poll of ADR 023: answers at once when there is a change past the cursor (or the
    /// caller must reset), otherwise holds until one lands, the hold expires, or the service
    /// stops. An expired hold is an ordinary empty page.
    async fn watch(&self, context: &RequestContext, params: WatchPlugs) -> Res<PlugChanges> {
        self.read_caller(context, params.authorization).await?;
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
        impl Handler<$request> for PlugService {
            async fn call(
                &self,
                request: Envelope<$request>,
            ) -> Res<ResponseEnvelope<$output>> {
                Ok(self.$method(&request.context, request.params).await?.into())
            }
        }
    )*};
}
handlers! {
    MintPlugsCapability => mint: Capability;
    PublishPlugVersion => publish: PublishedPlugVersion;
    RemovePlugVersion => remove_version: ();
    RemovePlug => remove_plug: ();
    ImportPlugDocument => import_document: PlugRecord;
    GetPublisher => get_publisher: String;
    ListPlugs => list: Vec<PlugRecord>;
    GetPlug => get: PlugRecord;
    GetPlugVersion => get_version: PlugVersionRecord;
    WatchPlugs => watch: PlugChanges;
}

service_utils_rs::bind_service_protocol! {
    pub struct PlugsBinding for PlugsProtocol => PlugsMessage {
        unary {
            MintPlugsCapability(MintPlugsCapability),
            PublishPlugVersion(PublishPlugVersion),
            RemovePlugVersion(RemovePlugVersion),
            RemovePlug(RemovePlug),
            ImportPlugDocument(ImportPlugDocument),
            GetPublisher(GetPublisher),
            ListPlugs(ListPlugs),
            GetPlug(GetPlug),
            GetPlugVersion(GetPlugVersion),
            WatchPlugs(WatchPlugs)
        }
        stream {}
    }
}

pub type PlugsHandler = Arc<PlugService>;

/// A booted registry service: the host that fronts it and the client its consumers use.
pub struct Plugs {
    pub host: Arc<Host<PlugsHandler>>,
    pub client: Client<PlugsProtocol>,
    /// The node's publisher DID, the namespace unsigned envelopes are signed for.
    pub publisher: String,
}

/// Boot the registry on `config.registry_drawer`: scan the drawer into the served state, wait for
/// the scan to catch up (so a restart serves what was published before it), and host the service.
/// The returned token stops the host, then the drawer follower, then retires the namespace.
pub async fn boot(config: PlugsConfig) -> Res<(Plugs, StopToken)> {
    store::migrate(&config.sql).await?;
    let capability = config
        .docs
        .call(
            RequestContext::root(),
            MintDocCapability {
                authorization: Authorization::identity(config.actor.capability),
                scope: DocScope {
                    drawers: BTreeMap::from([(config.registry_drawer.clone(), Access::Write)]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
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
            config.registry_drawer,
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
    let service = Arc::new(PlugService {
        capabilities,
        operator: config.operator,
        registry,
        publisher: publisher.clone(),
        node_key: config.node_key,
        tasks,
    });
    let (host, host_stop) = Host::new(Arc::clone(&service));
    let client = Client::direct(PlugsBinding::new(Arc::clone(&host)));
    Ok((
        Plugs {
            host,
            client,
            publisher,
        },
        host_stop.then(follow_stop).then(domain_stop),
    ))
}
