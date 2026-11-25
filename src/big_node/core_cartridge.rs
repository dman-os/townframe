//! The core cartridge (ADR 053): the node's built-in surfaces shipped as a cartridge that the node
//! publishes and installs itself, so they are versioned, listed and pinned like any other
//! cartridge's.
//!
//! **Bootstrap.** Admission never reads the core cartridge from the store: the Lexicons it
//! publishes are generated from the same Rust declarations the binary admits with
//! ([`defs_lexicon`]), so the store's copy is a record of what the node provides, for `day carts`,
//! audit and other cartridges' references, and never an input.
//!
//! **Identity.** Its id is `<the node's publisher DID>/core`. The node signs envelopes in its own
//! namespace with its own key (`big_cartridges`' publish), so the node publishes its core
//! cartridge the way a person publishes theirs, and no build-time signing key exists.
//!
//! **Versions.** [`VERSION`] is the version this build carries. The registry never changes a
//! published version's content and refuses a version that does not order after the newest one it
//! has had, so a binary older than the core cartridge a node has installed fails to boot rather
//! than un-admitting what other cartridges depend on (ADR 053 §4). Changing what [`package`]
//! carries is therefore a version bump, which the test below pins.

use crate::BigNode;
use crate::Res;
use big_blobs_core::BlobsScope;
use big_blobs_core::MintBlobsCapability;
use big_cabinet_core::DatumPin;
use big_cabinet_core::authority::AttenuateCapability;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::reference::defs_lexicon;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Digest;
use big_cartridges_core::Necessity;
use big_cartridges_core::Permbind;
use big_cartridges_core::PermbindMethod;
use big_cartridges_core::ResolvedCartridge;
use big_cartridges_core::Version;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::protocol::AwaitCartridgeGrants;
use big_cartridges_core::protocol::GetCartridge;
use big_cartridges_core::protocol::GetCartridgeVersion;
use big_cartridges_core::protocol::ListCartridgeGrants;
use big_cartridges_core::protocol::ListCartridgeInstalls;
use big_cartridges_core::protocol::PublishCartridgeVersion;
use big_cartridges_core::protocol::StageCartridgeVersion;
use big_cartridges_core::registry::version_key;
use big_cartridges_core::registry::version_pin;
use method_core::ByteBody;
use method_core::Method;
use method_core::XrpcMethod;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// The core cartridge version this build carries.
pub const VERSION: &str = "0.2.0";

/// `<publisher>/core`, where `publisher` is the node's publisher DID.
pub fn id(publisher: &str) -> CartridgeId {
    CartridgeId::new(publisher, "core").expect("a publisher DID and `core` form a cartridge id")
}

/// The methods the core nodule calls through dispatch (ADR 052 §2.2): the unprivileged authority
/// methods it opens each micro's actor and narrows handles onto it with, and the read-only
/// privileged methods it learns what to mint from. Nothing here writes a grant.
///
/// Each is declared with its necessity and whether its grant is privileged.
///
/// The authority methods are required: their provider is registered before the nodule service
/// reconciles. The nodules methods are optional because theirs is registered after the node is
/// assembled (`BigNode::register_late_built_ins`), so a required request for one would hold the
/// nodule at `ProviderMissing` through boot. The nodule service reconciles when the provider
/// registers, so the optional requests resolve then. Either way the grant below is what admits a call.
///
/// The grants of the three reads are privileged, because the service answers a nodule an
/// unscoped read of every cartridge only on a privileged grant of that very method
/// (`NoduleService::read_scope`). `listCartridgeInstalls` is not a privileged method for dispatch
/// (a scoped management handle may list the installs it manages), but a nodule's unscoped answer
/// is the same node bookkeeping the other two read, so it is held to the same rule.
fn methods() -> [(XrpcMethod, Necessity, bool); 6] {
    [
        (OpenActor::XRPC, Necessity::Required, false),
        (AttenuateCapability::XRPC, Necessity::Required, false),
        (RetractCapability::XRPC, Necessity::Required, false),
        (ListCartridgeGrants::XRPC, Necessity::Optional, true),
        (AwaitCartridgeGrants::XRPC, Necessity::Optional, true),
        (ListCartridgeInstalls::XRPC, Necessity::Optional, true),
    ]
}

/// The id and version the nodule service refuses changes to, `micros`, the subsystem actor the
/// core nodule acts as and the handles its routines' handles are minted from, and the grants the node makes it: one for each method [`package`]
/// declares a permbind on, `privileged` for the reads, which the service holds for the boot and
/// never writes to the cartridge's config document (a person could not have granted them, and
/// `setCartridgeGrant` refuses the core cartridge).
pub(crate) fn pin(
    publisher: &str,
    micros: OpenedActor,
    delegators: big_nodule::CoreDelegators,
) -> big_nodule::CoreCartridge {
    big_nodule::CoreCartridge {
        id: id(publisher),
        version: version(),
        actor: micros,
        delegators,
        grants: methods()
            .into_iter()
            .map(|(method, _, privileged)| {
                (
                    GrantKey::Method(method.nsid.to_owned()),
                    if privileged {
                        GrantBody::privileged()
                    } else {
                        GrantBody::present()
                    },
                )
            })
            .collect(),
    }
}

fn version() -> Version {
    VERSION
        .parse()
        .expect("the core cartridge version is semver")
}

/// The core cartridge of the node whose publisher DID is `publisher`: today the reference defs
/// (`org.example.daybook.defs`), the one type library every cartridge's Lexicons may refer to,
/// and the core nodule. The nodule has no routines yet; the micros routines of ADR 052 are its
/// first. It is declared now because an install is of a cartridge's nodule or micro (a
/// cartridge contributing neither is refused), so it is what makes the core cartridge installed.
/// Its manifest declares a method permbind for each of [`methods`], the runtime permbind of ADR
/// 052 §2.2 item 2, which [`pin`] grants.
fn package(publisher: &str) -> ResolvedCartridge {
    let defs = defs_lexicon();
    let nsid = defs["id"]
        .as_str()
        .expect("the defs Lexicon states its id")
        .parse()
        .expect("the defs Lexicon id is an NSID");
    let file = serde_json::from_value(defs).expect("a Lexicon file is a JSON object");
    let mut cartridge = Cartridge::new(id(publisher), version());
    cartridge.name = Some("Core".to_owned());
    cartridge.description =
        Some("The node's own vocabulary: the type libraries every cartridge may refer to.".into());
    cartridge.nodule = Some(Default::default());
    cartridge.permbinds = methods()
        .into_iter()
        .map(|(method, necessity, _)| {
            (
                method.nsid.to_owned(),
                Permbind::Method(PermbindMethod {
                    nsid: method
                        .nsid
                        .parse()
                        .expect("a built-in method id is an NSID"),
                    description: format!("The core nodule calls {} through dispatch.", method.nsid),
                    necessity,
                    min_revision: None,
                    extra: Default::default(),
                }),
            )
        })
        .collect();
    ResolvedCartridge::new(
        cartridge,
        BTreeMap::from([(nsid, file)]),
        BTreeMap::new(),
        BTreeMap::new(),
    )
}

impl BigNode {
    /// Publishes the core cartridge version this build carries unless the registry already has
    /// it, and installs it unless the install already pins it. Runs on every boot, before the
    /// node serves anyone, so a node created before the core cartridge existed takes it at its
    /// next boot and an upgraded binary moves the install to its own version.
    pub(crate) async fn ensure_core_cartridge(&self) -> Res<()> {
        let package = package(&self.cartridges.publisher);
        let operator = Authorization::identity(self.authority.root);
        let installed = self
            .nodules
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                ListCartridgeInstalls {},
            )
            .await?
            .installs
            .into_iter()
            .find(|record| record.cartridge_id == package.cartridge.id);
        let wanted = version_key(&package.cartridge.version);
        if installed
            .is_some_and(|record| record.install.cartridge.location().reference().key() == &wanted)
        {
            return Ok(());
        }
        let pin = self
            .publish_version(&package, &BTreeMap::new(), |envelope| envelope)
            .await?;
        self.nodules
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                big_cartridges_core::protocol::InstallCartridge { cartridge: pin },
            )
            .await?;
        Ok(())
    }

    /// Publishes `package` as the node operator, unless its version is published already, and
    /// returns the pin of that version at the registry document's current heads, which is what an
    /// install takes.
    ///
    /// `blobs` are the bytes of the package's wasm components by digest: they go into the blob
    /// store first, because staging checks a component's bytes are there. The payloads are
    /// staged into the cartridge's registry document and the envelope carrying the staged pins
    /// is published after `sign` has had it; an envelope `sign` leaves unsigned is signed by the
    /// node, which it can only be in the node's own namespace.
    pub(crate) async fn publish_version(
        &self,
        package: &ResolvedCartridge,
        blobs: &BTreeMap<Digest, Vec<u8>>,
        sign: impl FnOnce(Cartridge) -> Cartridge,
    ) -> Res<DatumPin> {
        let operator = Authorization::identity(self.authority.root);
        let id = &package.cartridge.id;
        let version = &package.cartridge.version;
        let published = self
            .cartridges
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                GetCartridgeVersion {
                    id: id.clone(),
                    version: version.clone(),
                },
            )
            .await
            .is_ok();
        if !published {
            if !blobs.is_empty() {
                let put = self
                    .blobs
                    .client
                    .call_with(
                        RequestContext::root(),
                        operator.metadata(),
                        MintBlobsCapability {
                            scope: BlobsScope {
                                put: true,
                                ..Default::default()
                            },
                            rights: Rights::USE,
                            subject: None,
                        },
                    )
                    .await?
                    .capability_id;
                for bytes in blobs.values() {
                    self.blobs
                        .put(
                            RequestContext::root(),
                            put,
                            ByteBody::from_bytes(bytes.clone()),
                        )
                        .await?;
                }
            }
            let staged = self
                .cartridges
                .client
                .call_with(
                    RequestContext::root(),
                    operator.metadata(),
                    StageCartridgeVersion {
                        id: id.clone(),
                        version: version.clone(),
                        lexicons: package.lexicons.clone(),
                        sidecars: package.sidecars.clone(),
                        components: package.components.clone(),
                    },
                )
                .await?;
            // The pins are part of what a signature covers, so the envelope takes the staged
            // pins before it is signed.
            let mut envelope = package.cartridge.clone();
            envelope.lexicons = staged.lexicons;
            envelope.sidecars = staged.sidecars;
            envelope.components = staged.components;
            envelope.signature = None;
            self.cartridges
                .client
                .call_with(
                    RequestContext::root(),
                    operator.metadata(),
                    PublishCartridgeVersion {
                        envelope: sign(envelope),
                    },
                )
                .await?;
        }
        let doc_id = self
            .cartridges
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                GetCartridge { id: id.clone() },
            )
            .await?
            .doc_id;
        let capability = self
            .docs
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                MintDocCapability {
                    scope: DocScope {
                        read: DocScope::read_all([doc_id.clone()]),
                        ..Default::default()
                    },
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await?
            .capability_id;
        let heads = self
            .docs
            .client
            .call(
                RequestContext::root(),
                ReadDatums {
                    capability,
                    doc_id: doc_id.clone(),
                    at: At::Current,
                    datums: Some(BTreeSet::new()),
                    optimistic: false,
                },
            )
            .await?
            .heads;
        Ok(version_pin(doc_id, version, heads))
    }
}

#[cfg(test)]
mod tests {
    /// What the core cartridge carries is pinned to [`super::VERSION`]: a published version never
    /// changes content, so new content under an old label would be refused by every node that
    /// has the old one. Changing the package changes this digest; bump the version with it.
    #[test]
    fn the_core_cartridge_content_changes_only_with_its_version() {
        // The envelope is part of the content (its permbinds are what the node grants), and a
        // fixed publisher makes its id one value.
        let package = super::package("did:web:example.org");
        let content = serde_json::to_vec(&(
            &package.cartridge,
            &package.lexicons,
            &package.sidecars,
            &package.components,
        ))
        .expect("a package serializes");
        assert_eq!(
            (
                super::VERSION,
                big_cartridges_core::Digest::of(&content).to_string()
            ),
            (
                "0.2.0",
                "zAY9PkRdrcWxNCzNJ3sMTy2mrh6NVCPWEpmcbVMns7Pm1".to_owned()
            ),
        );
    }
}
