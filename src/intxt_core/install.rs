//! Installing intxt on a node as a library call, for whoever holds operator authority over one:
//! `day x intxt install` over the CLI's connection, and a host that boots its own node in process
//! (the phone app) over its own transport. Both build a [`Caller`] and call [`install`].
//!
//! The flow is ADR 044's and ADR 047's, the way a node hosts any nodule's handler: the handler
//! component's bytes go into the blob store first, because the registry refuses a version whose
//! components it cannot find and the executor loads them from there when the nodule is enabled;
//! the version's payloads are then staged and the envelope published to the node's registry,
//! installed by the pin of the version just published, granted what its permbinds name (the
//! methods, and the drawers the caller passes in), and enabled.

use crate::cartridge::build_of;
use crate::cartridge::cartridge;
use crate::cartridge::next_version;
use crate::cartridge::requested_methods;
use crate::client::Caller;
use crate::methods::IntxtError;
use big_blobs_core::BlobInfo;
use big_blobs_core::BlobsScope;
use big_blobs_core::MAX_PUT_OCTETS;
use big_blobs_core::MintBlobsCapability;
use big_blobs_core::PutBlob;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cartridges_core::config::Component;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::protocol::GetCartridge;
use big_cartridges_core::protocol::InstallCartridge;
use big_cartridges_core::protocol::PublishCartridgeVersion;
use big_cartridges_core::protocol::SetCartridgeEnabled;
use big_cartridges_core::protocol::SetCartridgeGrant;
use big_cartridges_core::registry::version_key;
use big_cartridges_core::registry::version_pin;
use big_nodule_core::protocol::GetNodule;
use big_nodule_core::protocol::ListNodules;
use big_nodule_core::protocol::NoduleRecord;
use method_core::ByteBody;
use service_client_core_rs::xrpc::HttpTransport;

/// What [`install`] answers.
#[derive(Debug, Clone, PartialEq)]
pub struct Installed {
    /// The nodule's record. Its readiness says whether the executor loaded the handler: a
    /// component the host cannot run is `ExecutorUnavailable`, which is the record's to report,
    /// not an error of the install.
    pub record: NoduleRecord,
    /// The node already had this version of intxt installed and enabled, so nothing was written.
    pub already_installed: bool,
}

/// Installs intxt with the handler component `guest_wasm` on the node `caller` reaches, and
/// enables it. `caller` must hold the node's operator authority: it publishes to the registry,
/// installs, grants and enables.
///
/// The grants are the cartridge's (ADR 049 §4.1), one per thing its permbinds name: each
/// operation its method permbinds name as `grant/method/<nsid>`, and each of `drawers` (the
/// intxts the node already holds at `Write`, its GitHub drawer at `Read`) as
/// `grant/drawer/<id>`. A drawer intxt makes itself is not among them: the cartridge's grants
/// name drawers that exist when they are written.
///
/// Idempotent: when the node already runs this build of the handler and manifest, installed
/// and enabled, and every one of `drawers` is already granted at its access, it answers that
/// nodule's record without storing, publishing or granting anything. Drawers it is not yet
/// granted are granted, so installing again after the node holds new intxts extends the grants.
/// The version's build metadata names the handler and the manifest ([`crate::cartridge::cartridge`]),
/// so a rebuilt handler or a changed manifest is another build, and installing it publishes the
/// next patch after the one the node runs and upgrades the node.
///
/// The envelope is signed as the fixture namespace's owner, the namespace intxt's cartridge sits
/// under until it has its own (see [`crate::cartridge::cartridge`]).
pub async fn install<H: HttpTransport>(
    caller: &Caller<H>,
    guest_wasm: &[u8],
    drawers: &[(DrawerId, DrawerAccess)],
) -> Result<Installed, IntxtError> {
    // A probe at patch 0 names the build; the version it is published at is known once the node's
    // is.
    let mut fixture = cartridge(guest_wasm, 0);
    let (cartridge_id, build) = (fixture.id.clone(), build_of(&fixture.version).to_owned());
    let present = caller
        .call(ListNodules {})
        .await?
        .nodules
        .into_iter()
        .find(|record| record.cartridge_id == cartridge_id);
    let current = present.as_ref().is_some_and(|record| {
        record.enabled
            && record
                .version
                .as_ref()
                .is_some_and(|version| build_of(version) == build)
    });
    if !current {
        fixture.version = next_version(
            present.as_ref().and_then(|record| record.version.as_ref()),
            &build,
        );
    }
    let version = fixture.version.clone();
    let ungranted: Vec<_> = drawers
        .iter()
        .filter(|(drawer, access)| {
            !present
                .as_ref()
                .filter(|_| current)
                .and_then(|record| record.drawers.get(drawer))
                .is_some_and(|status| status.granted && status.desired == *access)
        })
        .collect();
    if current && ungranted.is_empty() {
        return Ok(Installed {
            record: present.expect("a current version is a present one"),
            already_installed: true,
        });
    }

    if !current {
        put_blob(caller, guest_wasm).await?;
        // The payloads (Lexicons, sidecars, components) are datums of the registry document the
        // envelope pins (ADR 047 §1), so they are staged first and the envelope that carries
        // their pins is published, signed as the fixture namespace's owner.
        let staged = caller.call(fixture.stage_request()).await?;
        caller
            .call(PublishCartridgeVersion {
                envelope: fixture.publishable(staged),
            })
            .await?;
        // The pin names the version at the heads of the registry document it was published to.
        let doc_id = caller
            .call(GetCartridge {
                id: cartridge_id.clone(),
            })
            .await?
            .doc_id;
        let (heads, _) = caller.read(&doc_id, &version_key(&version)).await?;
        caller
            .call(InstallCartridge {
                cartridge: version_pin(doc_id, &version, heads),
            })
            .await?;
        for method in requested_methods(&fixture) {
            caller
                .call(SetCartridgeGrant {
                    cartridge_id: cartridge_id.clone(),
                    grant: GrantKey::Method(method.to_owned()),
                    value: Some(GrantBody::present()),
                })
                .await?;
        }
    }
    for (drawer, access) in ungranted {
        caller
            .call(SetCartridgeGrant {
                cartridge_id: cartridge_id.clone(),
                grant: GrantKey::Drawer(drawer.clone()),
                value: Some(GrantBody::access((*access).into())),
            })
            .await?;
    }
    if !current {
        caller
            .call(SetCartridgeEnabled {
                cartridge_id: cartridge_id.clone(),
                component: Component::Nodule,
                enabled: true,
            })
            .await?;
    }
    let record = caller.call(GetNodule { cartridge_id }).await?;
    // A nodule holds its dispatch capability only while enabled, so whether the grants took is
    // known from the enabled record. Every method intxt's handler calls is grantable, and every
    // drawer the caller names is one the node holds, so a refusal means the node and this build
    // disagree about them.
    let grants = record.methods.as_ref().ok_or_else(|| {
        IntxtError::Invalid("the node reports no method grants for intxt".to_owned())
    })?;
    if !grants.granted {
        return Err(IntxtError::Invalid(format!(
            "the node did not grant intxt the methods its handler calls: {}",
            grants.error.as_deref().unwrap_or("no reason given")
        )));
    }
    for (drawer, _) in drawers {
        match record.drawers.get(drawer) {
            Some(status) if status.granted => {}
            other => {
                return Err(IntxtError::Invalid(format!(
                    "the node did not grant intxt the drawer {drawer}: {}",
                    other
                        .and_then(|status| status.error.as_deref())
                        .unwrap_or("it reports no grant")
                )));
            }
        }
    }
    Ok(Installed {
        record,
        already_installed: false,
    })
}

/// Puts `bytes` in the node's blob store as the caller's authority and answers what the node
/// stored. The bytes are the request body (ADR 042), so the node reads them as a stream and
/// refuses (`BlobsError::TooLarge`) past [`MAX_PUT_OCTETS`]; the check here only gives that
/// refusal a message that names the limit.
pub async fn put_blob<H: HttpTransport>(
    caller: &Caller<H>,
    bytes: &[u8],
) -> Result<BlobInfo, IntxtError> {
    if bytes.len() as u64 > MAX_PUT_OCTETS {
        return Err(IntxtError::Invalid(format!(
            "{} bytes is over the {MAX_PUT_OCTETS} octets one blob put carries",
            bytes.len()
        )));
    }
    let capability = caller
        .call(MintBlobsCapability {
            scope: BlobsScope {
                put: true,
                ..BlobsScope::default()
            },
            rights: Rights::USE,
            subject: None,
        })
        .await?
        .capability_id;
    caller
        .call_with_bytes(PutBlob { capability }, ByteBody::from_bytes(bytes.to_vec()))
        .await
}
