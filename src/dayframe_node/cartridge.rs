//! Micros that cartridges contribute (Clay on the node section 5, ADR 049 sections 3 and 6).
//!
//! A cartridge in the node's catalog may carry a micro beside its nodule: one envelope, one
//! install datum, each component enabled on its own. [`NodeComposition::sync_cartridge_micros`]
//! reconciles the micros of every install whose `micro` member is enabled:
//!
//! 1. the install's pin names the version datum, and the cartridges service resolves it to the
//!    envelope and its payloads (the same read a nodule's install takes);
//! 2. the typed [`big_cartridges_core::MicroContribution`] names the component by key, so the wasm
//!    bytes are read out of the blob store by the digest the envelope pins, and its Clay manifest
//!    is the contribution's `description` plus the identity the envelope owns (`id`,
//!    `package_version`, `authority`), which is the sidecar a host loads a package from;
//! 3. the micro's handles are minted from its `uses` intersected with the cartridge's grants. The
//!    grants are read as the nodule's are (`ListCartridgeGrants`, resolved by
//!    [`ResolvedGrants`]) and lowered by [`big_nodule::lower_uses`], the function that lowers a
//!    routine's `uses`, so a `uses` entry confers the same thing in either runtime. The handles
//!    are bound to the micro's own actor and held by the host: the guest names only operations.
//!
//! A micro with no granted `uses` is minted nothing and can call nothing. A sync after a grant
//! changes retracts the handles whose scope changed and mints them again, so the micro's next call
//! sees the new grant.

use crate::Installed;
use crate::Minted;
use crate::NodeComposition;
use crate::install_with;
use crate::retract;
use big_blobs_core::BlobId;
use big_cabinet_core::authority::Authorization;
use big_cartridges_core::CLAY_MICRO_MANIFEST_SCHEMA;
use big_cartridges_core::Component;
use big_cartridges_core::Version;
use big_cartridges_core::protocol::CartridgeInstallRecord;
use big_cartridges_core::protocol::ListCartridgeGrants;
use big_cartridges_core::protocol::ListCartridgeInstalls;
use big_cartridges_core::protocol::ResolveCartridgeVersion;
use big_nodule_core::NoduleAdmission;
use big_nodule_core::config::ResolvedGrants;
use serde_json::Value;
use service_utils_rs::RequestContext;
use std::collections::BTreeSet;
use utils_rs::prelude::Res;
use utils_rs::prelude::WrapErr;
use utils_rs::prelude::ferr;

/// The largest micro component the loader reads. The whole component is held in memory while it
/// compiles, as a nodule's handler is (`big_nodule_wasm`), so a hostile or mistaken envelope is
/// bounded by the same 64 MiB.
const MAX_COMPONENT_OCTETS: u64 = 64 * 1024 * 1024;

/// One micro a cartridge contributes, as a host loads a package from it: `manifest_json` and
/// `component` are the sidecar JSON and the wasm bytes of a standalone package, and `micro` is the
/// id the host installs it as, which is the cartridge's id (Clay on the node section 5).
#[derive(Debug, Clone)]
pub struct CartridgeMicro {
    /// The cartridge id text, `<did>/<name>`: the key of [`NodeComposition::egress`].
    pub micro: String,
    pub manifest_json: Vec<u8>,
    pub component: Vec<u8>,
}

impl NodeComposition {
    /// Reconciles the micros of the installed cartridges with what this composition holds, and
    /// answers the micros that are now installed, each as the host loads it. An entry that is
    /// `Err` could not be loaded (the registry has not got its payloads yet, its manifest is not
    /// Clay's, or it asks for access a micro cannot be minted); it is the host's to report beside
    /// its other installation errors and never stops the others. A micro that was installed from a
    /// cartridge and is no longer enabled or installed is dropped, and its handles retracted.
    ///
    /// It is idempotent: a micro whose scopes are what they were keeps its handles. Call it again
    /// when installs or grants change; it reads both from the node each time.
    pub async fn sync_cartridge_micros(&mut self) -> Res<Vec<Res<CartridgeMicro>>> {
        let operator = Authorization::identity(self.held.node.authority.root);
        let installs = self
            .held
            .node
            .nodules
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                ListCartridgeInstalls {},
            )
            .await
            .wrap_err("listing the installed cartridges")?
            .installs;
        let wanted: Vec<CartridgeInstallRecord> = installs
            .into_iter()
            .filter(|record| {
                record
                    .install
                    .micro
                    .as_ref()
                    .is_some_and(|micro| micro.enabled)
            })
            .collect();
        let wanted_ids: BTreeSet<String> = wanted
            .iter()
            .map(|record| record.cartridge_id.to_string())
            .collect();
        let dropped: Vec<String> = self
            .installed
            .iter()
            .filter(|(micro, installed)| installed.minted.is_some() && !wanted_ids.contains(*micro))
            .map(|(micro, _)| micro.clone())
            .collect();
        for micro in dropped {
            let installed = self
                .installed
                .remove(&micro)
                .expect("the micro was listed from the table");
            installed.retract_handles(&self.held.node).await?;
        }
        let mut loaded = Vec::with_capacity(wanted.len());
        for record in wanted {
            let micro = record.cartridge_id.to_string();
            loaded.push(
                self.sync_one(record)
                    .await
                    .wrap_err_with(|| format!("loading the micro of cartridge {micro}")),
            );
        }
        Ok(loaded)
    }

    async fn sync_one(&mut self, record: CartridgeInstallRecord) -> Res<CartridgeMicro> {
        let node = &self.held.node;
        let operator = Authorization::identity(node.authority.root);
        let CartridgeInstallRecord {
            cartridge_id,
            install,
        } = record;
        let version = pinned_version(&install.cartridge)?;
        let resolved = node
            .cartridges
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                ResolveCartridgeVersion {
                    id: cartridge_id.clone(),
                    version,
                },
            )
            .await
            .wrap_err("resolving the pinned version")?;
        let cartridge = &resolved.cartridge;
        let contribution = cartridge
            .micro()
            .expect("an install has a micro member exactly when its envelope contributes one");
        if contribution.schema != CLAY_MICRO_MANIFEST_SCHEMA {
            return Err(ferr!(
                "the micro's manifest is `{}`, and this host loads only `{CLAY_MICRO_MANIFEST_SCHEMA}`",
                contribution.schema
            ));
        }
        let component = resolved
            .components
            .get(&contribution.component)
            .expect("the registry resolves every component the envelope names");
        let Component::Wasm(wasm) = component else {
            return Err(ferr!("the micro's component is not a wasm component"));
        };
        let component = match node
            .blobs
            .service
            .read_all(&BlobId::new(wasm.digest.0), MAX_COMPONENT_OCTETS)
            .await?
        {
            Some(bytes) => bytes,
            None => {
                return Err(ferr!(
                    "the micro's component is not in this node's blob store yet"
                ));
            }
        };
        assert_eq!(
            blake3::hash(&component).as_bytes(),
            &wasm.digest.0,
            "the blob store returned bytes that do not hash to their id"
        );
        let manifest_json = sidecar_json(cartridge, contribution)?;

        let grants = node
            .nodules
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                ListCartridgeGrants {
                    cartridge_id: Some(cartridge_id.clone()),
                },
            )
            .await
            .wrap_err("listing the cartridge's grants")?;
        let grants = ResolvedGrants::from_grants(
            grants
                .grants
                .iter()
                .map(|record| (&record.grant, &record.value)),
        )
        .map_err(|reason| ferr!("the cartridge's grants do not resolve: {reason}"))?;
        // The grants that cover a method permbind of the cartridge, as the nodule binds them: an
        // authority-prefix grant never reaches beyond the operations the manifest names.
        let (effective, _unrequested) = resolved.bind_operations(&grants.operations);
        let scopes = big_nodule::lower_uses(
            &resolved,
            &install.config_doc,
            &contribution.uses,
            &grants.drawers,
            &effective,
        );
        let micro = cartridge_id.to_string();
        let held = self.installed.remove(&micro);
        let installed = match held {
            Some(held)
                if held
                    .minted
                    .as_ref()
                    .is_some_and(|held| held.scopes == scopes) =>
            {
                held
            }
            held => install_with(&self.held.node, &micro, scopes, held).await?,
        };
        self.installed.insert(micro.clone(), installed);
        Ok(CartridgeMicro {
            micro,
            manifest_json,
            component,
        })
    }
}

/// The version an install's pin names: the slot of the pinned datum is the version label
/// (`org.example.daybook.cartridge/<version>`, see `big_cartridges_core::registry::version_key`).
fn pinned_version(pin: &big_cabinet_core::DatumPin) -> Res<Version> {
    let slot = std::str::from_utf8(pin.location().reference().key().slot_bytes())
        .wrap_err("the install pins a version datum at a slot that is not text")?;
    slot.parse::<Version>()
        .map_err(|error| ferr!("the install pins version `{slot}`, which is not one: {error}"))
}

/// The standalone `<name>.manifest.json` sidecar of a package: Clay's manifest, which the
/// contribution carries as its `description`, and the identity the envelope owns, which the
/// importer split off (`big_cartridges::import`): `id` is the cartridge's name, `package_version`
/// its major version and `authority` the envelope's.
fn sidecar_json(
    cartridge: &big_cartridges_core::Cartridge,
    contribution: &big_cartridges_core::MicroContribution,
) -> Res<Vec<u8>> {
    let mut sidecar: serde_json::Map<String, Value> = contribution
        .description
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let package_version = u32::try_from(cartridge.version.0.major)
        .map_err(|_| ferr!("the version {} has no u32 major", cartridge.version))?;
    sidecar.insert("id".to_owned(), Value::String(cartridge.id.name.clone()));
    sidecar.insert("package_version".to_owned(), package_version.into());
    if let Some(authority) = &cartridge.authority {
        sidecar.insert("authority".to_owned(), Value::String(authority.clone()));
    }
    Ok(serde_json::to_vec(&Value::Object(sidecar)).expect("a JSON object serializes"))
}

impl Installed {
    /// The handles minted for this micro's scopes, dispatch's included. A micro of an interim
    /// install holds none (its capabilities are minted for the life of the actor).
    pub(crate) fn handle_ids(&self) -> impl Iterator<Item = big_cabinet_core::authority::CapId> {
        self.minted.iter().flat_map(|Minted { handles, .. }| {
            [handles.docs, handles.drawers, handles.blobs]
                .into_iter()
                .flatten()
        })
    }

    async fn retract_handles(self, node: &big_node::BigNode) -> Res<()> {
        let dispatch = self.minted.as_ref().and(self.egress.capability());
        for handle in dispatch.into_iter().chain(self.handle_ids()) {
            retract(node, handle).await?;
        }
        Ok(())
    }
}
