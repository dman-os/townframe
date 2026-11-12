//! The cartridge-level install, enablement and grant methods of ADR 044 §8, re-exported from
//! [`super`]. They live apart from the registry's own methods because they are served by the
//! runtime that reconciles the config documents, not by the registry.

use super::CartridgesError;
use crate::CartridgeId;
use crate::config::CartridgeInstall;
use crate::config::Component;
use crate::config::GrantBody;
use crate::config::GrantKey;
use big_cabinet_core::DatumPin;
use serde::Deserialize;
use serde::Serialize;
use service_core_rs::service_method;

// The cartridge-level management methods of ADR 044 §8. They are one set for a cartridge's nodule
// and micro alike, and the state they change (installs, enablement, grants) lives in the node's
// config document and the cartridge's config document, so the runtime that reconciles those
// documents serves them: the nodule service on this node. They are declared here, beside the rest
// of the cartridges surface, so a micro host and its clients depend on them without depending on
// the nodule runtime. Every one that changes configuration is privileged: no nodule's dispatch
// scope reaches them, which is what keeps a nodule from widening its own grants.

/// Installs a published version: pins the version datum `cartridge` names (a datum in the
/// cartridge's registry document, pinned at the document heads it was published at), creates the
/// cartridge's config document or reuses the one an earlier install of it left, and records the
/// install. Installing a cartridge that is installed already moves it to the pinned version and
/// keeps its enablement. The cartridge starts disabled.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstallCartridge {
    pub cartridge: DatumPin,
}

/// Enables or disables one component of an installed cartridge.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetCartridgeEnabled {
    pub cartridge_id: CartridgeId,
    pub component: Component,
    pub enabled: bool,
}

/// Removes the install, and with it the pin and the handler artifact references. The cartridge's
/// config document is kept, so a reinstall (or the same cartridge on a sister node) finds its
/// settings and grants; [`ClearCartridgeConfig`] is the explicit way to drop them.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UninstallCartridge {
    pub cartridge_id: CartridgeId,
}

/// Sets, or with `value` absent revokes, one grant of one component: the datum `grant` names in the
/// cartridge's config document. A drawer grant's value states its access and a method or authority
/// grant's states whether it is privileged (see [`GrantKey::accepts`]).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetCartridgeGrant {
    pub cartridge_id: CartridgeId,
    pub component: Component,
    pub grant: GrantKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<GrantBody>,
}

/// Deletes the cartridge's settings datums and every grant datum, nodule and micro, from its
/// config document. The install, if any, stays: the cartridge keeps running with no settings and
/// no grants.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClearCartridgeConfig {
    pub cartridge_id: CartridgeId,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListCartridgeInstalls {}

/// A live feed of [`CartridgeInstalls`]: the current rows first, then the full rows again after
/// each change to an install, so a shell reacts to enable and disable as they happen.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObserveCartridgeInstalls {}

/// One installed cartridge as the install surface reports it.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeInstallRecord {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub cartridge_id: CartridgeId,
    pub install: CartridgeInstall,
}

/// The result of [`ListCartridgeInstalls`], and each event of [`ObserveCartridgeInstalls`].
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeInstalls {
    pub installs: Vec<CartridgeInstallRecord>,
}

method_core::methods! {
    error = CartridgesError, nsid_prefix = "org.example.daybook.cartridges.";
    InstallCartridge => CartridgeInstallRecord, procedure, "installCartridge", privileged;
    SetCartridgeEnabled => CartridgeInstallRecord, procedure, "setCartridgeEnabled", privileged;
    UninstallCartridge => (), procedure, "uninstallCartridge", privileged;
    SetCartridgeGrant => (), procedure, "setCartridgeGrant", privileged;
    ClearCartridgeConfig => (), procedure, "clearCartridgeConfig", privileged;
    ListCartridgeInstalls => CartridgeInstalls, query, "listCartridgeInstalls";
}
service_method!(ObserveCartridgeInstalls => stream CartridgeInstalls, CartridgesError, "org.example.daybook.cartridges.observeCartridgeInstalls");
