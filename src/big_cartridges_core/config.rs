//! The datums that configure an installed cartridge (ADR 044).
//!
//! Two kinds of document hold them. The node's config document records, per cartridge, the
//! [`CartridgeInstall`] (`org.example.daybook.cartridges.install/<cartridgeId>`): which published
//! version this node runs, where the cartridge's configuration lives, and which of its components
//! are enabled here. The cartridge's own config document, one per cartridge in the shared config
//! drawer, holds what follows the user to a sister node: the cartridge's settings datums, which are
//! ordinary datums under the NSIDs the manifest declares, and one grant datum per grant
//! ([`NoduleGrant`], [`MicroGrant`]), keyed by [`GrantKey`].
//!
//! A cartridge's nodule and its micro ship in one envelope and run the same revision, so there is
//! one install datum with a member per [`Component`], and each component keeps its own grants
//! because they run as different actors.

use crate::CartridgeId;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumPin;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::Extra;
use big_cabinet_core::datum::is_nsid;
use big_cabinet_core::datum::is_nsid_authority;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::drawer::EntryAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::str::FromStr;

/// The NSID authority of the install and config-identity datums.
pub const CONFIG_AUTHORITY: &str = "org.example.daybook.cartridges";

/// The authority every dispatch grant of `*` names: the grant covers every NSID.
const ALL_AUTHORITIES: &str = "*";

/// The two components a cartridge can contribute. They are enabled independently and are granted
/// independently, since each runs as its own actor.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Component {
    Nodule,
    Micro,
}

impl Component {
    /// The tag of this component's grant datums.
    pub const fn grant_tag(self) -> &'static str {
        match self {
            Self::Nodule => NoduleGrant::ID,
            Self::Micro => MicroGrant::ID,
        }
    }
}

impl fmt::Display for Component {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Nodule => "nodule",
            Self::Micro => "micro",
        })
    }
}

/// `org.example.daybook.cartridges.install/<cartridgeId>`: what this node runs of a cartridge. It
/// lives in the node config document and replaces the copied envelope of ADR 027 with a pin: the
/// heads of `cartridge` select the revision, and holding the pin is what keeps that version.
///
/// A component's member is present exactly when the pinned envelope contributes it, and each is
/// enabled on its own.
#[macros::datum(authority = CONFIG_AUTHORITY, name = "install")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeInstall {
    /// The pinned version datum (`org.example.daybook.cartridge/<version>`) in the cartridge's
    /// registry document.
    pub cartridge: DatumPin,
    /// The cartridge's config document in the shared config drawer.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub config_doc: DocId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodule: Option<ComponentInstall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub micro: Option<ComponentInstall>,
    #[serde(flatten)]
    pub extra: Extra,
}

impl CartridgeInstall {
    pub fn component(&self, component: Component) -> Option<&ComponentInstall> {
        match component {
            Component::Nodule => self.nodule.as_ref(),
            Component::Micro => self.micro.as_ref(),
        }
    }

    pub fn component_mut(&mut self, component: Component) -> &mut Option<ComponentInstall> {
        match component {
            Component::Nodule => &mut self.nodule,
            Component::Micro => &mut self.micro,
        }
    }
}

/// One component's state on this node.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentInstall {
    pub enabled: bool,
    #[serde(flatten)]
    pub extra: Extra,
}

impl ComponentInstall {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            extra: Default::default(),
        }
    }
}

/// `org.example.daybook.cartridges.config/main`: names the cartridge a config document is for.
/// Written once when the document is created, so a node (or a sister) installing the same
/// cartridge finds the document by scanning the shared config drawer for it instead of creating a
/// second one.
#[macros::datum(authority = CONFIG_AUTHORITY, name = "config")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeConfigIdentity {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub cartridge: CartridgeId,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The slot [`CartridgeConfigIdentity`] is written at.
pub fn config_identity_key() -> DatumKey {
    DatumKey::from_text(CartridgeConfigIdentity::ID, "main")
        .expect("`main` is a valid datum key slot")
}

pub fn install_key(cartridge_id: &CartridgeId) -> DatumKey {
    DatumKey::from_text(CartridgeInstall::ID, cartridge_id.to_string())
        .expect("a CartridgeId is a valid datum key slot")
}

/// What one grant datum grants. A drawer grant states the access `access` and no `privileged`;
/// a method or authority grant states no `access` and whether the grant reaches privileged
/// methods (the nodule service holds no privilege, so a grant that sets it is reported as not
/// applied, as before).
macro_rules! grant_datum {
    ($(#[$doc:meta])* $name:ident, $authority:tt) => {
        $(#[$doc])*
        #[macros::datum(authority = $authority, name = "grant")]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct $name {
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub access: Option<EntryAccess>,
            #[serde(default, skip_serializing_if = "std::ops::Not::not")]
            pub privileged: bool,
            #[serde(flatten)]
            pub extra: Extra,
        }

        impl From<GrantBody> for $name {
            fn from(body: GrantBody) -> Self {
                Self {
                    access: body.access,
                    privileged: body.privileged,
                    extra: Default::default(),
                }
            }
        }
    };
}

grant_datum!(
    /// `org.example.daybook.nodule.grant/<grantKey>`: one grant of the cartridge's nodule.
    NoduleGrant,
    "org.example.daybook.nodule"
);
grant_datum!(
    /// `org.example.daybook.micro.grant/<grantKey>`: one grant of the cartridge's micro.
    MicroGrant,
    "org.example.daybook.micro"
);

/// The value of one grant on the wire: the body every component's grant datum carries.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<EntryAccess>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub privileged: bool,
}

impl GrantBody {
    pub fn drawer(access: EntryAccess) -> Self {
        Self {
            access: Some(access),
            privileged: false,
        }
    }

    pub fn method(privileged: bool) -> Self {
        Self {
            access: None,
            privileged,
        }
    }
}

/// What a grant is a grant of, and the slot of its datum: grants made on two devices for the same
/// thing converge to one datum, and different grants never share one. The keys use only portable
/// ids (drawer ids, NSIDs), never a per-node actor, so they mean the same thing on a sister.
///
/// A method grant names one operation and an authority grant a prefix of whole NSID segments (or
/// `*`), in separate slot kinds because an NSID-shaped name such as `org.example.tags` is both a
/// valid operation and a valid authority, and the two grant different things (a prefix never
/// stretches to a dev build's additions, ADR 028 §3).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GrantKey {
    /// `drawer:<drawerId>`: access to a local drawer.
    Drawer(DrawerId),
    /// `method:<nsid>`: one operation the component may invoke through dispatch.
    Method(String),
    /// `authority:<prefix>`: every operation under an NSID authority prefix, or `*`.
    Authority(String),
}

impl GrantKey {
    /// The key of the datum holding this grant for `component`.
    pub fn datum_key(&self, component: Component) -> DatumKey {
        DatumKey::from_text(component.grant_tag(), self.to_string())
            .expect("a grant key is a valid datum key slot")
    }

    /// The grant a datum slot names, `None` when it is not a grant key.
    pub fn from_slot(slot: &[u8]) -> Option<Self> {
        std::str::from_utf8(slot).ok()?.parse().ok()
    }

    /// Whether `body` has the shape this kind of grant stores: a drawer grant states its access
    /// and nothing else, a method or authority grant states no access.
    pub fn accepts(&self, body: &GrantBody) -> bool {
        match self {
            Self::Drawer(_) => body.access.is_some() && !body.privileged,
            Self::Method(_) | Self::Authority(_) => body.access.is_none(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "`{0}` is not a grant key: expected `drawer:<drawerId>`, `method:<nsid>` or `authority:<prefix>`"
)]
pub struct InvalidGrantKey(pub String);

impl fmt::Display for GrantKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drawer(drawer) => write!(formatter, "drawer:{drawer}"),
            Self::Method(nsid) => write!(formatter, "method:{nsid}"),
            Self::Authority(prefix) => write!(formatter, "authority:{prefix}"),
        }
    }
}

impl FromStr for GrantKey {
    type Err = InvalidGrantKey;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidGrantKey(value.to_owned());
        let (kind, target) = value.split_once(':').ok_or_else(invalid)?;
        match kind {
            "drawer" => target.parse().map(Self::Drawer).map_err(|_| invalid()),
            "method" if is_nsid(target) => Ok(Self::Method(target.to_owned())),
            "authority" if target == ALL_AUTHORITIES || is_nsid_authority(target) => {
                Ok(Self::Authority(target.to_owned()))
            }
            _ => Err(invalid()),
        }
    }
}

impl Serialize for GrantKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for GrantKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for GrantKey {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "GrantKey".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::GrantKey").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string" })
    }
}

/// The declarations of the configuration documents' datums, for the node's schema registry. The
/// embedder adds the declarations of its own config datums (the nodule's dev takeover and
/// routes) before collecting them into a universe.
#[cfg(feature = "schemars")]
pub fn config_declarations() -> Vec<big_cabinet_core::datum::Declaration> {
    use big_cabinet_core::datum::Declaration;
    vec![
        Declaration::of::<CartridgeInstall>().expect("CartridgeInstall declaration is valid"),
        Declaration::of::<CartridgeConfigIdentity>()
            .expect("CartridgeConfigIdentity declaration is valid"),
        Declaration::of::<NoduleGrant>().expect("NoduleGrant declaration is valid"),
        Declaration::of::<MicroGrant>().expect("MicroGrant declaration is valid"),
    ]
}
