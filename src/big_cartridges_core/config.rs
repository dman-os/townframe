//! The datums that configure an installed cartridge (ADR 044).
//!
//! Two kinds of document hold them. The node's config document records, per cartridge, the
//! [`CartridgeInstall`] (`org.example.daybook.cartridges.install/<cartridgeId>`): which published
//! version this node runs, where the cartridge's configuration lives, and which of its components
//! are enabled here. The cartridge's own config document, one per cartridge in the shared config
//! drawer, holds what follows the user to a sister node: the cartridge's settings datums, which are
//! ordinary datums under the NSIDs the manifest declares, and one grant datum per grant
//! ([`DrawerGrant`], [`CartridgeGrant`], [`NetGrant`], [`MethodGrant`], [`AuthorityGrant`]),
//! identified by [`GrantKey`].
//!
//! A cartridge's nodule and its micro ship in one envelope and run the same revision, so there is
//! one install datum with a member per [`Component`]. Grants are per cartridge, not per
//! component (ADR 049 §4.1): one grant covers both components, and each component's `uses`
//! decides what of it the component reaches.

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

/// The two components a cartridge can contribute. They are enabled independently; they share the
/// cartridge's grants (ADR 049 §4.1).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Component {
    Nodule,
    Micro,
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

/// The NSID authority of the grant datums, one datum name per kind of grant. The slot of each is
/// the thing granted (see [`GrantKey`]).
const GRANT_AUTHORITY: &str = "org.example.daybook.cartridges.grant";

/// `org.example.daybook.cartridges.grant.drawer/<drawerId>`: the cartridge may use the drawer at
/// `access`, capped by what its `#drawers` permbinds declare (ADR 049 §4.1).
#[macros::datum(authority = GRANT_AUTHORITY, name = "drawer")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerGrant {
    pub access: EntryAccess,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `org.example.daybook.cartridges.grant.cartridge/<cartridgeId>`: the cartridge may use data
/// from cartridge `<cartridgeId>` (its collections and datums included) at `access`.
#[macros::datum(authority = GRANT_AUTHORITY, name = "cartridge")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeGrant {
    pub access: EntryAccess,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `org.example.daybook.cartridges.grant.net/<host[:port]>`: the cartridge may use the network to
/// reach the host, on the one port when it is given and on any port otherwise (see
/// [`NetTarget`]).
#[macros::datum(authority = GRANT_AUTHORITY, name = "net")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetGrant {
    /// A member with nothing to say states `true` (the empty-member convention).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty: Option<bool>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `org.example.daybook.cartridges.grant.method/<nsid>`: the cartridge may invoke the one
/// method through dispatch.
#[macros::datum(authority = GRANT_AUTHORITY, name = "method")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MethodGrant {
    /// A member with nothing to say states `true` (the empty-member convention).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty: Option<bool>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `org.example.daybook.cartridges.grant.authority/<prefix>`: the cartridge may invoke every
/// method under an NSID authority prefix, or `*`. A separate datum from [`MethodGrant`]
/// because an NSID-shaped name such as `org.example.tags` is both a valid method and a valid
/// authority, and the two grant different things (a prefix never stretches to a dev build's
/// additions, ADR 028 §3).
#[macros::datum(authority = GRANT_AUTHORITY, name = "authority")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorityGrant {
    /// A member with nothing to say states `true` (the empty-member convention).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty: Option<bool>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The value of one grant on the wire: the access a drawer or cartridge grant states. The other
/// kinds of grant state nothing, so a grant of one is just present.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<EntryAccess>,
    /// A method grant that reaches methods declared `privileged` (`XrpcMethod::privileged`), which
    /// no other grant can. Only the node makes one, and only for its core cartridge, as a grant
    /// held for the boot (ADR 052 §2.2 item 2, ADR 053 §3): the runtime ignores the member on a
    /// grant read from a config document, and `setCartridgeGrant` refuses it, so a person or a
    /// sister node cannot confer it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub privileged: bool,
}

impl GrantBody {
    pub fn access(access: EntryAccess) -> Self {
        Self {
            access: Some(access),
            privileged: false,
        }
    }

    /// The value of a grant that states nothing.
    pub fn present() -> Self {
        Self {
            access: None,
            privileged: false,
        }
    }

    /// The value of a method grant that reaches a privileged method. See [`Self::privileged`].
    pub fn privileged() -> Self {
        Self {
            access: None,
            privileged: true,
        }
    }
}

/// What a grant is a grant of, and the slot of its datum: grants made on two devices for the same
/// thing converge to one datum, and different grants never share one. The keys use only portable
/// ids (drawer ids, cartridge ids, NSIDs, hosts), never a per-node actor, so they mean the same
/// thing on a sister. Each kind is its own datum (see [`GRANT_AUTHORITY`]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GrantKey {
    /// `drawer:<drawerId>`: access to a local drawer.
    Drawer(DrawerId),
    /// `cartridge:<cartridgeId>`: use of the data of another cartridge.
    Cartridge(CartridgeId),
    /// `net:<host[:port]>`: using the network to reach a host.
    Net(NetTarget),
    /// `method:<nsid>`: one method the cartridge may invoke through dispatch.
    Method(String),
    /// `authority:<prefix>`: every method under an NSID authority prefix, or `*`.
    Authority(String),
}

impl GrantKey {
    const DRAWER: &'static str = DrawerGrant::ID;
    const CARTRIDGE: &'static str = CartridgeGrant::ID;
    const NET: &'static str = NetGrant::ID;
    const METHOD: &'static str = MethodGrant::ID;
    const AUTHORITY: &'static str = AuthorityGrant::ID;

    /// The datum tag of this kind of grant.
    pub const fn tag(&self) -> &'static str {
        match self {
            Self::Drawer(_) => Self::DRAWER,
            Self::Cartridge(_) => Self::CARTRIDGE,
            Self::Net(_) => Self::NET,
            Self::Method(_) => Self::METHOD,
            Self::Authority(_) => Self::AUTHORITY,
        }
    }

    /// The key of the datum holding this grant in the cartridge's config document.
    pub fn datum_key(&self) -> DatumKey {
        let slot = match self {
            Self::Drawer(drawer) => drawer.to_string(),
            Self::Cartridge(cartridge) => cartridge.to_string(),
            Self::Net(target) => target.to_string(),
            Self::Method(slot) | Self::Authority(slot) => slot.clone(),
        };
        DatumKey::from_text(self.tag(), slot).expect("a grant key is a valid datum key slot")
    }

    /// The grant a datum key names, `None` when the key is not one of the grant datums or its
    /// slot is not that kind's shape.
    pub fn from_datum_key(key: &DatumKey) -> Option<Self> {
        let slot = std::str::from_utf8(key.slot_bytes()).ok()?;
        Some(match key.tag().as_str() {
            Self::DRAWER => Self::Drawer(slot.parse().ok()?),
            Self::CARTRIDGE => Self::Cartridge(slot.parse().ok()?),
            Self::NET => Self::Net(slot.parse().ok()?),
            Self::METHOD if is_nsid(slot) => Self::Method(slot.to_owned()),
            Self::AUTHORITY if slot == ALL_AUTHORITIES || is_nsid_authority(slot) => {
                Self::Authority(slot.to_owned())
            }
            _ => return None,
        })
    }

    /// Whether `body` has the shape this kind of grant stores: a drawer or cartridge grant states
    /// its access, every other kind states none, and only a method grant may be privileged.
    pub fn accepts(&self, body: &GrantBody) -> bool {
        match self {
            Self::Drawer(_) | Self::Cartridge(_) => body.access.is_some() && !body.privileged,
            Self::Net(_) | Self::Authority(_) => body.access.is_none() && !body.privileged,
            Self::Method(_) => body.access.is_none(),
        }
    }
}

/// A network host and, optionally, a port: what a `#net` permbind names and the slot of a
/// `grant/net` datum, in the syntax of Deno's `--allow-net`: `host` is any port, `host:port` that
/// port only, there is no scheme, and an IPv6 literal is bracketed (`[::1]:8000`). Matching is
/// exact, with no wildcards: the host case-insensitively (it is stored lowercased, and an IPv6
/// literal in its canonical form, so two spellings are one grant), and the port as stated. A
/// request's scheme is only ever compared through its port: `http://host` is `host:80`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NetTarget {
    host: String,
    port: Option<u16>,
}

/// Why text is not a [`NetTarget`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "`{0}` is not a network target: expected `<host>` or `<host>:<port>` with a DNS name, an IPv4 address or a bracketed IPv6 address (`[::1]:8000`), and no scheme; the node's loopback sentinel needs a port"
)]
pub struct InvalidNetTarget(pub String);

impl NetTarget {
    /// The host a guest names to reach the machine's own loopback, which is otherwise
    /// unreachable: a guest's `127.0.0.1` and `localhost` never leave its sandbox, so the
    /// machine's own services are reached through this reserved name only, as wasmCloud's
    /// `host.wasmcloud.internal` does for `wasi:sockets`. `.internal` is the TLD reserved for
    /// private-use names, so no public DNS answers for it. A target names it only with a port,
    /// since a grant of the whole loopback would be too coarse a thing to give by accident; the
    /// node also keeps it closed unless its `net.allow_host_loopback` is set.
    pub const HOST_LOOPBACK: &'static str = "host.daybook.internal";

    /// `host` (any case) and `port`. The host must be a DNS name or an IPv4 address (letters,
    /// digits, `-` and `.`) or an IPv6 address in brackets. `None` is every port.
    pub fn new(host: &str, port: Option<u16>) -> Result<Self, InvalidNetTarget> {
        let invalid = || {
            InvalidNetTarget(match port {
                Some(port) => format!("{host}:{port}"),
                None => host.to_owned(),
            })
        };
        let host = if let Some(inner) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
            let address: std::net::Ipv6Addr = inner.parse().map_err(|_| invalid())?;
            format!("[{address}]")
        } else {
            let valid = !host.is_empty()
                && host
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.')
                && !host.starts_with('.')
                && !host.ends_with('.');
            if !valid {
                return Err(invalid());
            }
            host.to_ascii_lowercase()
        };
        if port == Some(0) || (host == Self::HOST_LOOPBACK && port.is_none()) {
            return Err(invalid());
        }
        Ok(Self { host, port })
    }

    /// The host as a URL spells it: lowercase, an IPv6 literal bracketed.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The one port this names, `None` for every port.
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Whether the host is `localhost` or an address in a range a node never lets a guest dial:
    /// loopback, unspecified, link-local, multicast, private, CGNAT or unique-local. A permbind
    /// for one is refused at admission, since the egress would refuse every request to it.
    pub fn names_this_machine_or_lan(&self) -> bool {
        use std::net::IpAddr;
        let host = self.host.trim_start_matches('[').trim_end_matches(']');
        if host == "localhost" || host.ends_with(".localhost") {
            return true;
        }
        match host.parse::<IpAddr>().map(|address| address.to_canonical()) {
            Ok(IpAddr::V4(v4)) => {
                v4.is_loopback()
                    || v4.is_unspecified()
                    || v4.is_link_local()
                    || v4.is_multicast()
                    || v4.is_broadcast()
                    || v4.is_private()
                    || (v4.octets()[0] == 100 && (64..128).contains(&v4.octets()[1]))
            }
            Ok(IpAddr::V6(v6)) => {
                v6.is_loopback()
                    || v6.is_unspecified()
                    || v6.is_multicast()
                    || (v6.segments()[0] & 0xffc0) == 0xfe80
                    || (v6.segments()[0] & 0xfe00) == 0xfc00
            }
            Err(_) => false,
        }
    }

    /// Whether this target names `host` on `port`, as a request resolved them (the port is the
    /// URL's own or its scheme's default).
    pub fn reaches(&self, host: &str, port: u16) -> bool {
        self.host.eq_ignore_ascii_case(host) && self.port.is_none_or(|own| own == port)
    }

    /// Whether this target, as a grant, covers everything `other`, as a permbind, can reach:
    /// the same host, and either every port or exactly the permbind's one.
    pub fn covers(&self, other: &Self) -> bool {
        self.host == other.host && self.port.is_none_or(|own| other.port == Some(own))
    }
}

impl fmt::Display for NetTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.port {
            Some(port) => write!(formatter, "{}:{port}", self.host),
            None => formatter.write_str(&self.host),
        }
    }
}

impl FromStr for NetTarget {
    type Err = InvalidNetTarget;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidNetTarget(value.to_owned());
        // The port follows the last `:` that is outside the brackets of an IPv6 literal.
        let (host, port) = match value.rfind(':') {
            Some(at) if !value[at..].contains(']') => (&value[..at], Some(&value[at + 1..])),
            _ => (value, None),
        };
        let port = port
            .map(|port| port.parse().map_err(|_| invalid()))
            .transpose()?;
        Self::new(host, port).map_err(|_| invalid())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "`{0}` is not a grant key: expected `drawer:<drawerId>`, `cartridge:<cartridgeId>`, `net:<host[:port]>`, `method:<nsid>` or `authority:<prefix>`"
)]
pub struct InvalidGrantKey(pub String);

impl fmt::Display for GrantKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drawer(drawer) => write!(formatter, "drawer:{drawer}"),
            Self::Cartridge(cartridge) => write!(formatter, "cartridge:{cartridge}"),
            Self::Net(net) => write!(formatter, "net:{net}"),
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
            "cartridge" => target.parse().map(Self::Cartridge).map_err(|_| invalid()),
            "net" => target.parse().map(Self::Net).map_err(|_| invalid()),
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
        Declaration::of::<DrawerGrant>().expect("DrawerGrant declaration is valid"),
        Declaration::of::<CartridgeGrant>().expect("CartridgeGrant declaration is valid"),
        Declaration::of::<NetGrant>().expect("NetGrant declaration is valid"),
        Declaration::of::<MethodGrant>().expect("MethodGrant declaration is valid"),
        Declaration::of::<AuthorityGrant>().expect("AuthorityGrant declaration is valid"),
    ]
}
