//! What the listing methods (`ListDocs`, `ListKnownDrawers`, `ListDrawers`) answer: one row type
//! per listing and the object body that carries the rows.

use super::Drawer;
use super::DrawerAccess as Access;
use super::DrawerLayout;
use super::protocol::DrawerId;
use crate::dmeta::DocId;
use serde::Deserialize;
use serde::Serialize;

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedDoc {
    pub doc_id: DocId,
    pub access: Access,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<DrawerId>,
}

/// The result of [`ListDocs`](super::protocol::ListDocs).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedDocs {
    pub docs: Vec<ListedDoc>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnownDrawer {
    pub drawer_id: DrawerId,
    pub mirror: bool,
}

/// The result of [`ListKnownDrawers`](super::protocol::ListKnownDrawers).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnownDrawers {
    pub drawers: Vec<KnownDrawer>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedDrawer {
    pub drawer_id: DrawerId,
    pub layout: DrawerLayout,
    pub descriptor: Drawer,
    pub access: Access,
}

/// The result of [`ListDrawers`](super::protocol::ListDrawers).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedDrawers {
    pub drawers: Vec<ListedDrawer>,
}
