//! The value a facet holds that its own schema does not describe.

use indexmap::IndexMap;
use serde_json::Value;

/// The members of an object that its declaration does not name.
///
/// An open object carries one of these as its `#[serde(flatten)]` field, and that field is the only
/// thing that makes `schemars` emit `additionalProperties: true`. An `IndexMap` rather than
/// `serde_json::Map` so a pass-through keeps the order a peer sent.
pub type Extra = IndexMap<String, Value>;
