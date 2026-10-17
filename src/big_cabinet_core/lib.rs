// The declaration macro names this crate by path so that it resolves from any user of it; this
// alias is what makes the same expansion resolve inside the crate's own tests.
extern crate self as big_cabinet_core;

#[cfg(feature = "schemars")]
pub mod facet;

#[cfg(test)]
mod panproto_schema_store_poc;
