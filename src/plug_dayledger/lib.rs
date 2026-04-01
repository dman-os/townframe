mod interlude {
    pub use utils_rs::prelude::*;
}

pub mod types;

use daybook_types::manifest::{FacetManifest, PlugManifest};

pub fn plug_manifest() -> PlugManifest {
    use crate::types::{Account, Claim, DayledgerFacetTag, LedgerMeta, Txn};

    PlugManifest {
        namespace: "daybook".into(),
        name: "dayledger".into(),
        version: "0.0.1".parse().unwrap(),
        title: "Day Ledger".into(),
        desc: "Personal accounting facets and ledger data model".into(),
        facets: vec![
            FacetManifest {
                key_tag: DayledgerFacetTag::Claim.as_str().into(),
                value_schema: schemars::schema_for!(Claim),
                display_config: Default::default(),
                references: vec![],
            },
            FacetManifest {
                key_tag: DayledgerFacetTag::Txn.as_str().into(),
                value_schema: schemars::schema_for!(Txn),
                display_config: Default::default(),
                references: vec![],
            },
            FacetManifest {
                key_tag: DayledgerFacetTag::Account.as_str().into(),
                value_schema: schemars::schema_for!(Account),
                display_config: Default::default(),
                references: vec![],
            },
            FacetManifest {
                key_tag: DayledgerFacetTag::LedgerMeta.as_str().into(),
                value_schema: schemars::schema_for!(LedgerMeta),
                display_config: Default::default(),
                references: vec![],
            },
        ],
        local_states: Default::default(),
        dependencies: Default::default(),
        routines: Default::default(),
        wflow_bundles: Default::default(),
        commands: Default::default(),
        inits: Default::default(),
        processors: Default::default(),
    }
}
