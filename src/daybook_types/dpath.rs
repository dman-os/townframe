//! Dpaths — daybook's path-shaped object labels (FDR 001).
//!
//! A dpath is a UTF-8 label with path syntax (`/inbox/hello.md`). It is *not* a
//! filesystem position and carries no uniqueness constraint: any number of
//! documents may claim the same dpath, concurrently and from different devices.
//! The real filesystem mapping is a *derived* function of the claimant set.
//!
//! This module is the pure addressing core:
//!
//! - [`label::Dpath`]: the label itself, parse/validate per FDR 001 §1 (byte-exact
//!   UTF-8, no case folding, no normalization, **no reserved namespaces**).
//! - [`facet::DpathFacet`]: the facet value that declares a claim (FDR 001 §2).
//! - [`derive::derive_tree`]: the deterministic dpath → real-path mapping implementing
//!   the collision rules 1–3 (FDR 001 §3), the materialized surfaces of §5
//!   (`/by-id`, checkout metadata dirs), and conflicts-as-opinions (§4).
//! - [`naming::DpathNamePolicy`]: the collision naming knobs (FDR 001 Q1, §3).
//!
//! Nothing here touches automerge, the checkout store, or the real filesystem:
//! the input is a list of claims, the output is a tree of *derived* entries
//! with provenance. Two things are deliberately out of scope:
//!
//! - **binding stickiness** (FDR 001 §3 "binding stability"): "the file at this
//!   path today is at this path tomorrow" is a per-checkout property held by
//!   the transactional layer (ADR 011). This module is the *fresh* derivation.
//! - **lens entry-sets** (FDR 001 §9, ADR 012): one dpath maps to an entry
//!   *set*; only collisions *between* claimants are resolved here.

pub mod claim;
pub mod derive;
pub mod facet;
pub mod label;
pub mod reserved;

mod naming;

pub use claim::{DpathClaim, RawClaim};
pub use derive::{
    DerivedClaim, DerivedDirKind, DerivedEntry, DerivedTree, DpathRejection, derive_tree,
};
pub use facet::{DpathFacet, DpathTarget, DpathTargetIssue};
pub use label::{DPATH_FACET_TAG, Dpath, DpathError};
pub use naming::DpathNamePolicy;
pub use reserved::{BY_ID_SURFACE_NAME, ReservedKind, ReservedSurface};
