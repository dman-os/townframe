// FIXME: these should be tied to triage.rs?
// right now, we can't use these from routines since they
// head that the routine is working on might be an old one
// compared to the index

pub mod facet_ref;
pub mod facet_set;

pub use facet_ref::{
    DocFacetRefEdge, DocFacetRefIndexEvent, DocFacetRefIndexRepo, DocFacetRefIndexStopToken,
};
pub use facet_set::{
    DocFacetSetIndexEvent, DocFacetSetIndexRepo, DocFacetSetIndexStopToken, DocFacetTagMembership,
};
