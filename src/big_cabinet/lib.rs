pub mod authority;
pub mod document;
pub mod index;
pub mod schema;

pub use authority::{
    Actor, Authority, AuthorityError, AuthorityHandler, AuthorityProtocol, AuthorityService,
    AuthorityServiceError, CapId, Capability, Rights, boot,
};
pub use document::{
    DocumentAccess, DocumentsConfig, DocumentsError, DocumentsHandler, DocumentsProtocol,
    DocumentsRepo, DocumentsService, LedgerAccess, boot as boot_documents,
};
pub use index::facet_set::FacetSetStore;
pub use schema::{
    SchemaHandle, SchemaRegistration, SchemaRegistrationGroup, SchemaRepo, SchemaRepoError,
};
