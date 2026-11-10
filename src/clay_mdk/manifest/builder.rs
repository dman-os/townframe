//! The typed manifest builder: lowers declaration types ([`crate::decl`]) into the wire
//! [`MicroManifest`], which is unchanged. Each method appends one entry to the list the wire
//! shape holds, so entries come out in call order.
//!
//! Declaration mistakes (a contribution into a collection the manifest never requested for
//! write, an attachment to an undeclared publication, any other [`MicroManifest::validate`]
//! failure) are programming errors and panic at [`ManifestBuilder::build`], which a micro's own
//! snapshot test reaches.
use super::*;
use crate::Attachment;
use crate::CollectionDecl;
use crate::ComponentDecl;
use crate::ConfigDecl;
use crate::DeclaredCollection;
use crate::PackageDecl;

/// Builds a [`MicroManifest`] from declaration types.
pub struct ManifestBuilder {
    manifest: MicroManifest,
}

/// Hands out the attachments of one contribution's row: each call numbers the next attachment
/// and records the publication it refers to, so a row never spells an index.
pub struct Attachments<'a> {
    publications: &'a [PublicationDeclaration],
    declared: Vec<AttachmentDeclaration>,
}

impl Attachments<'_> {
    /// An attachment to the presentation `P` this manifest publishes.
    pub fn own<P: ComponentDecl>(&mut self) -> Attachment {
        self.by_name(P::NAME)
    }

    /// An attachment to a publication named by string: a call publication (calls stay named by
    /// string until they are declared by their `Method` request type).
    pub fn by_name(&mut self, publication: &str) -> Attachment {
        assert!(
            self.publications
                .iter()
                .any(|declared| declared.name == publication),
            "the row attaches {publication:?}, which the manifest does not publish"
        );
        let index = self.declared.len();
        self.declared.push(AttachmentDeclaration {
            index: u32::try_from(index).expect("a row has few attachments"),
            publication: publication.into(),
        });
        Attachment::at(index as u64)
    }
}

impl Default for ManifestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ManifestBuilder {
    /// An empty manifest of the `micro-client` world.
    pub fn new() -> Self {
        Self {
            manifest: MicroManifest {
                entrypoints: Entrypoints::micro_client(),
                publications: vec![],
                requires: vec![],
                configuration_requests: vec![],
                collection_contracts: vec![],
                collection_requests: vec![],
                contributions: vec![],
                extra: BTreeMap::new(),
            },
        }
    }

    /// Publish the presentation `P`.
    pub fn component<P: ComponentDecl>(mut self) -> Self {
        self.manifest.publications.push(PublicationDeclaration {
            name: P::NAME.into(),
            kind: PublicationKind::Presentation,
            contract: None,
        });
        self
    }

    /// Publish a call `name` under `contract`. Calls are named by string until they are
    /// declared by their `Method` request type.
    pub fn call(mut self, name: &str, contract: &str) -> Self {
        self.manifest.publications.push(PublicationDeclaration {
            name: name.into(),
            kind: PublicationKind::Call,
            contract: Some(Nsid::literal(contract)),
        });
        self
    }

    /// Require what another micro publishes (a call or a presentation) under the binding
    /// `binding`, naming the contract by string until calls are declared by their `Method`
    /// request type.
    pub fn requires(mut self, binding: &str, contract: &str, necessity: Necessity) -> Self {
        self.manifest.requires.push(Request {
            binding: binding.into(),
            contract: Nsid::literal(contract),
            access: None,
            necessity,
            resolution: BindingResolution::Configured,
            min_revision: None,
        });
        self
    }

    /// Request read and write access to the facet `C` declares.
    pub fn config<C: ConfigDecl>(mut self) -> Self {
        self.manifest
            .configuration_requests
            .push(ConfigurationRequest {
                binding: C::BINDING.into(),
                facets: vec![FacetAccess {
                    tag: Nsid::literal(C::NSID),
                    key_id: Some(C::KEY.into()),
                    read: true,
                    write: true,
                    create: false,
                    delete: false,
                }],
            });
        self
    }

    /// Declare the collection contract `C`, which this package owns.
    pub fn declares<C: DeclaredCollection>(mut self) -> Self {
        self.manifest.collection_contracts.push(CollectionContract {
            id: Nsid::literal(C::NSID),
            selector_schema: Nsid::literal(C::SELECTOR_SCHEMA),
            key_schema: Nsid::literal(C::KEY_SCHEMA),
            value_schema: Nsid::literal(C::VALUE_SCHEMA),
            write_context_schema: None,
            offered: C::OFFERED.to_vec(),
        });
        self
    }

    /// Request read access to collection `C` under `binding`.
    pub fn reads<C: CollectionDecl>(self, binding: &str, necessity: Necessity) -> Self {
        self.collection_request::<C>(binding, Access::Read, necessity)
    }

    /// Request write access to collection `C` under `binding`.
    pub fn writes<C: CollectionDecl>(self, binding: &str, necessity: Necessity) -> Self {
        self.collection_request::<C>(binding, Access::Write, necessity)
    }

    fn collection_request<C: CollectionDecl>(
        mut self,
        binding: &str,
        access: Access,
        necessity: Necessity,
    ) -> Self {
        self.manifest.collection_requests.push(Request {
            binding: binding.into(),
            contract: Nsid::literal(C::NSID),
            access: Some(access),
            necessity,
            resolution: BindingResolution::Configured,
            min_revision: None,
        });
        self
    }

    /// Declare the row `key` of collection `C`, written through the manifest's write request
    /// for `C`, which must come first. `row` builds the typed row and takes its attachments
    /// from the [`Attachments`] it is handed.
    pub fn contributes<C: CollectionDecl>(
        mut self,
        id: &str,
        key: &str,
        row: impl FnOnce(&mut Attachments) -> C::Value,
    ) -> Self {
        let mut writes = self.manifest.collection_requests.iter().filter(|request| {
            request.contract.as_str() == C::NSID && request.access == Some(Access::Write)
        });
        let binding = writes
            .next()
            .unwrap_or_else(|| panic!("contribution {id:?} needs a write request for {}", C::NSID))
            .binding
            .clone();
        assert!(
            writes.next().is_none(),
            "contribution {id:?} is ambiguous: several write requests for {}",
            C::NSID
        );
        let mut attachments = Attachments {
            publications: &self.manifest.publications,
            declared: vec![],
        };
        let row = row(&mut attachments);
        let attachments = attachments.declared;
        self.manifest.contributions.push(ContributionDeclaration {
            id: id.into(),
            binding,
            key: key.into(),
            payload: serde_json::to_value(row).expect("a declared row is JSON data"),
            attachments,
        });
        self
    }

    /// The manifest. Panics when it is internally inconsistent
    /// ([`MicroManifest::validate`]).
    pub fn build(self) -> MicroManifest {
        self.manifest
            .validate()
            .expect("the manifest builder produced an invalid manifest");
        self.manifest
    }
}

impl Sidecar {
    /// The sidecar of package `P` carrying `manifest`.
    pub fn of<P: PackageDecl>(manifest: MicroManifest) -> Self {
        Self::new(P::ID, P::VERSION, manifest).with_authority(P::AUTHORITY)
    }
}
