//! The typed manifest builder: lowers declaration types ([`crate::decl`]) into the wire
//! [`MicroManifest`], which is unchanged. Each method appends one entry to the list the wire
//! shape holds, so entries come out in call order.
//!
//! Declaration mistakes (a contribution into a collection the manifest never requested for
//! write, a link to an undeclared endpoint, any other [`MicroManifest::validate`]
//! failure) are programming errors and panic at [`ManifestBuilder::build`], which a micro's own
//! snapshot test reaches.
use super::*;
use crate::CollectionDecl;
use crate::ComponentDecl;
use crate::ConfigDecl;
use crate::DeclaredCollection;
use crate::Link;
use crate::PackageDecl;

/// Builds a [`MicroManifest`] from declaration types.
pub struct ManifestBuilder {
    manifest: MicroManifest,
}

/// Hands out the links of one contribution's row: each call numbers the next link and records
/// the endpoint it refers to, so a row never spells an index.
pub struct Links<'a> {
    endpoints: &'a BTreeMap<String, Endpoint>,
    declared: Vec<LinkDeclaration>,
}

impl Links<'_> {
    /// A link to the view `P` this manifest serves.
    pub fn own<P: ComponentDecl>(&mut self) -> Link {
        self.by_name(P::NAME)
    }

    /// A link to an endpoint named by string: a callback or a method (those stay named by
    /// string until they are declared by their `Method` request type).
    pub fn by_name(&mut self, endpoint: &str) -> Link {
        assert!(
            self.endpoints.contains_key(endpoint),
            "the row links {endpoint:?}, which the manifest does not declare"
        );
        let index = self.declared.len();
        self.declared.push(LinkDeclaration {
            index: u32::try_from(index).expect("a row has few links"),
            endpoint: endpoint.into(),
        });
        Link::at(index as u64)
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
                endpoints: BTreeMap::new(),
                requires: vec![],
                configuration_requests: vec![],
                collection_contracts: vec![],
                collection_requests: vec![],
                contributions: vec![],
                extra: BTreeMap::new(),
            },
        }
    }

    /// Serve the view `P`, under the contract `P::IMPLEMENTS` names when another micro requires
    /// it.
    pub fn view<P: ComponentDecl>(self) -> Self {
        self.endpoint(
            P::NAME,
            Endpoint::View {
                implements: P::IMPLEMENTS.map(Nsid::literal),
            },
        )
    }

    /// Serve a method `name` registered under the NSID `nsid`. Methods are named by string
    /// until they are declared by their `Method` request type.
    pub fn method(self, name: &str, nsid: &str) -> Self {
        self.endpoint(
            name,
            Endpoint::Method {
                nsid: Nsid::literal(nsid),
            },
        )
    }

    /// Serve a callback `name` implementing the contract `implements`: it is never
    /// registered and is reached only through a link a row carries.
    pub fn callback(self, name: &str, implements: &str) -> Self {
        self.endpoint(
            name,
            Endpoint::Callback {
                implements: Nsid::literal(implements),
            },
        )
    }

    fn endpoint(mut self, name: &str, endpoint: Endpoint) -> Self {
        let previous = self.manifest.endpoints.insert(name.into(), endpoint);
        assert!(previous.is_none(), "the manifest serves {name:?} twice");
        self
    }

    /// Require what another micro serves (a method or a view) under the binding
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
    /// for `C`, which must come first. `row` builds the typed row and takes its links from the
    /// [`Links`] it is handed.
    pub fn contributes<C: CollectionDecl>(
        mut self,
        id: &str,
        key: &str,
        row: impl FnOnce(&mut Links) -> C::Value,
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
        let mut links = Links {
            endpoints: &self.manifest.endpoints,
            declared: vec![],
        };
        let row = row(&mut links);
        let links = links.declared;
        self.manifest.contributions.push(ContributionDeclaration {
            id: id.into(),
            binding,
            key: key.into(),
            value: serde_json::to_value(row).expect("a declared row is JSON data"),
            links,
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
