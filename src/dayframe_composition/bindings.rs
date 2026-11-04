//! Collection binding resolution (ADR 031 §11): turning a manifest's collection requests plus
//! the micro's stored binding datum into the handles its factory reads at `initialize`.
//!
//! A micro declares requests per local binding naming a collection *contract*. Configuration
//! is final: a value stored in the binding datum wins, valid or not. Only when it says nothing
//! about a `Configured` request does the manifest's default apply, which binds a contract that
//! exactly one declared collection implements. A request that resolves to nothing leaves the
//! binding unbound; if it was required the micro is not startable until configuration changes.
use clay_docs::FacetRoute;
use clay_host::MicroId;
use clay_mdk::BindingResolution;
use clay_mdk::CollectionAccess;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::host::BoundCollection;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// The binding datum shares the install datum's document, one facet per micro.
pub(crate) const BINDINGS_TAG: &str = "clay.micro.bindings";

pub(crate) fn bindings_route(micro: &MicroId) -> FacetRoute {
    FacetRoute {
        document: super::INSTALLS_DOCUMENT.into(),
        branch: super::INSTALLS_BRANCH.into(),
        tag: BINDINGS_TAG.into(),
        key_id: micro.as_str().into(),
    }
}

/// One micro's explicit binding configuration: binding name -> the declared collection id it is
/// bound to, or `null` to leave it deliberately unbound. A binding with no entry is up to the
/// manifest's defaults. Unknown fields ride along so a datum written by a newer build survives
/// a rewrite by this one.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct BindingsDatum {
    pub bindings: BTreeMap<String, Option<String>>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl BindingsDatum {
    /// Only this crate writes the document, so a malformed datum is a bug.
    pub fn decode(value: serde_json::Value) -> Self {
        serde_json::from_value(value).expect("the bindings facet holds only binding datums")
    }

    pub fn encode(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("a binding datum serializes")
    }
}

/// The declared collections by the contract they implement. A contract may later have several
/// declared instances; today a declared collection's id is its contract's id.
#[derive(Default)]
pub(crate) struct DeclaredCollections(BTreeMap<String, BTreeSet<String>>);

impl DeclaredCollections {
    pub fn declare(&mut self, contract: &str, collection: &str) {
        self.0
            .entry(contract.to_owned())
            .or_default()
            .insert(collection.to_owned());
    }

    fn implements(&self, contract: &str, collection: &str) -> bool {
        self.0
            .get(contract)
            .is_some_and(|collections| collections.contains(collection))
    }

    /// The one collection implementing `contract`, when there is exactly one.
    fn sole(&self, contract: &str) -> Option<&str> {
        let mut collections = self.0.get(contract)?.iter();
        match (collections.next(), collections.next()) {
            (Some(only), None) => Some(only),
            _ => None,
        }
    }
}

/// What resolution decided for one micro.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Resolution {
    pub bound: BTreeMap<String, BoundCollection>,
    /// Required bindings that resolved to nothing.
    pub unresolved: Vec<String>,
}

pub(crate) fn resolve(
    manifest: &MicroManifest,
    datum: Option<&BindingsDatum>,
    declared: &DeclaredCollections,
) -> Resolution {
    let mut resolution = Resolution::default();
    for request in &manifest.collection_requests {
        let configured = datum.and_then(|datum| datum.bindings.get(&request.binding));
        let target = match configured {
            // Configuration is final: an entry that names a collection the contract does not
            // allow leaves the binding unbound rather than falling back to the default.
            Some(Some(id)) => declared
                .implements(&request.contract, id)
                .then_some(id.as_str()),
            Some(None) => None,
            // A `Configured` request defaults; an `AtRuntime` one (or a resolution this build
            // does not know) is only ever bound by an explicit grant in the datum.
            None if request.resolution == BindingResolution::Configured => {
                declared.sole(&request.contract)
            }
            None => None,
        };
        // Access this build does not know cannot be granted.
        let bound = target.and_then(|id| match request.access {
            CollectionAccess::Read => Some(BoundCollection::read(id)),
            CollectionAccess::Write => Some(BoundCollection::write(id)),
            CollectionAccess::Other(_) => None,
        });
        match bound {
            Some(bound) => {
                resolution.bound.insert(request.binding.clone(), bound);
            }
            // Only a known `Optional` may stay unbound. An unknown necessity likely comes from
            // a newer manifest format, and blocking the start is the conservative reading.
            None if request.necessity != Necessity::Optional => {
                resolution.unresolved.push(request.binding.clone());
            }
            None => {}
        }
    }
    resolution
}

#[cfg(test)]
mod tests {
    use super::*;
    use clay_mdk::CollectionRequest;
    use clay_mdk::Entrypoints;
    use clay_mdk::MANIFEST_SCHEMA;

    fn request(
        binding: &str,
        necessity: Necessity,
        resolution: BindingResolution,
    ) -> CollectionRequest {
        CollectionRequest {
            binding: binding.into(),
            contract: "tallies".into(),
            access: CollectionAccess::Write,
            necessity,
            resolution,
        }
    }

    fn manifest(requests: Vec<CollectionRequest>) -> MicroManifest {
        MicroManifest {
            schema: MANIFEST_SCHEMA,
            id: clay_mdk::MicroId::new("test.micro"),
            package_version: 1,
            entrypoints: Entrypoints::micro_client(),
            publications: vec![],
            requires: vec![],
            configuration_requests: vec![],
            collection_contracts: vec![],
            collection_requests: requests,
            contributions: vec![],
            extra: BTreeMap::new(),
        }
    }

    fn declared(collections: &[&str]) -> DeclaredCollections {
        let mut declared = DeclaredCollections::default();
        for collection in collections {
            declared.declare("tallies", collection);
        }
        declared
    }

    fn datum(entries: &[(&str, Option<&str>)]) -> BindingsDatum {
        BindingsDatum {
            bindings: entries
                .iter()
                .map(|(binding, id)| ((*binding).to_owned(), id.map(str::to_owned)))
                .collect(),
            extra: BTreeMap::new(),
        }
    }

    #[test]
    fn a_contract_with_exactly_one_declared_collection_binds_by_default() {
        let manifest = manifest(vec![request(
            "book",
            Necessity::Required,
            BindingResolution::Configured,
        )]);
        let resolution = resolve(&manifest, None, &declared(&["a"]));
        assert_eq!(
            resolution.bound,
            BTreeMap::from([("book".to_owned(), BoundCollection::write("a"))])
        );
        assert!(resolution.unresolved.is_empty());
    }

    #[test]
    fn ambiguity_and_absence_leave_the_default_unbound() {
        let manifest = manifest(vec![request(
            "book",
            Necessity::Required,
            BindingResolution::Configured,
        )]);
        for collections in [&["a", "b"][..], &[][..]] {
            let resolution = resolve(&manifest, None, &declared(collections));
            assert!(resolution.bound.is_empty());
            assert_eq!(resolution.unresolved, ["book"]);
        }
    }

    #[test]
    fn explicit_configuration_wins_over_the_default() {
        let manifest = manifest(vec![request(
            "book",
            Necessity::Required,
            BindingResolution::Configured,
        )]);
        let resolution = resolve(
            &manifest,
            Some(&datum(&[("book", Some("b"))])),
            &declared(&["a", "b"]),
        );
        assert_eq!(resolution.bound["book"], BoundCollection::write("b"));
        // Configuration is final: a collection the contract does not allow is not replaced by
        // the default, and an explicit null leaves the binding unbound.
        for entry in [Some("elsewhere"), None] {
            let resolution = resolve(
                &manifest,
                Some(&datum(&[("book", entry)])),
                &declared(&["a"]),
            );
            assert!(resolution.bound.is_empty());
            assert_eq!(resolution.unresolved, ["book"]);
        }
    }

    #[test]
    fn an_unknown_necessity_blocks_like_a_required_one() {
        let manifest = manifest(vec![request(
            "book",
            Necessity::Other("eventually".into()),
            BindingResolution::Configured,
        )]);
        let resolution = resolve(&manifest, None, &declared(&[]));
        assert_eq!(resolution.unresolved, ["book"]);
    }

    #[test]
    fn an_optional_binding_may_stay_unbound_and_a_runtime_one_is_never_defaulted() {
        let manifest = manifest(vec![
            request(
                "optional",
                Necessity::Optional,
                BindingResolution::Configured,
            ),
            request("later", Necessity::Required, BindingResolution::AtRuntime),
        ]);
        let resolution = resolve(
            &manifest,
            Some(&datum(&[("optional", None)])),
            &declared(&["a"]),
        );
        assert!(resolution.bound.is_empty());
        assert_eq!(resolution.unresolved, ["later"]);
        // An explicit grant in the datum arrives as the same handle.
        let resolution = resolve(
            &manifest,
            Some(&datum(&[("later", Some("a"))])),
            &declared(&["a"]),
        );
        assert_eq!(resolution.bound["later"], BoundCollection::write("a"));
        assert!(resolution.unresolved.is_empty());
    }
}
