//! Collection binding resolution (ADR 031 §11): turning a manifest's collection requests plus
//! the micro's stored binding datum into the handles its factory reads at `initialize`.
//!
//! A micro declares requests per local binding naming a collection *contract*. Configuration
//! is final: a value stored in the binding datum wins, valid or not. Only when it says nothing
//! about a `Configured` request does the manifest's default apply, which binds a contract that
//! exactly one declared collection implements. A request that resolves to nothing leaves the
//! binding unbound; if it was required the micro is not startable until configuration changes.
//!
//! A stored configuration records what it granted (the request's contract and access when the
//! user bound it) and holds only for requests that grant still covers (ADR 028 §3): a package
//! update that widens a request under the same binding name must be configured again, not
//! inherit the old consent.
use clay_docs::DatumRoute;
use clay_mdk::host::BoundCollection;
use clay_mdk::Access;
use clay_mdk::BindingResolution;
use clay_mdk::MicroId;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::Nsid;
use clay_mdk::Request;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// The binding datum shares the install datum's document, one datum per micro.
pub(crate) const BINDINGS_TAG: &str = "clay.micro.bindings";

pub(crate) fn bindings_route(micro: &MicroId) -> DatumRoute {
    DatumRoute {
        document: super::INSTALLS_DOCUMENT.into(),
        branch: super::INSTALLS_BRANCH.into(),
        tag: BINDINGS_TAG.into(),
        slot: micro.as_str().into(),
    }
}

/// One configured binding: the declared collection it is bound to and the request it was
/// bound under, which is what the user consented to. Resolution honors it only while the
/// manifest's current request is covered by `contract` and `access`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct BindingGrant {
    pub collection: Nsid,
    /// The contract NSID the request named when it was granted.
    pub contract: Nsid,
    pub access: Access,
}

impl BindingGrant {
    /// Grant `collection` to `request`, which names the contract and access being consented to.
    pub fn of(request: &Request, collection: Nsid) -> Self {
        Self {
            collection,
            contract: request.contract.clone(),
            access: request
                .access
                .clone()
                .expect("a collection request declares an access"),
        }
    }

    /// Whether `request` asks for no more than this grant gave: the same contract, and the
    /// same access or a narrower one (write implies read). A contract is an NSID with no
    /// order, so a different one is a different grant; an access this build does not know is
    /// covered only by itself.
    fn covers(&self, request: &Request) -> bool {
        self.contract == request.contract
            && match &request.access {
                Some(requested) => {
                    *requested == self.access
                        || (self.access == Access::Write && *requested == Access::Read)
                }
                None => false,
            }
    }
}

/// One micro's explicit binding configuration: binding name -> the grant that bound it, or
/// `null` to leave it deliberately unbound. A binding with no entry is up to the manifest's
/// defaults. Unknown fields ride along so a datum written by a newer build survives a rewrite
/// by this one.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct BindingsDatum {
    pub bindings: BTreeMap<String, Option<BindingGrant>>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl BindingsDatum {
    /// Only this crate writes the document, so a malformed datum is a bug.
    pub fn decode(value: serde_json::Value) -> Self {
        serde_json::from_value(value).expect("the bindings datum holds only binding datums")
    }

    pub fn encode(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("a binding datum serializes")
    }
}

/// The declared collections by the contract they implement. A contract may later have several
/// declared instances; today a declared collection's id is its contract's id.
#[derive(Default)]
pub(crate) struct DeclaredCollections(BTreeMap<Nsid, BTreeSet<Nsid>>);

impl DeclaredCollections {
    pub fn declare(&mut self, contract: &Nsid, collection: &Nsid) {
        self.0
            .entry(contract.clone())
            .or_default()
            .insert(collection.clone());
    }

    fn implements(&self, contract: &Nsid, collection: &Nsid) -> bool {
        self.0
            .get(contract)
            .is_some_and(|collections| collections.contains(collection))
    }

    /// The one collection implementing `contract`, when there is exactly one.
    fn sole(&self, contract: &Nsid) -> Option<&Nsid> {
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
            // allow, or whose grant no longer covers the request (it widened since), leaves
            // the binding unbound rather than falling back to the default or inheriting the
            // old consent.
            Some(Some(grant)) => (grant.covers(request)
                && declared.implements(&request.contract, &grant.collection))
            .then_some(&grant.collection),
            Some(None) => None,
            // A `Configured` request defaults; an `AtRuntime` one (or a resolution this build
            // does not know) is only ever bound by an explicit grant in the datum.
            None if request.resolution == BindingResolution::Configured => {
                declared.sole(&request.contract)
            }
            None => None,
        };
        // Access this build does not know cannot be granted.
        let bound = target.and_then(|id| match &request.access {
            Some(Access::Read) => Some(BoundCollection::read(id.clone())),
            Some(Access::Write) => Some(BoundCollection::write(id.clone())),
            Some(Access::Other(_)) | None => None,
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
    use clay_mdk::Entrypoints;

    fn request(binding: &str, necessity: Necessity, resolution: BindingResolution) -> Request {
        Request {
            binding: binding.into(),
            contract: nsid("tallies"),
            access: Some(Access::Write),
            necessity,
            resolution,
            min_revision: None,
        }
    }

    fn manifest(requests: Vec<Request>) -> MicroManifest {
        MicroManifest {
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

    fn nsid(name: &str) -> Nsid {
        Nsid::literal(&format!("org.example.test.{name}"))
    }

    fn declared(collections: &[&str]) -> DeclaredCollections {
        let mut declared = DeclaredCollections::default();
        for collection in collections {
            declared.declare(&nsid("tallies"), &nsid(collection));
        }
        declared
    }

    fn datum(entries: &[(&str, Option<&str>)]) -> BindingsDatum {
        BindingsDatum {
            bindings: entries
                .iter()
                .map(|(binding, id)| {
                    let grant = id.map(|id| BindingGrant {
                        collection: nsid(id),
                        contract: nsid("tallies"),
                        access: Access::Write,
                    });
                    ((*binding).to_owned(), grant)
                })
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
            BTreeMap::from([("book".to_owned(), BoundCollection::write(nsid("a")))])
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
        assert_eq!(resolution.bound["book"], BoundCollection::write(nsid("b")));
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
        assert_eq!(resolution.bound["later"], BoundCollection::write(nsid("a")));
        assert!(resolution.unresolved.is_empty());
    }

    /// A stored grant holds only for requests it still covers (ADR 028 §3): the same request,
    /// or a narrower access, keeps it; a widened access or another contract under the same
    /// binding name is unresolved rather than inheriting the old consent, while a binding
    /// with no stored grant still defaults.
    #[test]
    fn a_widened_request_does_not_inherit_the_granted_binding() {
        let granted = |access: Access, contract: &str| BindingGrant {
            collection: nsid("a"),
            contract: nsid(contract),
            access,
        };
        let stored = |grant: BindingGrant| BindingsDatum {
            bindings: BTreeMap::from([("book".to_owned(), Some(grant))]),
            extra: BTreeMap::new(),
        };
        let asking = |access: Access, contract: &str| {
            manifest(vec![Request {
                access: Some(access),
                contract: nsid(contract),
                ..request("book", Necessity::Required, BindingResolution::Configured)
            }])
        };
        let mut declared = declared(&["a"]);
        // The same collection also implements the other contract, so only the grant decides.
        declared.declare(&nsid("other"), &nsid("a"));

        // Identical and narrowed requests keep the grant, narrowed ones at the narrower access.
        for (given, asked) in [
            (Access::Write, Access::Write),
            (Access::Read, Access::Read),
            (Access::Write, Access::Read),
        ] {
            let resolution = resolve(
                &asking(asked.clone(), "tallies"),
                Some(&stored(granted(given, "tallies"))),
                &declared,
            );
            assert!(resolution.unresolved.is_empty());
            assert_eq!(
                resolution.bound["book"],
                match asked {
                    Access::Read => BoundCollection::read(nsid("a")),
                    _ => BoundCollection::write(nsid("a")),
                }
            );
        }

        // Widened access, another contract, or an access this build does not know that
        // differs: unresolved, and the stale grant is not bound.
        for (given, given_contract, asked, asked_contract) in [
            (Access::Read, "tallies", Access::Write, "tallies"),
            (Access::Write, "tallies", Access::Write, "other"),
            (
                Access::Read,
                "tallies",
                Access::Other("admin".into()),
                "tallies",
            ),
            (
                Access::Other("admin".into()),
                "tallies",
                Access::Read,
                "tallies",
            ),
        ] {
            let resolution = resolve(
                &asking(asked, asked_contract),
                Some(&stored(granted(given, given_contract))),
                &declared,
            );
            assert!(resolution.bound.is_empty());
            assert_eq!(resolution.unresolved, ["book"]);
        }

        // No stored grant: the default still applies, widened request or not.
        let resolution = resolve(&asking(Access::Write, "tallies"), None, &declared);
        assert_eq!(resolution.bound["book"], BoundCollection::write(nsid("a")));
    }
}
