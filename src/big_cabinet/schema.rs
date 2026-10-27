//! Known facet-schema configuration for one Cabinet embedding.
//!
//! A schema repository owns the embedder-provided Lexicon document and the
//! validated carried sidecar bundle together. It does not discover, publish,
//! fetch, or compare schemas. Unknown facets remain readable as best-effort
//! raw data, but writes require an explicitly configured known facet.
//!
//! The facets Cabinet itself maintains — dmeta, branch, drawer — are always registered, before any embedder group,
//! so an embedder cannot shadow them with its own definition. They are declared in Rust, so their
//! sidecar is the codec's authority by construction, and the Lexicon published for a core facet is
//! the projection of that same declaration. Core facets go through the same Lexicon admission path
//! as an embedder-registered pair — the projected Lexicon is checked against the sidecar and the
//! compiled schema is the overlay admission produces — so there is exactly one compilation path a
//! stored value is ever read against.

use big_cabinet_core::dmeta::BranchPath;
use big_cabinet_core::dmeta::Dmeta;
use big_cabinet_core::drawer::{
    Drawer, DrawerChildEntry, DrawerDocEntry, DrawerRoster, KnownDrawerEntry, SelfDocLayout,
    SingleGroupLayout,
};
use big_cabinet_core::facet::{
    Bundle, CompiledSchema, Declaration, LexiconAdmissionError, Universe, UniverseError,
};
use serde_json::Value;
use std::sync::LazyLock;
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;

static CORE: LazyLock<Universe> = LazyLock::new(|| {
    Universe::collect([
        Declaration::of::<Dmeta>().expect("dmeta is a well-formed facet declaration"),
        Declaration::of::<BranchPath>().expect("branch is a well-formed facet declaration"),
        Declaration::of::<Drawer>().expect("drawer is a well-formed facet declaration"),
        // A published union's members are carried as their own lexicons at admission, whether or
        // not the union is open.
        Declaration::of::<SelfDocLayout>()
            .expect("self-doc layout is a well-formed member declaration"),
        Declaration::of::<SingleGroupLayout>()
            .expect("single-group layout is a well-formed member declaration"),
        Declaration::of::<DrawerRoster>()
            .expect("drawer roster is a well-formed facet declaration"),
        Declaration::of::<DrawerDocEntry>()
            .expect("drawer doc entry is a well-formed facet declaration"),
        Declaration::of::<DrawerChildEntry>()
            .expect("drawer child entry is a well-formed facet declaration"),
        Declaration::of::<KnownDrawerEntry>()
            .expect("known drawer entry is a well-formed facet declaration"),
    ])
});

fn core_schemas() -> impl Iterator<Item = SchemaHandle> {
    let bundle = Bundle::new(&CORE, Vec::new());
    let sidecar = Arc::new(
        OwnedSidecar::from_bundle(&bundle).expect("core facet declarations form a valid bundle"),
    );
    let lexicons = bundle.lexicons().expect("core facets publish lexicons");
    (0..lexicons.len()).map(move |index| {
        let facet = &CORE.facets[index];
        let lexicon = &lexicons[index];
        assert_eq!(facet.id, lexicon.id, "a bundle projects facets in order");
        let carried: Vec<&Value> = lexicons
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(_, other)| &other.file)
            .collect();
        let compiled = bundle
            .validate_lexicon(&facet.id, &lexicon.file, &carried)
            .expect("a core facet's declaration admits its own published lexicon");
        SchemaHandle {
            inner: Arc::new(KnownSchema {
                facet_tag: facet.id.clone(),
                lexicon: lexicon.file.clone(),
                sidecar: Some(Arc::clone(&sidecar)),
                compiled,
            }),
        }
    })
}

pub struct SchemaRegistration {
    pub facet_tag: String,
    pub lexicon: Value,
}

impl SchemaRegistration {
    pub fn new(facet_tag: impl Into<String>, lexicon: Value) -> Self {
        Self {
            facet_tag: facet_tag.into(),
            lexicon,
        }
    }
}

pub struct SchemaRegistrationGroup<'a> {
    pub sidecar: Option<Bundle<'a>>,
    pub registrations: Vec<SchemaRegistration>,
    pub carried_lexicons: Vec<Value>,
}

impl<'a> SchemaRegistrationGroup<'a> {
    pub fn new(
        sidecar: Option<Bundle<'a>>,
        registrations: impl IntoIterator<Item = SchemaRegistration>,
        carried_lexicons: Vec<Value>,
    ) -> Self {
        Self {
            sidecar,
            registrations: registrations.into_iter().collect(),
            carried_lexicons,
        }
    }
}

#[derive(Debug, Error)]
pub enum SchemaRepoError {
    #[error("known facet schema tag is empty")]
    EmptyFacetTag,
    #[error("known facet schema tag `{0}` is configured more than once")]
    DuplicateFacetTag(String),
    #[error("sidecar bundle is invalid: {source}")]
    SidecarInvalid {
        #[source]
        source: UniverseError,
    },
    #[error("schema pair for facet `{tag}` is invalid: {source}")]
    Admission {
        tag: String,
        #[source]
        source: Box<LexiconAdmissionError>,
    },
    #[error("facet `{tag}` is not known to this SchemaRepo")]
    UnknownFacet { tag: String },
}

#[derive(Debug, Clone)]
struct OwnedSidecar {
    plug: Universe,
    dependencies: Vec<Universe>,
}

impl OwnedSidecar {
    fn from_bundle(bundle: &Bundle<'_>) -> Result<Self, SchemaRepoError> {
        bundle
            .validate()
            .map_err(|source| SchemaRepoError::SidecarInvalid { source })?;
        Ok(Self {
            plug: bundle.plug.clone(),
            dependencies: bundle
                .dependencies
                .iter()
                .map(|dependency| (*dependency).clone())
                .collect(),
        })
    }

    fn borrowed(&self) -> Bundle<'_> {
        Bundle::new(&self.plug, self.dependencies.iter().collect())
    }
}

#[derive(Debug)]
struct KnownSchema {
    facet_tag: String,
    lexicon: Value,
    sidecar: Option<Arc<OwnedSidecar>>,
    compiled: CompiledSchema,
}

#[derive(Debug, Clone)]
pub struct SchemaHandle {
    inner: Arc<KnownSchema>,
}

impl SchemaHandle {
    pub fn facet_tag(&self) -> &str {
        &self.inner.facet_tag
    }
    pub fn lexicon(&self) -> &Value {
        &self.inner.lexicon
    }
    pub fn compiled(&self) -> &CompiledSchema {
        &self.inner.compiled
    }
    pub fn sidecar(&self) -> Option<Bundle<'_>> {
        self.inner
            .sidecar
            .as_ref()
            .map(|sidecar| sidecar.borrowed())
    }
}

#[derive(Debug)]
pub struct SchemaRepo {
    known: BTreeMap<String, SchemaHandle>,
}

impl SchemaRepo {
    pub fn open<'a>(
        groups: impl IntoIterator<Item = SchemaRegistrationGroup<'a>>,
    ) -> Result<Self, SchemaRepoError> {
        let mut known: BTreeMap<String, SchemaHandle> = core_schemas()
            .map(|handle| (handle.facet_tag().to_owned(), handle))
            .collect();
        let empty = Universe::default();
        for group in groups {
            let mut tags = std::collections::BTreeSet::new();
            for registration in &group.registrations {
                if !tags.insert(&registration.facet_tag)
                    || known.contains_key(&registration.facet_tag)
                {
                    return Err(SchemaRepoError::DuplicateFacetTag(
                        registration.facet_tag.clone(),
                    ));
                }
            }
            let sidecar = group
                .sidecar
                .as_ref()
                .map(OwnedSidecar::from_bundle)
                .transpose()?
                .map(Arc::new);
            let borrowed = sidecar.as_ref().map_or_else(
                || Bundle::new(&empty, Vec::new()),
                |sidecar| sidecar.borrowed(),
            );
            for registration in &group.registrations {
                let tag = &registration.facet_tag;
                if tag.is_empty() {
                    return Err(SchemaRepoError::EmptyFacetTag);
                }
                if known.contains_key(tag) {
                    return Err(SchemaRepoError::DuplicateFacetTag(tag.clone()));
                }
                let carried: Vec<_> = group
                    .carried_lexicons
                    .iter()
                    .chain(
                        group
                            .registrations
                            .iter()
                            .filter(|other| !std::ptr::eq(*other, registration))
                            .map(|other| &other.lexicon),
                    )
                    .collect();
                let compiled = borrowed
                    .validate_lexicon(tag, &registration.lexicon, &carried)
                    .map_err(|source| SchemaRepoError::Admission {
                        tag: tag.clone(),
                        source: Box::new(source),
                    })?;
                known.insert(
                    tag.clone(),
                    SchemaHandle {
                        inner: Arc::new(KnownSchema {
                            facet_tag: tag.clone(),
                            lexicon: registration.lexicon.clone(),
                            sidecar: sidecar.clone(),
                            compiled,
                        }),
                    },
                );
            }
        }
        Ok(Self { known })
    }

    pub fn resolve_for_read(&self, facet_tag: &str) -> Option<SchemaHandle> {
        self.known.get(facet_tag).cloned()
    }

    /// The tags of the known facets whose schema declares at least one `reference` position: the
    /// selector a reference index consumes, derived once from the compiled schemas rather than
    /// re-derived per document.
    pub fn reference_tags(&self) -> Vec<String> {
        self.known
            .values()
            .filter(|handle| handle.compiled().declares_references())
            .map(|handle| handle.facet_tag().to_owned())
            .collect()
    }

    pub fn require_for_write(&self, facet_tag: &str) -> Result<SchemaHandle, SchemaRepoError> {
        self.resolve_for_read(facet_tag)
            .ok_or_else(|| SchemaRepoError::UnknownFacet {
                tag: facet_tag.to_owned(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_cabinet_core::facet::{FacetDecl, Stored, TypeDecl};
    use serde_json::json;

    fn record(tag: &str, value: Value) -> Value {
        json!({"lexicon": 1, "id": tag, "defs": {"main": {"type": "record", "key": "tid", "record": {"type": "object", "required": ["value"], "properties": {"value": value}}}}})
    }

    fn registration(tag: &str, value: Value) -> SchemaRegistration {
        SchemaRegistration::new(tag, record(tag, value))
    }

    fn universe(tag: &str, value: Value) -> Universe {
        Universe {
            facets: vec![FacetDecl {
                id: tag.to_owned(),
                name: "Main".to_owned(),
                schema: schemars::Schema::try_from(
                    json!({"type": "object", "properties": {"value": value}}),
                )
                .unwrap(),
                flags: Vec::new(),
            }],
            types: Vec::new(),
        }
    }

    #[test]
    fn independently_authored_records_are_authoritative_and_handles_are_owned() {
        let tag = "org.example.schema.note";
        let repo = SchemaRepo::open([SchemaRegistrationGroup::new(
            None,
            [registration(tag, json!({"type": "string", "maxLength": 2}))],
            Vec::new(),
        )])
        .unwrap();
        let first = repo.resolve_for_read(tag).unwrap();
        let second = repo.require_for_write(tag).unwrap();
        drop(repo);
        assert!(Arc::ptr_eq(&first.inner, &second.inner));
        assert!(std::ptr::eq(first.compiled(), second.compiled()));
        assert!(std::ptr::eq(
            first.compiled().document(),
            first.compiled().clone().document()
        ));
        assert!(first.sidecar().is_none());
        let value = json!({"$type": tag, "value": "é", "future": true});
        assert_eq!(
            Stored::check(first.compiled(), value.clone())
                .unwrap()
                .into_value(),
            value
        );
        for value in [
            json!({"value": "a"}),
            json!({"$type": tag}),
            json!({"$type": "org.example.wrong", "value": "a"}),
            json!({"$type": tag, "value": "abc"}),
        ] {
            assert!(Stored::check(first.compiled(), value).is_err());
        }
    }

    #[test]
    fn dependency_storage_annotations_are_owned_without_becoming_validation_authority() {
        let tag = "org.example.schema.text";
        let repo = {
            let plug = universe(tag, json!({"$ref": "#/$defs/Carried"}));
            let dependency = Universe {
                facets: Vec::new(),
                types: vec![TypeDecl {
                    name: "Carried".to_owned(),
                    schema: schemars::Schema::try_from(
                        json!({"type": "string", "x-automerge": "text"}),
                    )
                    .unwrap(),
                }],
            };
            SchemaRepo::open([SchemaRegistrationGroup::new(
                Some(Bundle::new(&plug, vec![&dependency])),
                [registration(tag, json!({"type": "string", "minLength": 2}))],
                Vec::new(),
            )])
            .unwrap()
        };
        let handle = repo.require_for_write(tag).unwrap();
        assert!(handle.sidecar().unwrap().document("Main").is_some());
        assert!(Stored::check(handle.compiled(), json!({"$type": tag, "value": "ab"})).is_ok());
        assert!(Stored::check(handle.compiled(), json!({"$type": tag, "value": "a"})).is_err());
        assert!(
            handle
                .compiled()
                .document()
                .to_string()
                .contains("x-automerge")
        );
    }

    #[test]
    fn semantic_contradictions_and_unapproved_validation_constraints_fail_open() {
        let tag = "org.example.schema.invalid";
        for supplement in [
            json!({"type": "integer"}),
            json!({"type": "string", "pattern": "a"}),
            json!({"type": "string", "maxLength": 2}),
            json!({"type": "string", "enum": ["a"]}),
            json!({"type": "string", "x-automerge": "counter"}),
            json!({"type": "string", "format": "email"}),
        ] {
            let universe = universe(tag, supplement.clone());
            assert!(matches!(
                SchemaRepo::open([SchemaRegistrationGroup::new(
                    Some(Bundle::new(&universe, Vec::new())),
                    [registration(tag, json!({"type": "string"}))],
                    Vec::new()
                )]),
                Err(SchemaRepoError::Admission { .. })
            ));
        }
    }

    #[test]
    fn external_lexicons_must_be_carried_and_are_owned_in_the_compiled_context() {
        let tag = "org.example.schema.ref";
        let external = json!({"lexicon": 1, "id": "org.example.types", "defs": {"label": {"type": "string", "enum": ["carried"]}}});
        let make = || {
            registration(
                tag,
                json!({"type": "ref", "ref": "org.example.types#label"}),
            )
        };
        assert!(
            SchemaRepo::open([SchemaRegistrationGroup::new(None, [make()], Vec::new())]).is_err()
        );
        let repo = SchemaRepo::open([SchemaRegistrationGroup::new(None, [make()], vec![external])])
            .unwrap();
        let handle = repo.require_for_write(tag).unwrap();
        drop(repo);
        assert!(
            Stored::check(handle.compiled(), json!({"$type": tag, "value": "carried"})).is_ok()
        );
        assert!(Stored::check(handle.compiled(), json!({"$type": tag, "value": "other"})).is_err());
    }

    #[test]
    fn peer_registrations_are_carried_without_cross_group_discovery() {
        let first = "org.example.schema.first";
        let second = "org.example.schema.second";
        let make = || registration(first, json!({"type": "ref", "ref": second}));
        let peer = || registration(second, json!({"type": "string", "maxLength": 2}));
        let repo = SchemaRepo::open([SchemaRegistrationGroup::new(
            None,
            [make(), peer()],
            Vec::new(),
        )])
        .unwrap();
        let handle = repo.require_for_write(first).unwrap();
        assert!(
            Stored::check(
                handle.compiled(),
                json!({"$type": first, "value": {"$type": second, "value": "ok"}})
            )
            .is_ok()
        );
        assert!(
            Stored::check(
                handle.compiled(),
                json!({"$type": first, "value": {"$type": second, "value": "long"}})
            )
            .is_err()
        );
        assert!(
            SchemaRepo::open([
                SchemaRegistrationGroup::new(None, [make()], Vec::new()),
                SchemaRegistrationGroup::new(None, [peer()], Vec::new()),
            ])
            .is_err()
        );
    }

    #[test]
    fn independent_groups_keep_same_named_supplements_isolated() {
        let first_tag = "org.example.schema.first";
        let second_tag = "org.example.schema.second";
        let first = universe(first_tag, json!({"type": "string", "x-automerge": "text"}));
        let second = universe(
            second_tag,
            json!({"type": "integer", "x-automerge": "counter"}),
        );
        let repo = SchemaRepo::open([
            SchemaRegistrationGroup::new(
                Some(Bundle::new(&first, Vec::new())),
                [registration(first_tag, json!({"type": "string"}))],
                Vec::new(),
            ),
            SchemaRegistrationGroup::new(
                Some(Bundle::new(&second, Vec::new())),
                [registration(second_tag, json!({"type": "integer"}))],
                Vec::new(),
            ),
        ])
        .unwrap();
        let first = repo.require_for_write(first_tag).unwrap();
        let second = repo.require_for_write(second_tag).unwrap();
        assert!(!Arc::ptr_eq(
            first.inner.sidecar.as_ref().unwrap(),
            second.inner.sidecar.as_ref().unwrap()
        ));
        assert!(
            Stored::check(
                first.compiled(),
                json!({"$type": first_tag, "value": "text"})
            )
            .is_ok()
        );
        assert!(Stored::check(second.compiled(), json!({"$type": second_tag, "value": 2})).is_ok());
        assert!(
            Stored::check(
                second.compiled(),
                json!({"$type": second_tag, "value": "text"})
            )
            .is_err()
        );
    }

    #[test]
    fn duplicates_identity_and_malformed_documents_fail_atomically() {
        let tag = "org.example.schema.note";
        let make = || registration(tag, json!({"type": "string"}));
        assert!(matches!(
            SchemaRepo::open([SchemaRegistrationGroup::new(
                None,
                [make(), make()],
                Vec::new()
            )]),
            Err(SchemaRepoError::DuplicateFacetTag(_))
        ));
        for document in [
            Value::Null,
            json!({"lexicon": 2, "id": tag}),
            record("org.example.other", json!({"type": "string"})),
            record(tag, json!({"type": "string", "maxLength": "two"})),
            record(tag, json!({"type": "ref", "ref": "#missing"})),
        ] {
            assert!(matches!(
                SchemaRepo::open([SchemaRegistrationGroup::new(
                    None,
                    [SchemaRegistration::new(tag, document)],
                    Vec::new()
                )]),
                Err(SchemaRepoError::Admission { .. })
            ));
        }
    }

    #[test]
    fn core_facets_are_admitted_like_registered_ones_and_keep_their_narrowing() {
        let tag = "org.example.daybook.dmeta";
        let repo = SchemaRepo::open(Vec::new()).unwrap();
        let handle = repo.require_for_write(tag).unwrap();
        assert!(handle.sidecar().is_some());
        let valid = json!({"$type": tag, "createdAt": "2024-01-02T03:04:05Z"});
        assert!(Stored::check(handle.compiled(), valid).is_ok());
        for invalid in [
            json!({"$type": tag, "createdAt": "not an instant"}),
            json!({"$type": tag, "createdAt": "2024-01-02T03:04:05Z", "actors": {"not-a-uuid": {"pubkey": "z", "actorUrl": "https://example.test"}}}),
            json!({"$type": tag, "createdAt": "2024-01-02T03:04:05Z", "facets": {"org.example.note/main": {"deletedAt": [], "signatures": [{"actorUuid": "not-a-uuid", "signature": {"$bytes": "AQ=="}}]}}}),
            json!({"$type": tag, "createdAt": "2024-01-02T03:04:05Z", "actors": {"01a15688-2519-7ea0-a1ea-421237f5c0cc": {"pubkey": "z", "actorUrl": "https://example.test", "delegation": "not bytes"}}}),
        ] {
            assert!(
                Stored::check(handle.compiled(), invalid.clone()).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn reference_tags_select_the_facets_that_declare_references() {
        let referencing = "org.example.schema.referencing";
        let plain = "org.example.schema.plain";
        let repo = SchemaRepo::open([
            SchemaRegistrationGroup::new(
                Some(Bundle::new(
                    &universe(
                        referencing,
                        json!({"type": "string", "x-automerge": "reference"}),
                    ),
                    Vec::new(),
                )),
                [registration(referencing, json!({"type": "string"}))],
                Vec::new(),
            ),
            SchemaRegistrationGroup::new(
                None,
                [registration(plain, json!({"type": "string"}))],
                Vec::new(),
            ),
        ])
        .unwrap();
        let tags = repo.reference_tags();
        // The core branch facet carries a `FacetReference`, so it is selected alongside the
        // registered referencing facet; the plain one and dmeta are not.
        assert_eq!(
            tags,
            vec![
                "org.example.daybook.branch".to_owned(),
                referencing.to_owned()
            ]
        );
    }

    #[test]
    fn unknown_reads_are_best_effort_but_unknown_writes_are_refused() {
        let repo = SchemaRepo::open(Vec::new()).unwrap();
        assert!(repo.resolve_for_read("org.example.unknown").is_none());
        assert!(matches!(
            repo.require_for_write("org.example.unknown"),
            Err(SchemaRepoError::UnknownFacet { .. })
        ));
    }
}
