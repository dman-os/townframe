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
use big_cabinet_core::drawer::Drawer;
use big_cabinet_core::drawer::DrawerChildEntry;
use big_cabinet_core::drawer::DrawerDocEntry;
use big_cabinet_core::drawer::DrawerRoster;
use big_cabinet_core::drawer::KnownDrawerEntry;
use big_cabinet_core::drawer::SelfDocLayout;
use big_cabinet_core::drawer::SingleGroupLayout;
use big_cabinet_core::facet::Bundle;
use big_cabinet_core::facet::CompiledSchema;
use big_cabinet_core::facet::Declaration;
use big_cabinet_core::facet::LexiconAdmissionError;
use big_cabinet_core::facet::Universe;
use big_cabinet_core::facet::UniverseError;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::LazyLock;
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
mod tests;
