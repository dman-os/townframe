//! Known datum-schema configuration for one Cabinet embedding.
//!
//! A schema repository owns the embedder-provided Lexicon document and the
//! validated carried sidecar bundle together. It does not discover, publish,
//! fetch, or compare schemas. Unknown datums remain readable as best-effort
//! raw data, but writes require an explicitly configured known datum.
//!
//! The datums Cabinet itself maintains — dmeta, branch, drawer — are always registered, before any embedder group,
//! so an embedder cannot shadow them with its own definition. They are declared in Rust, so their
//! sidecar is the codec's authority by construction, and the Lexicon published for a core datum is
//! the projection of that same declaration. Core datums go through the same Lexicon admission path
//! as an embedder-registered pair — the projected Lexicon is checked against the sidecar and the
//! compiled schema is the overlay admission produces — so there is exactly one compilation path a
//! stored value is ever read against.

use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::CompiledSchema;
use big_cabinet_core::datum::Declaration;
use big_cabinet_core::datum::LexiconAdmissionError;
use big_cabinet_core::datum::Universe;
use big_cabinet_core::datum::admit_lexicon;
use big_cabinet_core::datum::UniverseError;
use big_cabinet_core::dmeta::BranchPath;
use big_cabinet_core::dmeta::Dmeta;
use big_cabinet_core::drawer::Drawer;
use big_cabinet_core::drawer::DrawerChildEntry;
use big_cabinet_core::drawer::DrawerDocEntry;
use big_cabinet_core::drawer::DrawerMetaDoc;
use big_cabinet_core::drawer::DrawerRoster;
use big_cabinet_core::drawer::DrawerSuccessor;
use big_cabinet_core::drawer::KnownDrawerEntry;
use big_cabinet_core::drawer::SelfDocLayout;
use big_cabinet_core::drawer::SingleGroupLayout;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::RwLock;
use thiserror::Error;

static CORE: LazyLock<Universe> = LazyLock::new(|| {
    Universe::collect([
        Declaration::of::<Dmeta>().expect("dmeta is a well-formed datum declaration"),
        Declaration::of::<BranchPath>().expect("branch is a well-formed datum declaration"),
        Declaration::of::<Drawer>().expect("drawer is a well-formed datum declaration"),
        // A published union's members are carried as their own lexicons at admission, whether or
        // not the union is open.
        Declaration::of::<SelfDocLayout>()
            .expect("self-doc layout is a well-formed member declaration"),
        Declaration::of::<SingleGroupLayout>()
            .expect("single-group layout is a well-formed member declaration"),
        Declaration::of::<DrawerRoster>()
            .expect("drawer roster is a well-formed datum declaration"),
        Declaration::of::<DrawerDocEntry>()
            .expect("drawer doc entry is a well-formed datum declaration"),
        Declaration::of::<DrawerChildEntry>()
            .expect("drawer child entry is a well-formed datum declaration"),
        Declaration::of::<DrawerMetaDoc>()
            .expect("drawer meta doc is a well-formed datum declaration"),
        Declaration::of::<KnownDrawerEntry>()
            .expect("known drawer entry is a well-formed datum declaration"),
        Declaration::of::<DrawerSuccessor>()
            .expect("drawer successor is a well-formed datum declaration"),
    ])
});

fn core_schemas() -> impl Iterator<Item = SchemaHandle> {
    let bundle = Bundle::new(&CORE, Vec::new());
    let sidecar = Arc::new(
        OwnedSidecar::from_bundle(&bundle).expect("core datum declarations form a valid bundle"),
    );
    let lexicons = bundle.lexicons().expect("core datums publish lexicons");
    (0..lexicons.len()).map(move |index| {
        let datum = &CORE.datums[index];
        let lexicon = &lexicons[index];
        assert_eq!(datum.id, lexicon.id, "a bundle projects datums in order");
        let carried: Vec<&Value> = lexicons
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(_, other)| &other.file)
            .collect();
        let compiled = bundle
            .validate_lexicon(&datum.id, &lexicon.file, &carried)
            .expect("a core datum's declaration admits its own published lexicon");
        SchemaHandle {
            inner: Arc::new(KnownSchema {
                datum_tag: datum.id.clone(),
                lexicon: lexicon.file.clone(),
                sidecar: Some(Arc::clone(&sidecar)),
                compiled,
            }),
        }
    })
}

pub struct SchemaRegistration {
    pub datum_tag: String,
    pub lexicon: Value,
    /// The datum's sidecar JSON Schema document, when it arrives as data rather than as a Rust
    /// declaration: a cartridge's nodule ships one per record it holds. A group with a
    /// [`SchemaRegistrationGroup::sidecar`] renders its own, so it states none here.
    pub supplement: Option<Value>,
}

impl SchemaRegistration {
    pub fn new(datum_tag: impl Into<String>, lexicon: Value) -> Self {
        Self {
            datum_tag: datum_tag.into(),
            lexicon,
            supplement: None,
        }
    }

    /// The same registration with `supplement` as its sidecar document.
    #[must_use]
    pub fn with_supplement(mut self, supplement: Value) -> Self {
        self.supplement = Some(supplement);
        self
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
    #[error("known datum schema tag is empty")]
    EmptyDatumTag,
    #[error("known datum schema tag `{0}` is configured more than once")]
    DuplicateDatumTag(String),
    #[error("sidecar bundle is invalid: {source}")]
    SidecarInvalid {
        #[source]
        source: UniverseError,
    },
    #[error("schema pair for datum `{tag}` is invalid: {source}")]
    Admission {
        tag: String,
        #[source]
        source: Box<LexiconAdmissionError>,
    },
    #[error("datum `{tag}` is not known to this SchemaRepo")]
    UnknownDatum { tag: String },
}

#[derive(Debug, Clone)]
struct OwnedSidecar {
    cartridge: Universe,
    dependencies: Vec<Universe>,
}

impl OwnedSidecar {
    fn from_bundle(bundle: &Bundle<'_>) -> Result<Self, SchemaRepoError> {
        bundle
            .validate()
            .map_err(|source| SchemaRepoError::SidecarInvalid { source })?;
        Ok(Self {
            cartridge: bundle.cartridge.clone(),
            dependencies: bundle
                .dependencies
                .iter()
                .map(|dependency| (*dependency).clone())
                .collect(),
        })
    }

    fn borrowed(&self) -> Bundle<'_> {
        Bundle::new(&self.cartridge, self.dependencies.iter().collect())
    }
}

#[derive(Debug)]
struct KnownSchema {
    datum_tag: String,
    lexicon: Value,
    sidecar: Option<Arc<OwnedSidecar>>,
    compiled: CompiledSchema,
}

#[derive(Debug, Clone)]
pub struct SchemaHandle {
    inner: Arc<KnownSchema>,
}

impl SchemaHandle {
    pub fn datum_tag(&self) -> &str {
        &self.inner.datum_tag
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

/// The datum schemas this node knows. The boot set (the core datums and the groups the embedder
/// registers) is fixed for the life of the repo; beyond it, a cartridge's Lexicons are registered
/// under an owner and retired with it, live, as the cartridge is enabled and disabled. Readers hold a
/// [`SchemaHandle`], so a retired schema stays usable to whoever already resolved it and only
/// stops resolving for new readers.
#[derive(Debug)]
pub struct SchemaRepo {
    state: RwLock<KnownSchemas>,
}

#[derive(Debug, Clone, Default)]
struct KnownSchemas {
    known: BTreeMap<String, SchemaHandle>,
    /// The owner of each tag registered after boot; the boot set has none.
    owners: BTreeMap<String, String>,
}

impl KnownSchemas {
    fn admit_groups<'a>(
        &mut self,
        groups: impl IntoIterator<Item = SchemaRegistrationGroup<'a>>,
    ) -> Result<(), SchemaRepoError> {
        let known = &mut self.known;
        for group in groups {
            let mut tags = std::collections::BTreeSet::new();
            for registration in &group.registrations {
                if !tags.insert(&registration.datum_tag)
                    || known.contains_key(&registration.datum_tag)
                {
                    return Err(SchemaRepoError::DuplicateDatumTag(
                        registration.datum_tag.clone(),
                    ));
                }
            }
            let sidecar = group
                .sidecar
                .as_ref()
                .map(OwnedSidecar::from_bundle)
                .transpose()?
                .map(Arc::new);
            for registration in &group.registrations {
                let tag = &registration.datum_tag;
                if tag.is_empty() {
                    return Err(SchemaRepoError::EmptyDatumTag);
                }
                if known.contains_key(tag) {
                    return Err(SchemaRepoError::DuplicateDatumTag(tag.clone()));
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
                let compiled = match (&sidecar, &registration.supplement) {
                    (Some(_), Some(_)) => {
                        panic!("`{tag}` states a supplement in a group whose sidecar renders one")
                    }
                    (Some(sidecar), None) => {
                        sidecar
                            .borrowed()
                            .validate_lexicon(tag, &registration.lexicon, &carried)
                    }
                    (None, supplement) => admit_lexicon(
                        tag,
                        &registration.lexicon,
                        &carried,
                        supplement.as_ref(),
                    ),
                }
                .map_err(|source| SchemaRepoError::Admission {
                        tag: tag.clone(),
                        source: Box::new(source),
                    })?;
                known.insert(
                    tag.clone(),
                    SchemaHandle {
                        inner: Arc::new(KnownSchema {
                            datum_tag: tag.clone(),
                            lexicon: registration.lexicon.clone(),
                            sidecar: sidecar.clone(),
                            compiled,
                        }),
                    },
                );
            }
        }
        Ok(())
    }
}

impl SchemaRepo {
    pub fn open<'a>(
        groups: impl IntoIterator<Item = SchemaRegistrationGroup<'a>>,
    ) -> Result<Self, SchemaRepoError> {
        let mut state = KnownSchemas {
            known: core_schemas()
                .map(|handle| (handle.datum_tag().to_owned(), handle))
                .collect(),
            owners: BTreeMap::new(),
        };
        state.admit_groups(groups)?;
        Ok(Self {
            state: RwLock::new(state),
        })
    }

    /// A new repo holding this one's schemas plus `groups`, which become boot-set members of the
    /// new repo. The two repos are independent afterwards.
    pub fn with_groups<'a>(
        &self,
        groups: impl IntoIterator<Item = SchemaRegistrationGroup<'a>>,
    ) -> Result<Self, SchemaRepoError> {
        let mut state = self.read().clone();
        state.owners.clear();
        state.admit_groups(groups)?;
        Ok(Self {
            state: RwLock::new(state),
        })
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, KnownSchemas> {
        self.state.read().expect("schema repo lock is not poisoned")
    }

    /// Registers `group` as `owner`'s schemas, replacing whatever `owner` registered before. It
    /// is all or nothing: on error the repo, including `owner`'s earlier schemas, is untouched.
    /// A tag already known (a boot schema or another owner's) is refused, never shadowed. The
    /// write lock is held while the group compiles, so callers register only when the owner's
    /// schemas actually changed.
    pub fn register(
        &self,
        owner: &str,
        group: SchemaRegistrationGroup<'_>,
    ) -> Result<(), SchemaRepoError> {
        let mut state = self
            .state
            .write()
            .expect("schema repo lock is not poisoned");
        let mut next = state.clone();
        next.retire(owner);
        let tags: Vec<String> = group
            .registrations
            .iter()
            .map(|registration| registration.datum_tag.clone())
            .collect();
        next.admit_groups([group])?;
        next.owners
            .extend(tags.into_iter().map(|tag| (tag, owner.to_owned())));
        *state = next;
        Ok(())
    }

    /// Removes every schema `owner` registered. Retiring an owner with none is a no-op.
    pub fn retire(&self, owner: &str) {
        self.state
            .write()
            .expect("schema repo lock is not poisoned")
            .retire(owner);
    }

    /// Whether `datum_tag` belongs to the boot set: known to the node itself rather than
    /// registered by a cartridge.
    pub fn is_boot_schema(&self, datum_tag: &str) -> bool {
        let state = self.read();
        state.known.contains_key(datum_tag) && !state.owners.contains_key(datum_tag)
    }

    pub fn resolve_for_read(&self, datum_tag: &str) -> Option<SchemaHandle> {
        self.read().known.get(datum_tag).cloned()
    }

    /// The tags of the known datums whose schema declares at least one `reference` position: the
    /// selector a reference index consumes, derived once from the compiled schemas rather than
    /// re-derived per document.
    pub fn reference_tags(&self) -> Vec<String> {
        self.read()
            .known
            .values()
            .filter(|handle| handle.compiled().declares_references())
            .map(|handle| handle.datum_tag().to_owned())
            .collect()
    }

    pub fn require_for_write(&self, datum_tag: &str) -> Result<SchemaHandle, SchemaRepoError> {
        self.resolve_for_read(datum_tag)
            .ok_or_else(|| SchemaRepoError::UnknownDatum {
                tag: datum_tag.to_owned(),
            })
    }
}

impl KnownSchemas {
    fn retire(&mut self, owner: &str) {
        let tags: Vec<String> = self
            .owners
            .iter()
            .filter(|(_, tag_owner)| *tag_owner == owner)
            .map(|(tag, _)| tag.clone())
            .collect();
        for tag in tags {
            self.owners.remove(&tag);
            self.known.remove(&tag);
        }
    }
}

#[cfg(test)]
mod tests;
