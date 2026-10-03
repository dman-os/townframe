//! The first Daybook producer: one whole-document dpath whose Body selects a text Note.
//! Selection is explicit; unsupported compound or cross-document interpretations fail.

mod interlude {
    pub use daybook_types::doc::{
        BranchPath, ChangeHashSet, DocId, FacetKey, WellKnownFacet, WellKnownFacetTag,
    };
    pub use pauperfuse::backends::{
        BackendId, BackendTree, ByteReader, Description, OutputVersion, Producer, RelPath, Source,
        TreeEntry,
    };
    pub use utils_rs::prelude::*;
}
use crate::interlude::*;
use daybook_core::drawer::DrawerRepo;
use daybook_types::dpath::{Dpath, DpathFacet};
use std::collections::VecDeque;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported text projection: {0}")]
    Unsupported(String),
    #[error("exact Daybook source unavailable: {0}")]
    Unavailable(String),
    #[error(transparent)]
    Repository(#[from] eyre::Report),
}

/// Durable recipe for one raw-text output, with no rendered bytes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    pub document: DocId,
    pub facet: FacetKey,
    pub path: String,
}

impl Projection {
    /// Validates the current main-branch interpretation and returns its exact basis.
    pub async fn select(
        drawer: &DrawerRepo,
        document: DocId,
    ) -> Result<(Self, ChangeHashSet), Error> {
        let (doc, heads) = drawer
            .get_with_heads(&document, BranchPath::new("main"), None)
            .await?
            .ok_or_else(|| Error::Unavailable(format!("document {document} on main")))?;
        let mut dpaths = Vec::new();
        for (key, value) in &doc.facets {
            if let Some(path) = Dpath::parse_facet_key(key) {
                let path = path.map_err(|error| Error::Unsupported(error.to_string()))?;
                let scope = DpathFacet::from_json_value(value).map_err(Error::Unsupported)?;
                if !scope.is_whole_document() {
                    return Err(Error::Unsupported("selective dpath".into()));
                }
                dpaths.push(path);
            }
        }
        if dpaths.len() != 1 {
            return Err(Error::Unsupported(
                "exactly one whole-document dpath is required".into(),
            ));
        }
        let body_key = FacetKey::from(WellKnownFacetTag::Body);
        let body = doc
            .facets
            .get(&body_key)
            .ok_or_else(|| Error::Unsupported("Body is missing".into()))?;
        let WellKnownFacet::Body(body) = serde_json::from_value(body.clone())? else {
            return Err(Error::Unsupported("Body facet has the wrong shape".into()));
        };
        if body.order.len() != 1 {
            return Err(Error::Unsupported(
                "Body must select exactly one Note".into(),
            ));
        }
        let url = &body.order[0];
        // Existing Daybook URL parsing does not percent-decode. Never silently misresolve it.
        if !url.path().is_ascii() || url.path().contains('%') || url.path().contains('\\') {
            return Err(Error::Unsupported(
                "encoded/non-ASCII Body references await the facet-URL correction".into(),
            ));
        }
        let reference = daybook_types::url::parse_facet_ref(url)?;
        if reference.doc_id != "self" && reference.doc_id != document {
            return Err(Error::Unsupported("cross-document Body reference".into()));
        }
        if reference.branch.is_some() || reference.at.is_some() {
            return Err(Error::Unsupported("pinned Body references".into()));
        }
        let value = doc
            .facets
            .get(&reference.facet_key)
            .ok_or_else(|| Error::Unsupported("Body's Note is missing".into()))?;
        validate_note(value)?;
        let path = dpaths
            .pop()
            .unwrap()
            .segments()
            .collect::<Vec<_>>()
            .join("/");
        RelPath::parse(&path).map_err(|error| Error::Unsupported(error.to_string()))?;
        Ok((
            Self {
                document,
                facet: reference.facet_key,
                path,
            },
            heads,
        ))
    }
}

fn validate_note(value: &serde_json::Value) -> Result<daybook_types::doc::Note, Error> {
    let WellKnownFacet::Note(note) = serde_json::from_value(value.clone())? else {
        return Err(Error::Unsupported("Body must select a Note facet".into()));
    };
    if note.mime != "text/plain" {
        return Err(Error::Unsupported(
            "only text/plain Notes are supported".into(),
        ));
    }
    Ok(note)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Output {
    document: DocId,
    facet: FacetKey,
    branch: String,
}

/// A statically routed producer over an independently owned checkout branch.
pub struct Daybook {
    drawer: Arc<DrawerRepo>,
    id: BackendId,
    projection: Projection,
    branch: String,
    heads: ChangeHashSet,
}

impl Daybook {
    pub fn new(
        drawer: Arc<DrawerRepo>,
        id: BackendId,
        projection: Projection,
        branch: String,
        heads: ChangeHashSet,
    ) -> Self {
        Self {
            drawer,
            id,
            projection,
            branch,
            heads,
        }
    }
    fn output(&self) -> Output {
        Output {
            document: self.projection.document.clone(),
            facet: self.projection.facet.clone(),
            branch: self.branch.clone(),
        }
    }
    pub fn source(&self) -> Source {
        Source {
            backend: self.id.clone(),
            output: OutputVersion {
                output: serde_json::to_vec(&self.output()).unwrap(),
                version: serde_json::to_vec(&am_utils_rs::serialize_commit_heads(&self.heads))
                    .unwrap(),
            },
        }
    }
}

pub struct Observation(VecDeque<TreeEntry>);
impl BackendTree for Observation {
    type Error = Error;
    async fn next_entry(&mut self) -> Result<Option<TreeEntry>, Self::Error> {
        Ok(self.0.pop_front())
    }
}

pub struct Reader(Vec<u8>);
impl ByteReader for Reader {
    type Error = Error;
    async fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        let Ok(offset) = usize::try_from(offset) else {
            return Ok(0);
        };
        let bytes = self.0.get(offset..).unwrap_or_default();
        let length = bytes.len().min(buffer.len());
        buffer[..length].copy_from_slice(&bytes[..length]);
        Ok(length)
    }
}

impl Producer for Daybook {
    type Error = Error;
    type Tree = Observation;
    type Reader = Reader;
    fn id(&self) -> &BackendId {
        &self.id
    }
    async fn observe(&self) -> Result<Self::Tree, Self::Error> {
        let path = RelPath::parse(&self.projection.path)
            .map_err(|error| Error::Unsupported(error.to_string()))?;
        let mut entries = path
            .ancestors_inclusive()
            .take(path.len())
            .map(|path| TreeEntry {
                path,
                description: Description::Directory,
            })
            .collect::<Vec<_>>();
        entries.push(TreeEntry {
            path,
            description: Description::File {
                source: self.source(),
                size: None,
            },
        });
        Ok(Observation(entries.into()))
    }
    async fn open(&self, selector: &OutputVersion) -> Result<Self::Reader, Self::Error> {
        let output: Output = serde_json::from_slice(&selector.output)?;
        if output != self.output() {
            return Err(Error::Unavailable(
                "output does not belong to this projection".into(),
            ));
        }
        let heads: Vec<String> = serde_json::from_slice(&selector.version)?;
        let heads = ChangeHashSet(am_utils_rs::parse_commit_heads(&heads)?);
        let doc = self
            .drawer
            .get_doc_with_facets_at_branch_heads(
                &output.document,
                BranchPath::new(&output.branch),
                &heads,
                Some(vec![output.facet.clone()]),
            )
            .await?
            .ok_or_else(|| Error::Unavailable(format!("{} at selected heads", output.document)))?;
        let value = doc
            .facets
            .get(&output.facet)
            .ok_or_else(|| Error::Unavailable("selected Note is absent".into()))?;
        Ok(Reader(validate_note(value)?.content.into_bytes()))
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Repository(error.into())
    }
}
