//! The fixture node: [`NodeCalls`] over documents held in memory, answering `readDatums` as the
//! node does for the same data. It stands in until claytxt's micro -> node
//! transport lands (see [`crate::node`]), and is what the view-model tests read.
//!
//! Methods are answered through their JSON wire shape: the input is serialized, dispatched by the
//! method's NSID and the typed answer is deserialized as the method's own output, so the micro
//! exercises the same types it will exchange with the real node.
use crate::model::key;
use crate::node::CallError;
use crate::node::Configuration;
use crate::node::NodeCalls;
use big_blobs_core::Blob;
use big_blobs_core::BlobId;
use big_blobs_core::ReadBlob;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::MultibaseKey;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumPage;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::reference::DatumReference;
use big_cabinet_core::reference::ReferenceDocId;
use clay_mdk::ClientError;
use intxt_core::generic;
use intxt_core::work;
use method_core::ContextError;
use method_core::Unary;
use serde_json::Value;
use serde_json::json;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::future::ready;
use std::rc::Rc;
use std::time::Duration;

type Datums = BTreeMap<DatumKey, Value>;

/// The unified patch the sample's first commit entry carries.
pub(crate) const SAMPLE_PATCH: &str = "diff --git a/src/review.rs b/src/review.rs
--- a/src/review.rs
+++ b/src/review.rs
@@ -1,3 +1,4 @@
 fn review() {
-    println!(\"todo\");
+    println!(\"read the PR\");
+    println!(\"find its intxts\");
 }
";

#[derive(Clone, Default)]
struct Held {
    docs: BTreeMap<DocId, Datums>,
    /// The blob store: what `blobs.read` serves, by id.
    blobs: BTreeMap<BlobId, Vec<u8>>,
    /// The `githubPull` datums of the GitHub drawer, which a poll reports.
    pulls: Vec<(DocId, DatumKey)>,
}

/// A node held in memory. Clones share what they are asked, so a test sees the calls a view made.
#[derive(Clone, Default)]
pub struct Fixture {
    held: Rc<Held>,
    calls: Rc<RefCell<Vec<&'static str>>>,
}

impl Fixture {
    /// A node holding nothing.
    pub fn empty() -> Self {
        Self::default()
    }

    /// A document id from `tail`, left-padded: ids are 32 base58 characters after the `z`.
    pub fn doc_id(tail: &str) -> DocId {
        format!("z{tail:1>32}")
            .parse()
            .expect("a fixture id is well formed")
    }

    /// Hold `doc` with these datums.
    #[must_use]
    pub fn doc(mut self, doc: DocId, datums: impl IntoIterator<Item = (DatumKey, Value)>) -> Self {
        Rc::make_mut(&mut self.held)
            .docs
            .insert(doc, datums.into_iter().collect());
        self
    }

    /// Hold `bytes` as a patch blob: the `org.example.daybook.blob` datum at `slot` in `doc`
    /// (`text/x-diff`, as the node writes it) and the bytes in the blob store. The fixture does
    /// not hash: a blob's id is its place in the store, repeated, which only has to be stable.
    #[must_use]
    pub fn patch_blob(mut self, doc: &DocId, slot: &str, bytes: &str) -> Self {
        let held = Rc::make_mut(&mut self.held);
        let id = BlobId::new([u8::try_from(held.blobs.len()).expect("few blobs"); 32]);
        held.blobs.insert(id, bytes.as_bytes().to_vec());
        let blob = Blob {
            mime: "text/x-diff".to_owned(),
            length_octets: bytes.len() as u64,
            digest: id.digest_str(),
            inline: None,
            urls: None,
            extra: Default::default(),
        };
        held.docs
            .get_mut(doc)
            .expect("the document is held")
            .insert(
                key(Blob::ID, slot),
                serde_json::to_value(blob).expect("a blob datum is plain JSON"),
            );
        self
    }

    /// Have the poll report `key` of `doc` as a held pull request, whatever the document holds.
    #[must_use]
    pub fn held_pull(mut self, doc: DocId, key: DatumKey) -> Self {
        Rc::make_mut(&mut self.held).pulls.push((doc, key));
        self
    }

    /// Hold a pull request: its own document with the `githubPull` datum at
    /// `githubPull/<slot>` (`<owner>/<repo>/<number>`).
    #[must_use]
    pub fn pull(self, doc: DocId, slot: &str, title: &str, state: &str, body: &str) -> Self {
        let (repo, number) = slot
            .rsplit_once('/')
            .expect("a pull request's slot is owner/repo/number");
        let datum = json!({
            "title": title, "body": body, "state": state,
            "head": "feat/intxt-review-micro", "base": "main",
            "headSha": "4c1f0a7e", "baseSha": "9a0e11c0",
            "updatedAt": "2026-11-17T10:00:00Z",
            "url": format!("https://github.com/{repo}/pull/{number}"),
        });
        let pull = key(work::GithubPull::ID, slot);
        self.doc(doc.clone(), [(pull.clone(), datum)])
            .held_pull(doc, pull)
    }

    /// Hold an intxt as D70 lays it out: the drawer's own document with the intxt marker and a
    /// pointer to `index`, and the `index` content document with the title, a body listing
    /// `entries` in order, and the entries.
    #[must_use]
    pub fn intxt(
        self,
        drawer: DocId,
        index: DocId,
        title: &str,
        description: Option<&str>,
        entries: Vec<(DatumKey, Value)>,
    ) -> Self {
        let body = generic::Body {
            order: entries
                .iter()
                .map(|(entry, _)| DatumReference::new(ReferenceDocId::SelfDoc, entry.clone()))
                .collect(),
            extra: Default::default(),
        };
        let title = generic::Title {
            title: title.to_owned(),
            description: description.map(str::to_owned),
            extra: Default::default(),
        };
        let pointer = generic::Index {
            doc: index.to_string(),
            extra: Default::default(),
        };
        let descriptor = [
            (
                key(generic::Intxt::ID, "main"),
                serde_json::to_value(generic::Intxt::default()).expect("a marker is plain JSON"),
            ),
            (
                key(generic::Index::ID, "main"),
                serde_json::to_value(pointer).expect("an index pointer is plain JSON"),
            ),
        ];
        let listing = [
            (
                key(generic::Title::ID, "main"),
                serde_json::to_value(title).expect("a title is plain JSON"),
            ),
            (
                key(generic::Body::ID, "main"),
                serde_json::to_value(body).expect("a body is plain JSON"),
            ),
        ];
        self.doc(drawer, descriptor)
            .doc(index, listing.into_iter().chain(entries))
    }

    /// A body that lists an entry its index document does not hold.
    #[cfg(test)]
    #[must_use]
    pub fn listing_a_missing_entry(mut self, index: &DocId) -> Self {
        let body_key = key(generic::Body::ID, "main");
        let datums = Rc::make_mut(&mut self.held)
            .docs
            .get_mut(index)
            .expect("the index is held");
        let mut body: generic::Body =
            serde_json::from_value(datums[&body_key].clone()).expect("a body");
        body.order.push(DatumReference::new(
            ReferenceDocId::SelfDoc,
            key(work::SessionEntry::ID, "gone"),
        ));
        datums.insert(
            body_key,
            serde_json::to_value(body).expect("a body is plain JSON"),
        );
        self
    }

    /// The NSIDs of the calls made so far, in order, by this node and every clone of it.
    pub fn calls(&self) -> Vec<&'static str> {
        self.calls.borrow().clone()
    }

    #[cfg(test)]
    pub fn forget_calls(&self) {
        self.calls.borrow_mut().clear();
    }

    /// What the micro opens on: two pull requests, the first naming two intxts and a document that
    /// is no intxt in its description block (`PrLinks::block`), the second naming none.
    pub fn sample() -> Self {
        let first = Self::doc_id("31");
        let second = Self::doc_id("32");
        let first_index = Self::doc_id("33");
        let second_index = Self::doc_id("34");
        let not_an_intxt = Self::doc_id("41");

        let commit = |tip: &str, base: Option<&str>, patch: Option<&str>| {
            let mut entry = json!({
                "tip": format!("{}/{tip}", work::GitCommit::ID),
                "pack": "dtxt:datum:self/org.example.daybook.blob/af1349b9f5f9a1a6a0404dea36dcc949",
            });
            if let Some(base) = base {
                entry["base"] = json!(format!("{}/{base}", work::GitCommit::ID));
            }
            // A patch is a reference to a blob datum in the entry's own document (`self`); an
            // entry without one is a diff too large to carry. `patch_blob` holds the datum.
            if patch.is_some() {
                entry["patch"] = json!(format!("dtxt:datum:self/{}/patch-{}", Blob::ID, &tip[..7]));
            }
            (key(work::GitCommitEntry::ID, &tip[..7]), entry)
        };
        let pull_entry = (
            key(work::GithubPullEntry::ID, "dman-os/townframe/150"),
            json!({ "pull": format!("dtxt:datum:{}/{}/dman-os/townframe/150", Self::doc_id("21"), work::GithubPull::ID) }),
        );

        let mut named = generic::PrLink::new(generic::Intxt::reference(
            &first,
            [MultibaseKey::try_from([7_u8; 32].as_slice()).expect("32 bytes are a node key")],
        ));
        named.title = Some("intxt: commits live with the intxt".to_owned());
        named.refs = vec!["refs/heads/stack/intxt-16-capture-records-commits".to_owned()];
        let description = format!(
            "Reads a pull request and the intxts it names on the phone.\n\n{}",
            generic::PrLinks {
                intxts: vec![
                    named,
                    generic::PrLink::new(generic::Intxt::reference(&second, [])),
                    generic::PrLink::new(generic::Intxt::reference(&not_an_intxt, [])),
                ],
                ..Default::default()
            }
            .block()
        );

        Self::empty()
            .pull(
                Self::doc_id("21"),
                "dman-os/townframe/150",
                "intxt: read a PR and its intxts on the phone",
                "open",
                &description,
            )
            .pull(
                Self::doc_id("22"),
                "dman-os/townframe/151",
                "chore: pin the review micro's lockfile",
                "merged",
                "",
            )
            .intxt(
                first.clone(),
                first_index.clone(),
                "intxt: commits live with the intxt",
                Some("Commits travel with the intxt that names them."),
                vec![
                    commit("4c1f0a7e22", Some("9a0e11c0d4"), Some(SAMPLE_PATCH)),
                    commit("77b2d03e91", None, None),
                    pull_entry.clone(),
                ],
            )
            .patch_blob(&first_index, "patch-4c1f0a7", SAMPLE_PATCH)
            .intxt(
                second,
                second_index,
                "review micro: the PR on the phone",
                Some("Reads a PR on the phone and finds the intxts it names."),
                vec![pull_entry],
            )
            .doc(not_an_intxt, [])
    }

    /// `None` is the node refusing the call.
    fn answer(&self, nsid: &'static str, input: Value) -> Option<Value> {
        self.calls.borrow_mut().push(nsid);
        let answer = match nsid {
            micro_review_manifest::READ_DATUMS => serde_json::to_value(
                self.read(serde_json::from_value(input).expect("a ReadDatums"))?,
            ),
            micro_review_manifest::POLL_DATUMS => serde_json::to_value(
                self.poll(serde_json::from_value(input).expect("a PollDatums")),
            ),
            other => unimplemented!("the fixture node does not serve {other}"),
        };
        Some(answer.expect("a method output is plain JSON"))
    }

    /// Every held pull request in one caught-up page, whatever the predicate says: the fixture
    /// holds one GitHub drawer.
    fn poll(&self, _input: PollDatums) -> DatumPage {
        DatumPage {
            events: self
                .held
                .pulls
                .iter()
                .map(|(doc_id, datum_key)| DatumEvent::Datum {
                    revision: 1,
                    doc_id: doc_id.clone(),
                    datum_key: datum_key.clone(),
                    datum: ObservedDatum::Present {
                        doc_heads: vec![],
                        version: DatumVersion {
                            heads: vec![],
                            clock: 1,
                        },
                    },
                })
                .collect(),
            through: 1,
            caught_up: true,
        }
    }

    fn read(&self, input: ReadDatums) -> Option<DatumsRead> {
        let datums = self.held.docs.get(&input.doc_id)?;
        let version = DatumVersion {
            heads: vec![],
            clock: 1,
        };
        let accepted = |value: &Value| DatumRead::Accepted {
            value: value.clone(),
            version: version.clone(),
            authors: vec![],
        };
        let read = match &input.datums {
            None => datums
                .iter()
                .map(|(datum, value)| (datum.clone(), accepted(value)))
                .collect(),
            // A key the document does not hold reads as absent, as at the node.
            Some(only) => only
                .iter()
                .map(|datum| {
                    let read = datums.get(datum).map_or_else(
                        || DatumRead::Absent {
                            version: version.clone(),
                        },
                        accepted,
                    );
                    (datum.clone(), read)
                })
                .collect(),
        };
        Some(DatumsRead {
            heads: vec![],
            datums: read,
        })
    }
}

impl NodeCalls for Fixture {
    fn call<M: Unary>(
        &self,
        input: M,
    ) -> impl Future<Output = Result<M::Output, ClientError<M::Error>>> {
        let input = serde_json::to_value(&input).expect("a method input is plain JSON");
        // The node refuses a document it does not grant as it refuses any call it will not
        // serve: a method error. The fixture cannot name a method's own refusals generically,
        // so it refuses with the context error every method error can carry.
        ready(
            self.answer(M::XRPC.nsid, input)
                .map(|answer| {
                    serde_json::from_value(answer)
                        .expect("the fixture answers in the method's output shape")
                })
                .ok_or_else(|| ClientError::Method(ContextError::Unauthenticated.into())),
        )
    }

    fn read_blob(&self, input: ReadBlob) -> impl Future<Output = Result<Vec<u8>, CallError>> {
        self.calls
            .borrow_mut()
            .push(micro_review_manifest::READ_BLOB);
        ready(
            self.held
                .blobs
                .get(&input.blob_id)
                .cloned()
                .ok_or_else(|| CallError::Refused("blob is not locally available".to_owned())),
        )
    }

    /// Time does not pass at a fixture node: its clock stays at zero and a sleep never ends, so
    /// a view that paces itself on them is laid out once and never advances a frame.
    fn sleep(&self, _duration: Duration) -> impl Future<Output = ()> {
        std::future::pending()
    }

    fn now(&self) -> Duration {
        Duration::ZERO
    }

    fn configuration(&self) -> Result<Configuration, CallError> {
        Ok(Configuration {
            github_drawer: Fixture::doc_id("91"),
            intxt_drawers: vec![Fixture::doc_id("92")],
            doc_cap: CapId::from_bytes([0; 16]),
            blob_cap: CapId::from_bytes([1; 16]),
        })
    }
}

/// Two fixtures are the same node when they are clones of one.
impl PartialEq for Fixture {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.held, &other.held) && Rc::ptr_eq(&self.calls, &other.calls)
    }
}
