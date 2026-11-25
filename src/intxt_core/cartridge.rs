//! intxt as a cartridge (ledger D38, D41): what admits intxt's vocabularies (`intxt_core`) to a
//! node, as an ADR 047 manifest.
//!
//! The cartridge holds every intxt record under its authority, `org.example.intxt`. Each record
//! ships its Lexicon, which states what a value is, and its sidecar, which states only how the
//! node stores it (ADR 028): `note.content` as collaborative text and `body.order` as a list, so
//! concurrent edits to a file and concurrent appends to an intxt's parts merge instead of the last
//! writer replacing the value. Without the sidecar every position would keep the plain JSON kind
//! its Lexicon states.
//!
//! It also serves three operations, `createIntxt`, `addToIntxt` and `summarizeIntxts`, as routines of its one nodule
//! (ADR 047 §3): each is an export of the handler component (`intxt_nodule::Intxt`). What the
//! cartridge reaches is one list of permbinds (§5), and each routine's `uses` is the part of it
//! that routine takes (ADR 049 §2): the intxt drawers it makes and reads, the datum kinds it reads
//! and writes in them, and the operations of other authorities its handler calls. What an
//! operator grants the cartridge (ADR 049 §4.1) is the person's answer to each. The operations'
//! Lexicons are projected from the Rust types of its methods.

use big_blobs_core::Blob;
use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::drawer::protocol::AddDoc;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::ListDocDrawers;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cartridges_core::Cartridge;
use big_cartridges_core::ComponentExport;
use big_cartridges_core::Necessity;
use big_cartridges_core::Nodule;
use big_cartridges_core::Permbind;
use big_cartridges_core::PermbindDatum;
use big_cartridges_core::PermbindDrawers;
use big_cartridges_core::PermbindMethod;
use big_cartridges_core::Routine;
use big_cartridges_core::RoutineImpl;
use big_cartridges_core::RoutineUse;
use big_cartridges_core::Version;
use big_cartridges_core::test_support::FixtureCartridge;
use indexmap::IndexMap;
use method_core::Method;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::generic;
use crate::methods::AddToIntxt;
use crate::methods::CreateIntxt;
use crate::methods::SummarizeIntxts;
use crate::work;

/// The authority every intxt record is under. A cartridge registers only NSIDs it owns, so the
/// generic datums intxt holds until they migrate out before the beta (ledger D39) are under it too.
pub const AUTHORITY: &str = "org.example.intxt";

/// The key of the handler component in the cartridge.
pub const HANDLER_COMPONENT: &str = "handler";

/// The export of the handler component that serves every routine: the component dispatches on
/// the operation's NSID itself (`big_ndk::Nodule::route`).
const HANDLER_EXPORT: &str = "handle";

/// The permbind key of the operation or datum kind `nsid`: its last segment, which is unique
/// among the operations and kinds the cartridge binds (`permbinds` asserts it).
fn permbind_key(nsid: &str) -> &str {
    nsid.rsplit('.')
        .next()
        .expect("a split always yields a segment")
}

/// The operations `cartridge`'s method permbinds name: what an operator grants it as
/// `grant/method/<nsid>` (ADR 049 §4.1).
pub fn requested_methods(cartridge: &FixtureCartridge) -> impl Iterator<Item = &str> {
    cartridge
        .permbinds
        .values()
        .filter_map(|permbind| match permbind {
            Permbind::Method(method) => Some(method.nsid.as_str()),
            _ => None,
        })
}

/// The datum permbinds, one per kind (ADR 047 §5: `#datum` names exactly one NSID). The access is
/// the union of what the routines below take.
#[rustfmt::skip]
const DATUMS: [(&str, &str, bool, bool, bool, bool); 13] = [
    (generic::Intxt::ID, "intxt markers", true, true, true, true),
    (generic::Index::ID, "intxt index pointers", true, true, true, true),
    (generic::Title::ID, "intxt titles", true, true, true, true),
    (generic::Body::ID, "intxt bodies", true, true, true, true),
    (
        work::WorktreeEntry::ID,
        "worktree entries",
        true,
        true,
        false,
        true,
    ),
    (
        work::RepoEntry::ID,
        "repository entries",
        true,
        true,
        false,
        true,
    ),
    (
        work::SessionEntry::ID,
        "agent session entries",
        true,
        true,
        false,
        true,
    ),
    (
        work::GithubPullEntry::ID,
        "pull request entries",
        true,
        true,
        false,
        true,
    ),
    (
        work::Worktree::ID,
        "the worktrees you add",
        true,
        false,
        false,
        true,
    ),
    (
        work::GitRepo::ID,
        "the git repositories you add",
        true,
        false,
        false,
        true,
    ),
    (
        work::AgentSession::ID,
        "the agent sessions you add",
        true,
        false,
        false,
        true,
    ),
    // Read by `addToIntxt` when the part is a pull request, and by the review micro.
    (
        work::GithubPull::ID,
        "imported GitHub pull requests",
        true,
        false,
        false,
        false,
    ),
    (Blob::ID, "packs and diffs", true, false, true, true),
];

/// intxt's cartridge version, with `wasm` as its handler component: the envelope, the payloads
/// its pins name, and the component's bytes, as a publisher stages and publishes them. Its
/// permbinds are the whole of what it reaches, and an operator grants the cartridge the methods
/// and drawers they name (`intxt_core::install`).
///
/// The version is `0.1.<patch>+<build>`. The build metadata names everything published: the
/// first 16 hex of the BLAKE3 over the handler's bytes and the envelope, Lexicons and sidecars
/// ([`build_digest`]), so a change to the handler or to the manifest alone is another build. A
/// published version never changes content, and `install` skips the build the node already
/// runs, so a build whose identity left out the manifest would never reach a node that holds one
/// with the same handler (that is how `createDoc` went ungranted). The registry orders versions
/// by precedence and must not see two builds that differ in metadata alone, so the order is
/// `patch`, which the caller takes from what the node runs ([`next_version`]). A probe at patch 0
/// names the same build.
///
pub fn cartridge(wasm: &[u8], patch: u64) -> FixtureCartridge {
    let bundle = Bundle::new(&crate::UNIVERSE, Vec::new());
    let mut cartridge = FixtureCartridge::new(Cartridge::new(
        // intxt has no namespace of its own yet, so its id sits under the fixture namespace
        // (`test-support`, enabled by the `cartridge` feature for this alone).
        big_cartridges_core::example_cartridge!("intxt")
            .parse()
            .expect("intxt's cartridge id is valid"),
        // The build metadata is known once everything it names is in.
        version(patch, "0"),
    ));
    cartridge.authority = Some(AUTHORITY.to_owned());
    cartridge.name = Some("intxt".to_owned());
    for lexicon in bundle
        .lexicons()
        .expect("intxt's declarations publish lexicons")
    {
        cartridge.with_lexicon(as_map(lexicon.file));
    }
    for datum in &crate::UNIVERSE.datums {
        let document = Value::from(
            bundle
                .document(&datum.name)
                .expect("every declared datum renders its document"),
        );
        if declares_storage(&document) {
            cartridge.with_sidecar(&datum.id, as_map(document));
        }
    }
    cartridge
        .with_lexicon(as_map(
            method_lexicon::lexicon::<CreateIntxt>()
                .expect("createIntxt's types project a Lexicon"),
        ))
        .with_lexicon(as_map(
            method_lexicon::lexicon::<AddToIntxt>().expect("addToIntxt's types project a Lexicon"),
        ))
        .with_lexicon(as_map(
            method_lexicon::lexicon::<SummarizeIntxts>()
                .expect("summarizeIntxts's types project a Lexicon"),
        ))
        .with_wasm(HANDLER_COMPONENT, wasm);

    manifest(&mut cartridge);
    cartridge.version = version(patch, &build_digest(wasm, &cartridge));
    cartridge
}

/// `0.1.<patch>+<build>`.
fn version(patch: u64, build: &str) -> Version {
    format!("0.1.{patch}+{build}")
        .parse()
        .expect("intxt's version is valid")
}

/// The build metadata of `fixture` built from `wasm`: everything the version publishes. The
/// version itself is not in it, so the patch does not change the build.
fn build_digest(wasm: &[u8], fixture: &FixtureCartridge) -> String {
    let mut envelope = fixture.resolved.cartridge.clone();
    envelope.version = version(0, "0");
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(wasm.len() as u64).to_le_bytes());
    hasher.update(wasm);
    hasher.update(
        &serde_json::to_vec(&(
            &envelope,
            &fixture.resolved.lexicons,
            &fixture.resolved.sidecars,
        ))
        .expect("a cartridge serializes"),
    );
    hasher.finalize().to_hex()[..16].to_owned()
}

/// The build metadata of a version `cartridge` made: what names the build.
pub fn build_of(version: &Version) -> &str {
    version.0.build.as_str()
}

/// The version to publish `build` at, given the `known` versions: the one the node runs and the
/// ones the registry has held (it refuses a version that does not order after the newest it has
/// had, even one since removed from the node). One patch after the highest, which the registry
/// orders after all of them. A legacy version without a patch counter (`0.1.0`,
/// `0.1.0+<hash>`) counts as patch 0, so with no other it becomes `0.1.1+<build>`.
pub fn next_version<'a>(known: impl IntoIterator<Item = &'a Version>, build: &str) -> Version {
    version(
        known
            .into_iter()
            .map(|known| known.0.patch)
            .max()
            .unwrap_or(0)
            + 1,
        build,
    )
}

/// Writes the cartridge's permbinds and its nodule's routines, as the ADR 047 datums themselves.
/// This is the one place they are written: nodules' authoring builder (ADR 049 §2) replaces
/// `manifest` and what it calls, and nothing around it.
fn manifest(cartridge: &mut FixtureCartridge) {
    cartridge.permbinds = permbinds();
    cartridge.nodule = Some(nodule());

    // MICRO: the review micro joins here as `cartridge.micro` (ADR 049 §3, the proposal's
    // `micro` member: component `review`, its description inline, and `uses` of `pullRequests`,
    // `intxts` (read), `githubPull`, the intxt records, `contentBlobs` and `summarizeIntxts`).
    // Nothing in the nodule takes `pullRequests` yet, so only the micro makes it used.
}

/// The cartridge's permbinds: what it reaches, whichever routine reaches it.
fn permbinds() -> BTreeMap<String, Permbind> {
    let necessity = |required: bool| {
        if required {
            Necessity::Required
        } else {
            Necessity::Optional
        }
    };
    let mut permbinds = BTreeMap::<String, Permbind>::new();
    let mut bind = |key: &str, permbind: Permbind| {
        assert!(
            permbinds.insert(key.to_owned(), permbind).is_none(),
            "two permbinds are keyed `{key}`"
        );
    };

    // The intxts' drawers: the ones the cartridge makes and the ones it is granted (ADR 049
    // §4.1), and the node's GitHub drawer, which the cartridge only reads.
    bind(
        "intxts",
        Permbind::Drawers(PermbindDrawers {
            description: "The intxts: one drawer each, made and kept by intxt".to_owned(),
            resolution: Default::default(),
            read: Some(necessity(true)),
            write: Some(necessity(true)),
            create: Some(necessity(true)),
            delete: None,
            extra: Default::default(),
        }),
    );
    bind(
        "pullRequests",
        Permbind::Drawers(PermbindDrawers {
            description: "Your imported GitHub pull requests".to_owned(),
            resolution: Default::default(),
            read: Some(necessity(false)),
            write: None,
            create: None,
            delete: None,
            extra: Default::default(),
        }),
    );

    // One datum permbind per kind: `#datum` names exactly one NSID (ADR 047 §5), so intxt's record
    // family is a permbind each. The access is the union of what the routines below take, and
    // `datums` is the one place it is written. A kind is in `UNIVERSE` or is another
    // authority's (the blob).
    //
    //   (kind,                          what it is,                        read,  write, create, required)
    for (id, what, read, write, create, required) in DATUMS {
        let access = |granted: bool| granted.then(|| necessity(required));
        bind(
            permbind_key(id),
            Permbind::Datum(PermbindDatum {
                nsid: id.parse().expect("a datum kind is an NSID"),
                description: format!("intxt reads and writes {what}"),
                read: access(read),
                write: access(write),
                create: access(create),
                delete: None,
                extra: Default::default(),
            }),
        );
    }

    // The operations of other authorities the routines call. `mintDocCapability` and
    // `mintDrawerCapability` are interim: the routines still mint their own handles as their
    // caller, because the handles the runtime mints from `uses` cannot reach a drawer the
    // routine made nor a part's document in a drawer it was not granted (ADR 049 §6.1). They go
    // with that.
    for nsid in [
        ReadDatums::XRPC.nsid,
        WriteDatums::XRPC.nsid,
        CreateDoc::XRPC.nsid,
        CreateDrawer::XRPC.nsid,
        AddDoc::XRPC.nsid,
        ListDocDrawers::XRPC.nsid,
        MintDocCapability::XRPC.nsid,
        MintDrawerCapability::XRPC.nsid,
    ] {
        bind(
            permbind_key(nsid),
            Permbind::Method(PermbindMethod {
                nsid: nsid.parse().expect("an operation is an NSID"),
                description: format!("intxt calls {nsid}"),
                necessity: Necessity::Required,
                min_revision: None,
                extra: Default::default(),
            }),
        );
    }
    permbinds
}

/// The cartridge's nodule: each routine takes only what its handler touches (ADR 047 §3).
fn nodule() -> Nodule {
    // Each routine takes only what its handler touches (ADR 047 §3). `access` is `read`,
    // `write`, `create`; an operation is `access(false, false, false)`.
    let access = |read, write, create| RoutineUse {
        read,
        write,
        create,
        ..RoutineUse::default()
    };
    let routine = |operation: &str, uses: &[(&str, RoutineUse)]| {
        (
            permbind_key(operation).to_owned(),
            Routine {
                r#impl: RoutineImpl::ComponentExport(ComponentExport {
                    component: HANDLER_COMPONENT.to_owned(),
                    export: HANDLER_EXPORT.to_owned(),
                    extra: Default::default(),
                }),
                implements: operation.parse().expect("an operation is an NSID"),
                uses: uses
                    .iter()
                    .map(|(binding, usage)| (permbind_key(binding).to_owned(), usage.clone()))
                    .collect(),
                extra: Default::default(),
            },
        )
    };
    let call = access(false, false, false);
    Nodule {
        routines: [
            routine(
                CreateIntxt::XRPC.nsid,
                &[
                    ("intxts", access(false, false, true)),
                    (generic::Intxt::ID, access(false, true, true)),
                    (generic::Index::ID, access(false, true, true)),
                    (generic::Title::ID, access(false, true, true)),
                    (generic::Body::ID, access(false, true, true)),
                    (CreateDrawer::XRPC.nsid, call.clone()),
                    (CreateDoc::XRPC.nsid, call.clone()),
                    (WriteDatums::XRPC.nsid, call.clone()),
                    (MintDrawerCapability::XRPC.nsid, call.clone()),
                    (MintDocCapability::XRPC.nsid, call.clone()),
                ],
            ),
            routine(
                AddToIntxt::XRPC.nsid,
                &[
                    ("intxts", access(true, true, false)),
                    (generic::Index::ID, access(true, false, false)),
                    (generic::Body::ID, access(true, true, false)),
                    (work::WorktreeEntry::ID, access(true, true, false)),
                    (work::RepoEntry::ID, access(true, true, false)),
                    (work::SessionEntry::ID, access(true, true, false)),
                    (work::GithubPullEntry::ID, access(true, true, false)),
                    (work::Worktree::ID, access(true, false, false)),
                    (work::GitRepo::ID, access(true, false, false)),
                    (work::AgentSession::ID, access(true, false, false)),
                    (work::GithubPull::ID, access(true, false, false)),
                    (Blob::ID, access(true, false, false)),
                    (ReadDatums::XRPC.nsid, call.clone()),
                    (WriteDatums::XRPC.nsid, call.clone()),
                    (ListDocDrawers::XRPC.nsid, call.clone()),
                    (AddDoc::XRPC.nsid, call.clone()),
                    (MintDrawerCapability::XRPC.nsid, call.clone()),
                    (MintDocCapability::XRPC.nsid, call.clone()),
                ],
            ),
            routine(
                SummarizeIntxts::XRPC.nsid,
                &[
                    ("intxts", access(true, false, false)),
                    (generic::Intxt::ID, access(true, false, false)),
                    (generic::Index::ID, access(true, false, false)),
                    (generic::Title::ID, access(true, false, false)),
                    (generic::Body::ID, access(true, false, false)),
                    (ReadDatums::XRPC.nsid, call.clone()),
                    (MintDocCapability::XRPC.nsid, call),
                ],
            ),
        ]
        .into(),
        ..Default::default()
    }
}

/// Whether `value` declares a storage form anywhere: a record that declares none needs no
/// sidecar, since without one every position keeps the plain JSON kind its Lexicon states.
fn declares_storage(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.contains_key("x-automerge") || map.values().any(declares_storage),
        Value::Array(items) => items.iter().any(declares_storage),
        _ => false,
    }
}

fn as_map(value: Value) -> IndexMap<String, Value> {
    serde_json::from_value(value).expect("a JSON object")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generic;

    /// The build metadata names everything published: the same inputs give the same build (and
    /// the patch does not change it), other handler bytes or another manifest give another one.
    #[test]
    fn the_build_names_the_handler_and_the_manifest() {
        let build = |fixture: &FixtureCartridge| build_of(&fixture.version).to_owned();
        let (one, again, other) = (
            cartridge(b"handler one", 0),
            cartridge(b"handler one", 0),
            cartridge(b"handler two", 0),
        );
        assert_eq!(build(&one), build(&again));
        assert_ne!(build(&one), build(&other));
        assert_eq!(build(&one), build(&cartridge(b"handler one", 7)));
        assert_eq!(build(&one).len(), 16);
        assert!(build(&one).bytes().all(|byte| byte.is_ascii_hexdigit()));
        // The same handler under a manifest without `createDoc`: another build.
        let mut trimmed = cartridge(b"handler one", 0);
        assert!(trimmed.permbinds.remove("createDoc").is_some());
        assert_ne!(build_digest(b"handler one", &trimmed), build(&one));
    }

    /// The patch is the registry's order: a build the node runs is current (its metadata is the
    /// build's), a new build is one patch after the running one, a legacy version counts as
    /// patch 0, and consecutive upgrades, even back to an earlier build, order after each other.
    #[test]
    fn the_version_to_publish_orders_after_the_running_one() {
        let parse = |text: &str| text.parse::<Version>().unwrap();
        assert_eq!(next_version([], "aaaa").to_string(), "0.1.1+aaaa");
        assert_eq!(
            next_version([&parse("0.1.0+legacyhandlerhash")], "bbbb").to_string(),
            "0.1.1+bbbb"
        );
        assert_eq!(
            next_version([&parse("0.1.0")], "bbbb").to_string(),
            "0.1.1+bbbb"
        );
        assert_eq!(
            next_version([&parse("0.1.4+aaaa")], "bbbb").to_string(),
            "0.1.5+bbbb"
        );
        let first = next_version([], "ffff");
        let second = next_version([&first], "0000");
        let third = next_version([&second], "ffff");
        assert!(first < second && second < third, "{first} {second} {third}");
        assert_eq!(build_of(&third), "ffff");
        // The registry has held a newer version than the one the node runs: after the newest.
        assert_eq!(
            next_version([&parse("0.1.2+aaaa"), &parse("0.1.5+cccc")], "dddd").to_string(),
            "0.1.6+dddd"
        );
        assert_eq!(build_of(&parse("0.1.3+abcd")), "abcd");
    }

    use big_cabinet_core::datum::DatumMember;
    use big_cartridges_core::test_support::cartridge_id;
    use big_nodule_core::NoduleAdmission;

    /// The cartridge is admitted as a manifest, holds every record intxt declares so none is left
    /// for another holder, ships the sidecar of each record that declares storage forms, and
    /// serves its operations.
    #[test]
    fn the_cartridge_holds_every_intxt_record() {
        let fixture = cartridge(b"a handler", 0);
        assert_eq!(fixture.id, cartridge_id("intxt"));
        let admitted = fixture
            .resolved
            .admit(fixture.authority.as_deref())
            .expect("intxt's manifest is admitted");
        assert_eq!(
            admitted
                .operations
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            [
                AddToIntxt::XRPC.nsid,
                CreateIntxt::XRPC.nsid,
                SummarizeIntxts::XRPC.nsid
            ],
        );
        // Nothing is requested (and granted) that no part uses. `pullRequests` is the review
        // micro's alone, which the cartridge does not carry yet (see `MICRO` in `cartridge`).
        let nodule = fixture.nodule.as_ref().unwrap();
        for permbind in fixture
            .permbinds
            .keys()
            .filter(|key| *key != "pullRequests")
        {
            assert!(
                nodule
                    .routines
                    .values()
                    .any(|routine| routine.uses.contains_key(permbind)),
                "no routine uses {permbind}"
            );
        }
        // The manifest's Lexicons are a map, so they come back in NSID order, not declaration order.
        let mut held: Vec<&str> = fixture
            .resolved
            .registrable_datums(fixture.authority.as_deref())
            .held
            .iter()
            .map(|(id, _, _)| *id)
            .collect();
        held.sort_unstable();
        for record in [generic::Note::ID, generic::Body::ID] {
            assert!(
                fixture
                    .resolved
                    .sidecars
                    .keys()
                    .any(|nsid| nsid.as_str() == record),
                "{record} has no sidecar"
            );
        }
        let mut declared: Vec<&str> = crate::UNIVERSE
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect();
        declared.sort_unstable();
        let unheld: Vec<_> = fixture
            .resolved
            .datum_lexicons()
            .filter(|file| !held.contains(&file["id"].as_str().unwrap()))
            .map(|file| serde_json::to_string(&file["defs"]["main"]).unwrap())
            .collect();
        assert_eq!(held, declared, "not held: {unheld:?}");
    }

    /// Each routine takes what ADR 047 §3 says it may and no more: the intxt drawers it makes or
    /// reads, the datum kinds it touches in them, and the operations it calls. Every datum
    /// permbind names one kind intxt declares (or the blob), and no grant is held by a permbind
    /// that is not used.
    #[test]
    fn each_routine_uses_only_what_it_needs() {
        let fixture = cartridge(b"a handler", 0);
        let nodule = fixture.nodule.as_ref().unwrap();
        // `r`, `w`, `c` are the accesses taken; none is an operation.
        let uses = |routine: &str| -> BTreeMap<&str, String> {
            nodule.routines[routine]
                .uses
                .iter()
                .map(|(key, usage)| {
                    let access: String = [
                        (usage.read, 'r'),
                        (usage.write, 'w'),
                        (usage.create, 'c'),
                        (usage.delete, 'd'),
                    ]
                    .into_iter()
                    .filter_map(|(taken, letter)| taken.then_some(letter))
                    .collect();
                    (key.as_str(), access)
                })
                .collect()
        };
        let table = |rows: &[(&'static str, &str)]| -> BTreeMap<&'static str, String> {
            rows.iter()
                .map(|(key, access)| (*key, (*access).to_owned()))
                .collect()
        };
        assert_eq!(
            uses("createIntxt"),
            table(&[
                ("intxts", "c"),
                ("intxt", "wc"),
                ("index", "wc"),
                ("title", "wc"),
                ("body", "wc"),
                ("createDrawer", ""),
                ("createDoc", ""),
                ("writeDatums", ""),
                // Interim, until the routines hold the handles the runtime mints.
                ("mintDrawerCapability", ""),
                ("mintDocCapability", ""),
            ]),
        );
        assert_eq!(
            uses("addToIntxt"),
            table(&[
                ("intxts", "rw"),
                ("index", "r"),
                ("body", "rw"),
                ("worktreeEntry", "rw"),
                ("repoEntry", "rw"),
                ("sessionEntry", "rw"),
                ("githubPullEntry", "rw"),
                ("worktree", "r"),
                ("gitRepo", "r"),
                ("agentSession", "r"),
                ("githubPull", "r"),
                ("blob", "r"),
                ("readDatums", ""),
                ("writeDatums", ""),
                ("listDocDrawers", ""),
                ("addDoc", ""),
                ("mintDrawerCapability", ""),
                ("mintDocCapability", ""),
            ]),
        );
        assert_eq!(
            uses("summarizeIntxts"),
            table(&[
                ("intxts", "r"),
                ("intxt", "r"),
                ("index", "r"),
                ("title", "r"),
                ("body", "r"),
                ("readDatums", ""),
                ("mintDocCapability", ""),
            ]),
        );
        let declared: Vec<&str> = crate::UNIVERSE
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .chain([Blob::ID])
            .collect();
        for (key, permbind) in &fixture.permbinds {
            if let Permbind::Datum(datum) = permbind {
                assert!(
                    declared.contains(&datum.nsid.as_str()),
                    "{key} binds {}, which intxt does not declare",
                    datum.nsid
                );
            }
        }
    }
}
