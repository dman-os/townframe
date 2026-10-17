use super::*;
use serde_json::json;

fn route(document: &str, key_id: &str) -> FacetRoute {
    FacetRoute {
        document: document.into(),
        branch: "main".into(),
        tag: "org.example.config".into(),
        key_id: key_id.into(),
    }
}
fn selection(documents: &[&str]) -> WatchSelection {
    WatchSelection {
        documents: documents.iter().map(|id| (*id).into()).collect(),
        branch: "main".into(),
        facets: FacetSelection::Tags(["org.example.config".into()].into()),
    }
}
fn grant(
    host: &Host,
    instance: &Instance,
    documents: &[&str],
    key_id: Option<&str>,
) -> DocumentAccess {
    host.grant_documents(
        instance,
        documents
            .iter()
            .map(|document| DocumentGrant {
                document: (*document).into(),
                branch: "main".into(),
                facets: vec![RoutineFacetAccess {
                    tag: "org.example.config".into(),
                    key_id: key_id.map(str::to_owned),
                    read: true,
                    write: true,
                    create: true,
                    delete: true,
                    ..Default::default()
                }],
            })
            .collect(),
    )
    .unwrap()
}

#[test]
fn multiple_document_mirrors_filter_warm_cache_and_follow_membership() {
    let host = Host::new();
    let node = host.mock_documents();
    node.seed(route("a", "main"), json!(1));
    node.seed(route("a", "secret"), json!(2));
    node.seed(route("b", "main"), json!(3));
    node.seed(route("unrelated", "main"), json!(4));
    let privileged = host.start("privileged");
    let warm = grant(&host, &privileged, &["a"], /*key_id*/ None)
        .watch(selection(&["a"]))
        .unwrap();
    assert_eq!(warm.current().unwrap().facets.len(), 2);
    let instance = host.start("restricted");
    let access = grant(&host, &instance, &["a", "b"], Some("main"));
    let mirror = access.watch(selection(&["a", "b"])).unwrap();
    assert_eq!(
        mirror.current().unwrap().facets,
        BTreeMap::from([
            (route("a", "main"), FacetValue::Present(json!(1))),
            (route("b", "main"), FacetValue::Present(json!(3)))
        ])
    );
    let reads = node.reads();
    assert!(matches!(
        access.watch(selection(&["unrelated"])),
        Err(AccessError::Denied)
    ));
    assert_eq!(node.reads(), reads);
    let before = mirror.current().unwrap();
    node.external_change(route("a", "secret"), Some(json!(99)));
    assert_eq!(mirror.current().unwrap(), before);
    node.external_change(route("b", "main"), /*value*/ None);
    assert_eq!(
        mirror.current().unwrap().facets[&route("b", "main")],
        FacetValue::Absent
    );
    node.external_change(route("b", "main"), Some(json!(5)));
    assert_eq!(
        mirror.current().unwrap().facets[&route("b", "main")],
        FacetValue::Present(json!(5))
    );
    assert!(!node.cached_routes().contains(&route("unrelated", "main")));
    let mut updates = mirror.subscribe().unwrap();
    instance.close();
    assert!(updates.borrow_and_update().revoked);
    assert!(updates.borrow().facets.is_empty());
    assert!(updates.has_changed().is_err());
    host.close();
}

#[test]
fn config_owner_resolution_and_manifest_rights_are_not_conflated() {
    let host = Host::new();
    let node = host.mock_documents();
    node.bind_config("alpha".into(), "a".into(), "main".into());
    node.bind_config("beta".into(), "b".into(), "main".into());
    node.seed(route("a", "main"), json!(1));
    let instance = host.start("alpha");
    let configs = host
        .grant_config(
            &instance,
            "alpha",
            vec![
                RoutineFacetAccess {
                    tag: "org.example.config".into(),
                    key_id: Some("main".into()),
                    read: true,
                    ..Default::default()
                },
                RoutineFacetAccess {
                    tag: "org.example.config".into(),
                    key_id: Some("main".into()),
                    write: true,
                    ..Default::default()
                },
                RoutineFacetAccess {
                    owner_plug_id: Some("beta".into()),
                    tag: "org.example.config".into(),
                    key_id: Some("new".into()),
                    create: true,
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    let facet = configs["alpha"]
        .facet::<Value>("org.example.config".into(), "main".into())
        .unwrap();
    assert_eq!(facet.value(&facet.watch.current().unwrap()), Some(json!(1)));
    facet.write(json!(2)).unwrap();
    assert_eq!(
        configs["alpha"]
            .mutate(
                "org.example.config".into(),
                "main".into(),
                FacetMutation::Delete
            )
            .unwrap_err(),
        AccessError::Denied
    );
    assert_eq!(
        configs["alpha"]
            .mutate(
                "org.example.config".into(),
                "other".into(),
                FacetMutation::Write(json!(3))
            )
            .unwrap_err(),
        AccessError::Denied
    );
    assert!(matches!(
        configs["beta"].facet::<Value>("org.example.config".into(), "new".into()),
        Err(AccessError::Denied)
    ));
    configs["beta"]
        .mutate(
            "org.example.config".into(),
            "new".into(),
            FacetMutation::Create(json!(4)),
        )
        .unwrap();
    assert_eq!(
        configs["beta"]
            .mutate(
                "org.example.config".into(),
                "new".into(),
                FacetMutation::Write(json!(5))
            )
            .unwrap_err(),
        AccessError::Denied
    );
    node.settle(/*accept*/ true);
    assert_eq!(node.reads(), vec![route("a", "main"), route("b", "new")]);
    host.close();
}

#[test]
fn held_refreshes_deduplicate_and_cannot_resurrect_removed_selections() {
    let host = Host::new();
    let node = host.mock_documents();
    node.hold_refreshes(/*hold*/ true);
    node.seed(route("a", "main"), json!(1));
    node.seed(route("b", "main"), json!(2));
    let first = host.start("first");
    let second = host.start("second");
    let access = grant(&host, &first, &["a", "b"], Some("main"));
    let mirror = access.watch(selection(&["a"])).unwrap();
    let other = grant(&host, &second, &["a"], Some("main"))
        .watch(selection(&["a"]))
        .unwrap();
    assert_eq!(node.pending_refreshes(), 1);
    assert!(mirror.current().unwrap().refreshing);
    mirror.select(selection(&["b"])).unwrap();
    other.close();
    node.complete_refreshes();
    assert_eq!(node.reads(), vec![route("b", "main")]);
    let snapshot = mirror.current().unwrap();
    assert!(!snapshot.refreshing);
    assert_eq!(
        snapshot.facets,
        BTreeMap::from([(route("b", "main"), FacetValue::Present(json!(2)))])
    );
    mirror.select(selection(&["a"])).unwrap();
    first.close();
    node.complete_refreshes();
    assert_eq!(node.reads(), vec![route("b", "main")]);
    host.close();
}

#[test]
fn optimistic_overlays_are_shared_authorized_and_pinned_after_watch_closure() {
    let host = Host::new();
    let node = host.mock_documents();
    node.seed(route("a", "main"), json!(1));
    let first = host.start("writer");
    let second = host.start("reader");
    let access = grant(&host, &first, &["a"], Some("main"));
    let mirror = access.watch(selection(&["a"])).unwrap();
    let other = grant(&host, &second, &["a"], Some("main"))
        .watch(selection(&["a"]))
        .unwrap();
    access
        .mutate(route("a", "main"), FacetMutation::Write(json!(2)))
        .unwrap();
    access
        .mutate(route("a", "main"), FacetMutation::Write(json!(3)))
        .unwrap();
    mirror.refresh().unwrap();
    assert_eq!(
        other.current().unwrap().facets[&route("a", "main")],
        FacetValue::Present(json!(3))
    );
    assert_eq!(other.current().unwrap().pending.len(), 2);
    first.close();
    node.settle(/*accept*/ true);
    assert!(other.current().unwrap().pending.is_empty());
    assert_eq!(
        other.current().unwrap().facets[&route("a", "main")],
        FacetValue::Present(json!(3))
    );
    second.close();
    assert!(node.cached_routes().contains(&route("a", "main")));
    host.close();
}

#[test]
fn only_unretained_projection_values_are_evicted() {
    let host = Host::new();
    let node = host.mock_documents();
    let instance = host.start("cache-reader");
    let documents = (0..40).map(|id| format!("doc-{id}")).collect::<Vec<_>>();
    for document in &documents {
        node.seed(route(document, "main"), json!(document));
    }
    let access = grant(
        &host,
        &instance,
        &documents.iter().map(String::as_str).collect::<Vec<_>>(),
        Some("main"),
    );
    let retained = access
        .watch(selection(
            &documents.iter().map(String::as_str).collect::<Vec<_>>(),
        ))
        .unwrap();
    assert_eq!(node.cached_routes().len(), 40);
    drop(retained);
    assert_eq!(node.cached_routes().len(), 32);
    host.close();
}
