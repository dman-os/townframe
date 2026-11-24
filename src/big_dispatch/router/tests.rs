use super::*;

struct Echo;

#[async_trait]
impl Provider for Echo {
    async fn call(
        &self,
        _: &Caller,
        _: &str,
        _: &RequestContext,
        _: &InvocationContext,
        input: Value,
        _: Option<Body>,
    ) -> Result<Body, ProviderError> {
        Ok(Body::Json(input))
    }
}

fn ready() -> ProviderState {
    ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(Echo)))
}

fn ops(nsids: &[&str]) -> Vec<(String, Operation)> {
    nsids
        .iter()
        .map(|n| ((*n).to_owned(), Operation::unchecked(None)))
        .collect()
}

fn provider_of(router: &Router, nsid: &str) -> String {
    router.resolve(nsid).unwrap().provider_id.clone()
}

#[test]
fn claims_followers_see_each_change_of_claims_once_and_a_close_on_retire() {
    let router = Router::default();
    let mut claims = router.follow_claims();
    assert!(!claims.has_changed().unwrap());

    let state = ready();
    router.register("p", ops(&["x.a"]), state.clone()).unwrap();
    router.register("q", ops(&["x.b"]), state.clone()).unwrap();
    // A burst is one pending change to the follower, which then reads the claims as they are.
    assert!(claims.has_changed().unwrap());
    assert_eq!(*claims.borrow_and_update(), 2);
    assert!(!claims.has_changed().unwrap());

    // Registering what is already registered swaps nothing, and routes are not claims.
    router.register("p", ops(&["x.a"]), state).unwrap();
    router.set_routes(BTreeMap::from([(
        "x.a".to_owned(),
        Route::Provider("p".to_owned()),
    )]));
    assert!(!claims.has_changed().unwrap());

    router.unbind("p");
    assert!(claims.has_changed().unwrap());
    claims.borrow_and_update();

    // Retired, the signal closes for its followers and the router still applies claims.
    router.retire_claims();
    router.retire_claims();
    assert!(claims.has_changed().is_err());
    router.unbind("q");
    assert_eq!(
        router.resolve("x.b").unwrap_err(),
        DispatchError::UnknownOperation
    );
}

#[test]
fn registered_operations_resolve_with_their_state_and_kind() {
    let router = Router::default();
    assert_eq!(
        router.resolve("a.b.c").unwrap_err(),
        DispatchError::UnknownOperation
    );
    let state = ready();
    router
        .register(
            "p",
            [(
                "a.b.c".to_owned(),
                Operation::unchecked(Some(OperationKind::Query)),
            )],
            state.clone(),
        )
        .unwrap();
    let entry = router.resolve("a.b.c").unwrap();
    assert_eq!(entry.provider_id, "p");
    assert_eq!(entry.operation_kind, Some(OperationKind::Query));
    assert_eq!(entry.state, state);
}

#[test]
fn register_replaces_the_providers_entries_atomically() {
    let router = Router::default();
    router
        .register("p", ops(&["a.one", "a.two"]), ready())
        .unwrap();
    router
        .register(
            "p",
            ops(&["a.two", "a.three"]),
            ProviderState::NotReady(Readiness::Disabled),
        )
        .unwrap();
    assert_eq!(
        router.resolve("a.one").unwrap_err(),
        DispatchError::UnknownOperation
    );
    for nsid in ["a.two", "a.three"] {
        assert_eq!(
            router.resolve(nsid).unwrap().state,
            ProviderState::NotReady(Readiness::Disabled)
        );
    }
    assert_eq!(router.operations_of("p"), ["a.three", "a.two"]);
}

#[test]
fn unbind_is_idempotent_and_leaves_other_providers_alone() {
    let router = Router::default();
    router.unbind("never-registered");
    router.register("p", ops(&["a.one"]), ready()).unwrap();
    router.register("q", ops(&["b.one"]), ready()).unwrap();
    router.unbind("p");
    router.unbind("p");
    assert_eq!(
        router.resolve("a.one").unwrap_err(),
        DispatchError::UnknownOperation
    );
    assert!(router.operations_of("p").is_empty());
    assert_eq!(provider_of(&router, "b.one"), "q");
}

#[test]
fn two_providers_for_one_nsid_is_ambiguous_never_first_wins() {
    let router = Router::default();
    router.register("p", ops(&["x.op"]), ready()).unwrap();
    router
        .register("q", ops(&["x.op", "q.only"]), ready())
        .unwrap();
    assert_eq!(
        router.resolve("x.op").unwrap_err(),
        DispatchError::NotReady {
            reason: NotReadyReason::Ambiguous
        }
    );
    assert_eq!(provider_of(&router, "q.only"), "q");
    // Removing either claimant resolves the other, whatever order they arrived in.
    router.unbind("p");
    assert_eq!(provider_of(&router, "x.op"), "q");
    router.register("p", ops(&["x.op"]), ready()).unwrap();
    router.unbind("q");
    assert_eq!(provider_of(&router, "x.op"), "p");
}

#[test]
fn a_route_settles_an_ambiguity_and_the_routed_provider_is_never_skipped() {
    let router = Router::default();
    router.register("p", ops(&["x.op"]), ready()).unwrap();
    router.register("q", ops(&["x.op"]), ready()).unwrap();
    router.set_routes([("x.op".to_owned(), Route::Provider("q".into()))].into());
    assert_eq!(provider_of(&router, "x.op"), "q");
    // No fall back to the other claimant while the routed one is not serving.
    router
        .register(
            "q",
            ops(&["x.op"]),
            ProviderState::NotReady(Readiness::Disabled),
        )
        .unwrap();
    assert_eq!(
        router.resolve("x.op").unwrap().state,
        ProviderState::NotReady(Readiness::Disabled)
    );
    router.set_routes(BTreeMap::new());
    assert_eq!(
        router.resolve("x.op").unwrap_err(),
        DispatchError::NotReady {
            reason: NotReadyReason::Ambiguous
        }
    );
}

#[test]
fn a_route_to_a_provider_that_does_not_claim_the_nsid_is_retryable_not_a_fallback() {
    let router = Router::default();
    router.register("p", ops(&["x.op"]), ready()).unwrap();
    router.set_routes([("x.op".to_owned(), Route::Provider("q".into()))].into());
    let missing = DispatchError::NotReady {
        reason: NotReadyReason::RouteTargetMissing {
            provider: "q".into(),
        },
    };
    assert_eq!(router.resolve("x.op").unwrap_err(), missing);
    router.register("q", ops(&["x.op"]), ready()).unwrap();
    assert_eq!(provider_of(&router, "x.op"), "q");
    router.unbind("q");
    assert_eq!(router.resolve("x.op").unwrap_err(), missing);
}

#[test]
fn the_longest_matching_route_wins_and_prefixes_match_whole_segments() {
    let router = Router::default();
    for provider in ["a", "b", "c"] {
        router
            .register(
                provider,
                ops(&["org.x.list", "org.x.sub.get", "org.xy.get"]),
                ready(),
            )
            .unwrap();
    }
    router.set_routes(
        [
            ("org.x".to_owned(), Route::Provider("a".into())),
            ("org.x.sub".to_owned(), Route::Provider("b".into())),
            ("org.x.sub.get".to_owned(), Route::Provider("c".into())),
        ]
        .into(),
    );
    assert_eq!(provider_of(&router, "org.x.list"), "a");
    assert_eq!(provider_of(&router, "org.x.sub.get"), "c");
    // `org.x` does not own `org.xy.get`: three claimants and no route is ambiguous.
    assert_eq!(
        router.resolve("org.xy.get").unwrap_err(),
        DispatchError::NotReady {
            reason: NotReadyReason::Ambiguous
        }
    );
    router.set_routes([("org.x.sub".to_owned(), Route::Provider("b".into()))].into());
    assert_eq!(provider_of(&router, "org.x.sub.get"), "b");
}

#[test]
fn a_damaged_route_refuses_what_it_covers_and_nothing_else() {
    let router = Router::default();
    router
        .register("p", ops(&["a.one", "b.one"]), ready())
        .unwrap();
    router.set_routes(
        [(
            "a".to_owned(),
            Route::Invalid("route `a` is damaged".into()),
        )]
        .into(),
    );
    assert_eq!(
        router.resolve("a.one").unwrap_err(),
        DispatchError::NotReady {
            reason: NotReadyReason::RouteInvalid {
                reason: "route `a` is damaged".into()
            }
        }
    );
    assert_eq!(provider_of(&router, "b.one"), "p");
}

#[test]
fn routes_survive_registrations_and_setting_the_same_routes_swaps_nothing() {
    let router = Router::default();
    let routes: BTreeMap<String, Route> = [("x.op".to_owned(), Route::Provider("p".into()))].into();
    router.set_routes(routes.clone());
    let before = router.snapshot();
    router.set_routes(routes.clone());
    assert!(Arc::ptr_eq(&before, &router.snapshot()));
    router.register("p", ops(&["x.op"]), ready()).unwrap();
    router.unbind("p");
    assert_eq!(router.routes(), routes);
}

#[test]
fn claimants_lists_every_provider_of_an_nsid_ready_or_not() {
    let router = Router::default();
    assert!(router.claimants("x.op").is_empty());
    router.register("q", ops(&["x.op"]), ready()).unwrap();
    router
        .register(
            "p",
            ops(&["x.op"]),
            ProviderState::NotReady(Readiness::Disabled),
        )
        .unwrap();
    assert_eq!(router.claimants("x.op"), ["p", "q"]);
}

#[test]
fn reverse_index_tracks_exactly_what_each_provider_claims() {
    let router = Router::default();
    router
        .register("p", ops(&["a.two", "a.one"]), ready())
        .unwrap();
    router.register("q", ops(&["a.one"]), ready()).unwrap();
    assert_eq!(router.operations_of("p"), ["a.one", "a.two"]);
    assert_eq!(router.operations_of("q"), ["a.one"]);
    router.register("p", ops(&["a.two"]), ready()).unwrap();
    assert_eq!(router.operations_of("p"), ["a.two"]);
    assert_eq!(provider_of(&router, "a.one"), "q");
    router.register("p", [], ready()).unwrap();
    assert!(router.operations_of("p").is_empty());
}

#[test]
fn registering_the_same_claim_again_does_not_swap_the_table() {
    let router = Router::default();
    let state = ready();
    router
        .register("p", ops(&["a.one"]), state.clone())
        .unwrap();
    let before = router.snapshot();
    router.register("p", ops(&["a.one"]), state).unwrap();
    assert!(Arc::ptr_eq(&before, &router.snapshot()));
    router.unbind("absent");
    assert!(Arc::ptr_eq(&before, &router.snapshot()));
}
