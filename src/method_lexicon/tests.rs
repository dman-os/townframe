use super::lexicon;
use method_core::schema::MethodSchema;
use serde_json::Value;

/// Every built-in method: its NSID with the result of assembling its Lexicon. One list, so a
/// method added to a service is one line here, and the count below fails when a service grows one
/// the list does not name.
fn every_builtin() -> Vec<(&'static str, Result<Value, super::MethodLexiconError>)> {
    fn one<M: MethodSchema>() -> (&'static str, Result<Value, super::MethodLexiconError>) {
        (M::XRPC.nsid, lexicon::<M>())
    }
    macro_rules! group {
        ($($method:path),* $(,)?) => { vec![$(one::<$method>()),*] };
    }
    use big_blobs_core::protocol as blobs;
    use big_cabinet_core::authority;
    use big_cabinet_core::doc as docs;
    use big_cabinet_core::drawer::protocol as drawers;
    use big_cartridges_core::protocol as cartridges;
    use big_dispatch_core as dispatch;
    use big_nodule_core::protocol as nodules;
    let mut all = group![
        authority::OpenActor,
        authority::ResolveActor,
        authority::ResolveAuthority,
        authority::MintCapability,
        authority::AttenuateCapability,
        authority::InspectCapability,
        authority::RetractCapability,
        authority::SignDatumClaim,
        authority::ObserveCapability,
        docs::MintDocCapability,
        docs::CreateDoc,
        docs::WriteDatums,
        docs::BranchDoc,
        docs::MergeDoc,
        docs::ReadDatums,
        docs::ListDatums,
        docs::PollDatums,
        docs::QueryDatumRefs,
        docs::UnsafeInspectDatum,
        docs::InspectDatumClaims,
        docs::GetDocAvailability,
        docs::ObserveDocAvailability,
        drawers::MintDrawerCapability,
        drawers::CreateDrawer,
        drawers::AddDoc,
        drawers::CreateMetaDoc,
        drawers::RemoveDoc,
        drawers::AddChild,
        drawers::RemoveChild,
        drawers::GetDrawer,
        drawers::ListDocs,
        drawers::RememberDrawer,
        drawers::ForgetDrawer,
        drawers::ListKnownDrawers,
        drawers::SetDrawerPolicy,
        drawers::SetDrawerSuccessor,
        drawers::ResolveDrawerSuccessor,
        drawers::ListDrawers,
        drawers::GrantDrawer,
        drawers::RevokeDrawer,
        drawers::InvitePrincipal,
        drawers::RevokePrincipal,
        dispatch::Invoke,
        dispatch::MintDispatchCapability,
        nodules::MintNoduleCapability,
        nodules::AdmitNodule,
        nodules::ConfigureNodule,
        nodules::SetNoduleEnabled,
        nodules::SetNoduleDrawerGrants,
        nodules::SetNoduleOperationGrants,
        nodules::ConfirmNoduleResolution,
        nodules::SetNoduleSubstitution,
        nodules::UninstallNodule,
        nodules::GetNodule,
        nodules::ListNodules,
        blobs::PutBlob,
        blobs::ReadBlob,
        blobs::StatBlob,
        cartridges::MintCartridgesCapability,
        cartridges::PublishCartridgeVersion,
        cartridges::RemoveCartridgeVersion,
        cartridges::RemoveCartridge,
        cartridges::ImportCartridgeDocument,
        cartridges::GetPublisher,
        cartridges::ListCartridges,
        cartridges::GetCartridge,
        cartridges::GetCartridgeVersion,
        cartridges::WatchCartridges,
    ];
    all.sort_by_key(|(nsid, _)| *nsid);
    all
}

#[test]
fn every_builtin_method_publishes_a_lexicon_that_passes_the_frame_check() {
    let all = every_builtin();
    let mut nsids: Vec<_> = all.iter().map(|(nsid, _)| *nsid).collect();
    nsids.dedup();
    assert_eq!(nsids.len(), all.len(), "an NSID is listed twice");

    let failures: Vec<_> = all
        .iter()
        .filter_map(|(nsid, result)| {
            result
                .as_ref()
                .err()
                .map(|error| format!("{nsid}: {error}"))
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn lexicon_of<M: MethodSchema>() -> Value {
    lexicon::<M>().expect("the method publishes a Lexicon")
}

#[test]
fn a_procedure_states_its_request_as_the_input_body() {
    insta::assert_json_snapshot!(lexicon_of::<big_cabinet_core::drawer::protocol::AddChild>());
}

#[test]
fn a_query_states_its_request_as_parameters() {
    insta::assert_json_snapshot!(lexicon_of::<big_cabinet_core::doc::GetDocAvailability>());
}

#[test]
fn a_subscription_states_its_event_as_the_message() {
    insta::assert_json_snapshot!(lexicon_of::<big_cabinet_core::doc::ObserveDocAvailability>());
}

#[test]
fn a_method_with_no_result_states_no_output_and_lists_its_errors() {
    let file = lexicon_of::<big_cabinet_core::drawer::protocol::ForgetDrawer>();
    let main = &file["defs"]["main"];
    assert!(main.get("output").is_none());
    let names: Vec<&str> = main["errors"]
        .as_array()
        .expect("the error enum's variants are listed")
        .iter()
        .map(|error| error["name"].as_str().expect("a name"))
        .collect();
    assert!(names.contains(&"Denied"), "{names:?}");
    assert!(
        !names.contains(&"Context"),
        "ContextError is a common error, not a per-method entry: {names:?}"
    );
}
