//! Reproducer for the stress-flake delegation-propagation failure.
//!
//! Mirrors the daybook shape in miniature: a group containing a peer, a
//! document created with the group as coparent, then a full keyhive sync
//! round. The flake hypothesis under test: the creator's serving-side
//! visibility projection (or the sync itself) fails to deliver the fresh
//! document's delegation events to the group's members.

use keyhive_core::{
    access::Access,
    principal::{
        membered::Membered,
        peer::Peer,
        identifier::Identifier,
    },
};
use nonempty::nonempty;
use subduction_keyhive::test_utils::{
    create_channel_pair, keyhive_peer_id, make_keyhive, make_protocol_with_shared_keyhive,
};

use crate::interlude::*;

pub async fn cli() -> Res<()> {
    let iterations = std::env::var("KEYHIVE_VIS_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let mut failures = 0_usize;
    for i in 0..iterations {
        if let Err(err) = one_round(i).await {
            failures += 1;
            println!("── iteration {i}: FAILED\n{err:#}");
        } else {
            println!("── iteration {i}: ok");
        }
    }
    println!("\n{failures}/{iterations} iterations failed");
    if failures > 0 {
        eyre::bail!("{failures} of {iterations} iterations failed to propagate doc delegations");
    }
    Ok(())
}

async fn one_round(i: usize) -> Res<()> {
    let alice = make_keyhive().await;
    let bob = make_keyhive().await;

    let (a_card, b_card) = (alice.contact_card().await?, bob.contact_card().await?);
    alice.receive_contact_card(&b_card).await?;
    bob.receive_contact_card(&a_card).await?;

    let alice_id = keyhive_peer_id(&alice);
    let bob_id = keyhive_peer_id(&bob);

    let (alice_proto, alice, _) = make_protocol_with_shared_keyhive(alice).await;
    let (bob_proto, bob, _) = make_protocol_with_shared_keyhive(bob).await;
    let (a_conn, b_conn) = create_channel_pair(alice_id.clone(), &bob_id);
    alice_proto.add_peer(bob_id.clone(), a_conn.clone()).await;
    bob_proto.add_peer(alice_id.clone(), b_conn.clone()).await;

    // Group owned by alice, bob added as a member — the daybook
    // `content_docs_group` shape.
    let group = alice.generate_group(vec![]).await?;
    let bob_ident_on_alice = bob_id
        .to_identifier()
        .map_err(|e| eyre::eyre!("bob identifier: {e}"))?;
    let bob_agent_on_alice = alice
        .get_agent(bob_ident_on_alice)
        .await
        .ok_or_eyre("alice has no agent for bob")?;
    let gid = group.lock().await.group_id();
    alice
        .add_member(
            bob_agent_on_alice.clone(),
            &Membered::Group(gid, group.clone()),
            Access::Read,
            &[],
        )
        .await?;

    verbose_round(
        "round1", &alice_proto, &bob_proto, &alice_id, &bob_id, &a_conn, &b_conn,
    )
    .await;

    // Sanity: bob must know the group before the doc exists.
    let bob_has_group = bob.get_group(gid).await.is_some();
    if !bob_has_group {
        eyre::bail!("bob did not receive the group in round 1");
    }

    // Creator-side serving projection BEFORE any sync of the new doc.
    let events_before = alice.static_events_for_agent(&bob_agent_on_alice).await;

    // The doc creation under test: coparent = the group.
    let doc = alice
        .generate_doc(
            vec![Peer::Group(gid, group.clone())],
            nonempty![[0u8; 32]],
        )
        .await?;
    let doc_id = doc.lock().await.doc_id();

    // big_repo's create_document calls this after persisting the new events;
    // mirror it so the creator's cache generation is bumped like in prod.
    alice_proto.note_local_keyhive_changed().await?;

    let events_after = alice.static_events_for_agent(&bob_agent_on_alice).await;
    let new_visible = events_after.len() as i64 - events_before.len() as i64;

    let doc_reachable = alice
        .docs_reachable_by_agent(&bob_agent_on_alice)
        .await
        .contains_key(&doc_id);

    // Full sync round: this is what should carry the delegations to bob.
    verbose_round(
        "round2", &alice_proto, &bob_proto, &alice_id, &bob_id, &a_conn, &b_conn,
    )
    .await;

    let bob_self_ident: Identifier = bob.id().into();
    let bob_agent_on_bob = bob
        .get_agent(bob_self_ident)
        .await
        .ok_or_eyre("bob has no self agent")?;
    let mut bob_got_doc = bob
        .docs_reachable_by_agent(&bob_agent_on_bob)
        .await
        .contains_key(&doc_id);

    // If the first round failed, try a few more to distinguish
    // "server never advertises" from "harness under-drains messages".
    let mut extra_rounds = 0_usize;
    while !bob_got_doc && extra_rounds < 3 {
            verbose_round(
                "extra", &alice_proto, &bob_proto, &alice_id, &bob_id, &a_conn, &b_conn,
            )
        .await;
        extra_rounds += 1;
        bob_got_doc = bob
            .docs_reachable_by_agent(&bob_agent_on_bob)
            .await
            .contains_key(&doc_id);
    }

    println!(
        "   iteration {i}: new_events_visible_to_bob_in_creator_projection={new_visible} \
         doc_reachable_by_bob_on_creator={doc_reachable} bob_got_doc_after_sync={bob_got_doc} \
         extra_rounds={extra_rounds}"
    );

    if !doc_reachable || !bob_got_doc {
        eyre::bail!(
            "propagation mismatch: creator_projection_reachable={doc_reachable} bob_got_doc={bob_got_doc}"
        );
    }
    Ok(())
}

/// `run_sync_round` with message tracing.
async fn verbose_round(
    label: &str,
    initiator_proto: &subduction_keyhive::test_utils::TestProtocol,
    responder_proto: &subduction_keyhive::test_utils::TestProtocol,
    initiator_id: &subduction_keyhive::KeyhivePeerId,
    responder_id: &subduction_keyhive::KeyhivePeerId,
    initiator_conn: &subduction_keyhive::test_utils::ChannelConnection,
    responder_conn: &subduction_keyhive::test_utils::ChannelConnection,
) {
    println!("   [{label}] initiator sync_keyhive");
    initiator_proto
        .sync_keyhive(Some(responder_id))
        .await
        .expect("initiator sync_keyhive failed");

    let sync_request = responder_conn
        .inbound_rx
        .recv()
        .await
        .expect("failed to receive sync request");
    println!("   [{label}] responder <- {}", "signed-message");
    responder_proto
        .handle_message(initiator_id, sync_request, None)
        .await
        .expect("responder failed to handle sync request");

    let sync_response = initiator_conn
        .inbound_rx
        .recv()
        .await
        .expect("failed to receive sync response");
    println!("   [{label}] initiator <- {}", "signed-message");
    initiator_proto
        .handle_message(responder_id, sync_response, None)
        .await
        .expect("initiator failed to handle sync response");

    loop {
        let mut handled = false;

        if let Ok(msg) = responder_conn.inbound_rx.try_recv() {
            println!("   [{label}] responder <- {}", "signed-message");
            responder_proto
                .handle_message(initiator_id, msg, None)
                .await
                .expect("responder failed to handle message");
            handled = true;
        }

        if let Ok(msg) = initiator_conn.inbound_rx.try_recv() {
            println!("   [{label}] initiator <- {}", "signed-message");
            initiator_proto
                .handle_message(responder_id, msg, None)
                .await
                .expect("initiator failed to handle message");
            handled = true;
        }

        if !handled {
            break;
        }
    }
}
