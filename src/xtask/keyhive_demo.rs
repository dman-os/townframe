use future_form::Local;
use keyhive_core::{
    access::Access,
    keyhive::Keyhive,
    listener::no_listener::NoListener,
    principal::{membered::Membered, peer::Peer},
    store::ciphertext::memory::MemoryCiphertextStore,
};
use keyhive_crypto::signer::memory::MemorySigner as KeyhiveMemorySigner;
use nonempty::nonempty;
use sedimentree_core::{
    codec::{
        decode::{self, DecodeFields},
        encode::{self, EncodeFields},
        error::DecodeError,
        schema::{self, Schema},
    },
    id::SedimentreeId,
};
use subduction_core::peer::id::PeerId;
use subduction_crypto::{
    signed::Signed, signer::memory::MemorySigner as SubductionMemorySigner,
    verified_author::VerifiedAuthor,
};
use subduction_keyhive::{
    policy::{authorize_fetch_with, authorize_put_with},
    test_utils::{
        create_channel_pair, keyhive_peer_id, make_protocol_with_shared_keyhive, run_sync_round,
    },
};

use crate::interlude::*;

type DemoKeyhive = Keyhive<
    Local,
    KeyhiveMemorySigner,
    [u8; 32],
    Vec<u8>,
    MemoryCiphertextStore<[u8; 32], Vec<u8>>,
    NoListener,
    rand08::rngs::OsRng,
>;

#[derive(Debug, Clone, Copy)]
struct DemoWrite {
    nonce: u64,
}

impl Schema for DemoWrite {
    const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
    const TYPE_BYTE: u8 = b'D';
    const VERSION: u8 = 0;
}

impl EncodeFields for DemoWrite {
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        encode::u64(self.nonce, buf);
    }

    fn fields_size(&self) -> usize {
        8
    }
}

impl DecodeFields for DemoWrite {
    const MIN_SIGNED_SIZE: usize = 4 + 32 + 8 + 64;

    fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        Ok((
            Self {
                nonce: decode::u64(buf, 0)?,
            },
            8,
        ))
    }
}

pub async fn cli() -> Res<()> {
    let alice = keyhive_from_seed(0xA1).await?;
    let bob = keyhive_from_seed(0xB0).await?;

    exchange_contact_cards(&alice, &bob).await?;

    let alice_id = keyhive_peer_id(&alice);
    let bob_id = keyhive_peer_id(&bob);
    let (alice_proto, alice_kh, _) = make_protocol_with_shared_keyhive(alice).await;
    let (bob_proto, bob_kh, _) = make_protocol_with_shared_keyhive(bob).await;
    let (alice_conn, bob_conn) = create_channel_pair(alice_id.clone(), &bob_id);

    alice_proto
        .add_peer(bob_id.clone(), alice_conn.clone())
        .await;
    bob_proto.add_peer(alice_id.clone(), bob_conn.clone()).await;

    let created = {
        let kh = alice_kh.lock().await;
        let set_group = kh.generate_group(vec![]).await?;
        let set_group_id = set_group.lock().await.group_id();
        let bob_agent = kh
            .get_agent(bob_id.to_identifier()?)
            .await
            .ok_or_eyre("alice keyhive did not learn bob from contact card")?;

        kh.add_member(
            bob_agent,
            &Membered::Group(set_group_id, Arc::clone(&set_group)),
            Access::Edit,
            &[],
        )
        .await?;

        let first_doc = kh
            .generate_doc(
                vec![Peer::Group(set_group_id, Arc::clone(&set_group))],
                nonempty![[0x11u8; 32]],
            )
            .await?;
        let second_doc = kh
            .generate_doc(
                vec![Peer::Group(set_group_id, Arc::clone(&set_group))],
                nonempty![[0x22u8; 32]],
            )
            .await?;

        let first_doc_id = first_doc.lock().await.doc_id();
        let second_doc_id = second_doc.lock().await.doc_id();
        for ii in 0..25000 {
            let doc = kh
                .generate_doc(
                    vec![Peer::Group(set_group_id, Arc::clone(&set_group))],
                    nonempty![[ii as u8; 32]],
                )
                .await?;
        }
        vec![
            ("notes/today", first_doc_id),
            ("plugin.todo/items", second_doc_id),
        ]
    };

    warn!("doing sync");

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    run_sync_round(
        &alice_proto,
        &bob_proto,
        &alice_id,
        &bob_id,
        &alice_conn,
        &bob_conn,
    )
    .await;

    warn!("sync done");

    let bob_peer = PeerId::new(*bob_id.verifying_key());
    let mallory_signer = SubductionMemorySigner::from_bytes(&[0xC9; 32]);
    let mallory_peer = PeerId::from(mallory_signer.verifying_key());
    let bob_author = verified_author_from_seed(0xB0).await;
    let mallory_author = verified_author_from_seed(0xC9).await;

    let kh = bob_kh.lock().await;
    let reachable = kh.reachable_docs().await;

    println!("alice peer: {alice_id}");
    println!("bob peer:   {bob_id}");
    println!("bob reachable doc count: {}", reachable.len());

    for (label, doc_id) in created {
        let sedimentree_id = SedimentreeId::new(doc_id.to_bytes());
        eyre::ensure!(
            reachable.contains_key(&doc_id),
            "bob did not see reachable doc {label} ({doc_id}) after keyhive sync"
        );
        authorize_fetch_with(&kh, bob_peer, sedimentree_id).await?;
        authorize_put_with(&kh, bob_peer, bob_author, sedimentree_id).await?;

        let mallory_fetch = authorize_fetch_with(&kh, mallory_peer, sedimentree_id).await;
        eyre::ensure!(
            mallory_fetch.is_err(),
            "mallory unexpectedly fetched {label} ({doc_id})"
        );
        let mallory_put =
            authorize_put_with(&kh, mallory_peer, mallory_author, sedimentree_id).await;
        eyre::ensure!(
            mallory_put.is_err(),
            "mallory unexpectedly put {label} ({doc_id})"
        );

        println!("authorized item set member: {label} -> {sedimentree_id}");
    }

    println!("subduction_keyhive policy accepted bob fetch/put and rejected mallory");

    tokio::time::sleep(tokio::time::Duration::from_secs(10000)).await;
    Ok(())
}

async fn keyhive_from_seed(seed: u8) -> Res<DemoKeyhive> {
    let signer = KeyhiveMemorySigner(ed25519_dalek::SigningKey::from_bytes(&[seed; 32]));
    Ok(Keyhive::generate(
        signer,
        MemoryCiphertextStore::new(),
        NoListener,
        rand08::rngs::OsRng,
    )
    .await?)
}

async fn exchange_contact_cards(left: &DemoKeyhive, right: &DemoKeyhive) -> Res<()> {
    let left_card = left.contact_card().await?;
    let right_card = right.contact_card().await?;
    left.receive_contact_card(&right_card).await?;
    right.receive_contact_card(&left_card).await?;
    Ok(())
}

async fn verified_author_from_seed(seed: u8) -> VerifiedAuthor {
    let signer = SubductionMemorySigner::from_bytes(&[seed; 32]);
    let verified = Signed::seal::<future_form::Sendable, _>(&signer, DemoWrite { nonce: 1 }).await;
    verified.verified_author()
}
