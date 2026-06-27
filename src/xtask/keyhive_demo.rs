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

    println!("=== Testing application-secret predecessor key chain ===");

    // Store encrypted content and keys for the chain demo
    let mut pre_grant_enc: Option<beekem::encrypted::EncryptedContent<Vec<u8>, [u8; 32]>> = None;
    let mut post_grant_enc: Option<beekem::encrypted::EncryptedContent<Vec<u8>, [u8; 32]>> = None;
    let mut sealed_pred_key: Option<Vec<u8>> = None;
    let pre_grant_pcs: Option<Vec<u8>> = None;
    let post_grant_pcs: Option<Vec<u8>> = None;

    let doc_id = {
        let kh = alice_kh.lock().await;
        let doc = kh.generate_doc(vec![], nonempty![[0xAAu8; 32]]).await?;
        let doc_id = doc.lock().await.doc_id();
        let doc_id_bytes = doc_id.to_bytes();

        // Encrypt BEFORE adding Bob (pre-grant)
        let pre_ref = [0x01u8; 32];
        let pre_content = b"pre-grant";
        let (enc_pre, key_pre) = kh
            .try_encrypt_content_keyed(doc.clone(), &pre_ref, &vec![], pre_content)
            .await?;
        let pre_ec = enc_pre.encrypted_content().clone();
        println!(
            "Pre-grant  pcs_key_hash: {:?}",
            &pre_ec.pcs_key_hash.raw.as_bytes()[..8]
        );
        pre_grant_enc = Some(pre_ec);

        // Add Bob
        let bob_agent = kh
            .get_agent(bob_id.to_identifier()?)
            .await
            .ok_or_eyre("alice keyhive did not learn bob from contact card")?;
        let update = kh
            .add_member(
                bob_agent,
                &Membered::Document(doc_id, doc.clone()),
                Access::Edit,
                &[],
            )
            .await?;
        // E2EE branch: CGKA ops are fired to the event listener automatically.
        // No manual receive_cgka_op needed.
        println!("Added Bob, cgka_ops={}", update.cgka_ops.len());

        // Encrypt AFTER adding Bob (post-grant)
        let post_ref = [0x02u8; 32];
        let post_content = b"post-grant";
        let (enc_post, key_post) = kh
            .try_encrypt_content_keyed(doc.clone(), &post_ref, &vec![], post_content)
            .await?;
        let post_ec = enc_post.encrypted_content().clone();
        println!(
            "Post-grant pcs_key_hash: {:?}",
            &post_ec.pcs_key_hash.raw.as_bytes()[..8]
        );
        post_grant_enc = Some(post_ec);

        // Build predecessor chain: seal the pre-grant key with the post-grant key.
        // When Bob decrypts the post-grant blob (via CGKA), he gets key_post.
        // From key_post, he can unwrap the sealed predecessor to get key_pre.
        let sealed = key_post
            .try_seal(key_pre.as_slice(), &doc_id_bytes)
            .map_err(|e| eyre::eyre!("try_seal failed: {e}"))?;
        println!(
            "Sealed pred key: {} bytes (key_post sealed key_pre)",
            sealed.len()
        );
        sealed_pred_key = Some(sealed);

        doc_id
    };

    // Sync Alice -> Bob (multiple rounds)
    println!("=== Syncing ===");
    run_sync_round(
        &alice_proto,
        &bob_proto,
        &alice_id,
        &bob_id,
        &alice_conn,
        &bob_conn,
    )
    .await;
    run_sync_round(
        &bob_proto,
        &alice_proto,
        &bob_id,
        &alice_id,
        &bob_conn,
        &alice_conn,
    )
    .await;
    run_sync_round(
        &alice_proto,
        &bob_proto,
        &alice_id,
        &bob_id,
        &alice_conn,
        &bob_conn,
    )
    .await;
    println!("Sync done");

    // Bob: try to decrypt
    {
        let kh = bob_kh.lock().await;
        if let Some(doc) = kh.get_document(doc_id).await {
            let mut locked = doc.lock().await;
            println!("Bob's doc cgka: {}", locked.cgka().is_ok());

            // POST-GRANT: should work via CGKA
            if let Some(ref post_ec) = post_grant_enc {
                match locked.try_decrypt_content_keyed(post_ec) {
                    Ok((pt, key)) => {
                        let text = String::from_utf8_lossy(&pt);
                        println!("POST-GRANT decrypt OK (CGKA): {text}");
                        println!("  Post-grant key available for chain: yes");

                        // Now try PRE-GRANT using CGKA (should fail)
                        if let Some(ref pre_ec) = pre_grant_enc {
                            match locked.try_decrypt_content(pre_ec) {
                                Ok(_) => println!("PRE-GRANT decrypt OK (unexpected!)"),
                                Err(e) => {
                                    println!("PRE-GRANT decrypt via CGKA: FAILED ({e}) — expected forward-secrecy");

                                    // Now try the application-level chain:
                                    // Use the post-grant key to unwrap the sealed predecessor key
                                    if let Some(ref sealed) = sealed_pred_key {
                                        match key.try_open(sealed) {
                                            Ok(pred_key_bytes) => {
                                                let pred_arr: [u8; 32] = pred_key_bytes
                                                    .try_into()
                                                    .expect("key is 32 bytes");
                                                let pred_key =
                                                    keyhive_crypto::symmetric_key::SymmetricKey::from(pred_arr);
                                                match pre_ec.try_decrypt(pred_key) {
                                                    Ok(pt) => {
                                                        let text = String::from_utf8_lossy(&pt);
                                                        println!(
                                                            "PRE-GRANT decrypt via CHAIN: OK! → \"{text}\""
                                                        );
                                                    }
                                                    Err(e) => {
                                                        println!(
                                                            "PRE-GRANT decrypt via chain: FAILED ({e})"
                                                        );
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                println!("try_open sealed pred key failed: {e}");
                                            }
                                        }
                                    } else {
                                        println!("No sealed predecessor key available");
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => println!("POST-GRANT decrypt FAILED: {e}"),
                }
            }
        }
    }

    println!("=== Done ===");
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
