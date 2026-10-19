use super::*;
use std::{path::PathBuf, sync::Arc};
use tempfile::TempDir;

async fn store_and_path() -> (Arc<SecretStore>, TempDir, PathBuf) {
    let keyring = keyring_core::mock::Store::new().expect("mock keyring")
        as Arc<keyring_core::CredentialStore>;
    let store = Arc::new(
        SecretStore::boot_with_store(keyring)
            .await
            .expect("mock keyring"),
    );
    let directory = tempfile::tempdir().expect("authority state directory");
    let path = directory.path().join("authority.sqlite");
    (store, directory, path)
}

fn facet_claim(actor_uuid: Uuid7, seed: u8) -> FacetClaim {
    FacetClaim {
        origin_document: big_cabinet_core::DocumentId::new(b"mutation-document"),
        facet_key: big_cabinet_core::FacetKey::from_text("org.example.note", "main")
            .expect("facet key"),
        chosen_uuid: Uuid7::new(),
        actor_uuid,
        operation: big_cabinet_core::dmeta::ClaimOperation::Set,
        value_cid: Some({
            let bytes = {
                let mut cid = vec![0x01, 0x55, 0x1e, 0x20];
                cid.extend([seed; 32]);
                cid
            };
            big_cabinet_core::dmeta::Cid::try_from(bytes.as_slice()).expect("cid")
        }),
        predecessors: Vec::new(),
    }
}

#[tokio::test]
async fn boot_returns_operator_root_and_reopen_is_stable() {
    let (store, _dir, path) = store_and_path().await;
    let (repo, root) = AuthorityRepo::boot(Arc::clone(&store), &path)
        .await
        .expect("boot");
    let verified = repo.verify_issued(&root).await.expect("root issued");
    assert_eq!(verified.root_actor, Some(repo.operator_public_key()));
    let (reopened, root2) = AuthorityRepo::boot(store, &path).await.expect("reopen");
    assert_eq!(root, root2);
    assert_eq!(repo.operator_uuid(), reopened.operator_uuid());
    assert_eq!(repo.node_public_key(), reopened.node_public_key());
}

#[tokio::test]
async fn actor_creation_binds_uuid_url_and_certificate() {
    let (store, _dir, path) = store_and_path().await;
    let (repo, root) = AuthorityRepo::boot(store, &path).await.expect("boot");
    let actor = repo
        .create_actor(&root, "rt/plugs/ocr")
        .await
        .expect("actor");
    assert!(actor.url.ends_with("/rt/plugs/ocr"), "{}", actor.url);
    // Certificate verifies against the node issuer and the dmeta binding.
    let metadata = big_cabinet_core::dmeta::ActorMetadata {
        pubkey: MultibaseKey::try_from(actor.public_key.as_slice()).expect("key"),
        actor_url: actor.url.clone(),
        delegation: Some(actor.certificate.clone()),
    };
    big_cabinet_core::dmeta::Attribution::verify(
        &actor.certificate,
        &repo.node_issuer(),
        actor.uuid,
        &metadata,
    )
    .expect("attribution");
    let found = repo
        .actor_of(&actor.cap)
        .await
        .expect("actor_of")
        .expect("actor");
    assert_eq!(found.uuid, actor.uuid);
    // A path that does not extend the subsystem's own path is refused.
    assert!(matches!(
        repo.create_actor(&actor.cap, "other/thing").await,
        Err(AuthorityError::InvalidActorPath)
    ));
    let child = repo
        .create_actor(&actor.cap, "rt/plugs/ocr/worker")
        .await
        .expect("child actor");
    assert!(child.url.ends_with("/rt/plugs/ocr/worker"));
}

#[tokio::test]
async fn caps_narrow_and_retraction_invalidates() {
    let (store, _dir, path) = store_and_path().await;
    let (repo, root) = AuthorityRepo::boot(store, &path).await.expect("boot");
    let actor = repo.create_actor(&root, "docs").await.expect("actor");
    let edit = repo
        .mint(&actor.cap, "doc:xyz", Rights::USE)
        .await
        .expect("mint");
    repo.verify_issued(&edit).await.expect("issued");
    // Escalation is refused.
    assert!(matches!(
        repo.mint(&edit, "doc:xyz", Rights::MINT_CHILD).await,
        Err(AuthorityError::Escalation) | Err(AuthorityError::Unauthorized)
    ));
    repo.retract(&edit).await.expect("retract");
    assert!(matches!(
        repo.verify_issued(&edit).await,
        Err(AuthorityError::InvalidCap)
    ));
}

#[tokio::test]
async fn ledger_is_generic_and_exact() {
    let (store, _dir, path) = store_and_path().await;
    let (repo, _root) = AuthorityRepo::boot(store, &path).await.expect("boot");
    assert!(
        !repo
            .has_grant("a", "drawer:d", "Write")
            .await
            .expect("none")
    );
    repo.grant("a", "drawer:d", "Write").await.expect("grant");
    assert!(repo.has_grant("a", "drawer:d", "Write").await.expect("has"));
    assert!(
        !repo
            .has_grant("a", "drawer:d", "Read")
            .await
            .expect("exact")
    );
    repo.revoke_grant("a", "drawer:d", "Write")
        .await
        .expect("revoke");
    assert!(
        !repo
            .has_grant("a", "drawer:d", "Write")
            .await
            .expect("gone")
    );
}

#[tokio::test]
async fn sign_as_signs_as_root_actor_and_binds_actor_uuid() {
    let (store, _dir, path) = store_and_path().await;
    let (repo, root) = AuthorityRepo::boot(store, &path).await.expect("boot");
    let actor = repo.create_actor(&root, "docs").await.expect("actor");
    let edit = repo
        .mint(&actor.cap, "doc:xyz", Rights::USE)
        .await
        .expect("mint");
    let claim = facet_claim(actor.uuid, 7);
    let signed = repo.sign_as(&edit, &claim).await.expect("sign");
    signed.verify().expect("core verify");
    assert_eq!(
        signed.actor_public_key,
        MultibaseKey::try_from(actor.public_key.as_slice()).expect("key")
    );
    repo.verify_claim(&signed).expect("verify claim");
    // Wrong actor UUID in the claim is refused.
    let wrong = facet_claim(Uuid7::new(), 7);
    assert!(matches!(
        repo.sign_as(&edit, &wrong).await,
        Err(AuthorityError::Unauthorized)
    ));
}

#[tokio::test]
async fn actor_keys_survive_reopen_and_old_actor_still_signs() {
    let (store, _dir, path) = store_and_path().await;
    let (repo, root) = AuthorityRepo::boot(Arc::clone(&store), &path)
        .await
        .expect("boot");
    let actor = repo.create_actor(&root, "docs").await.expect("actor");
    let (reopened, root2) = AuthorityRepo::boot(store, &path).await.expect("reopen");
    assert_eq!(root, root2);
    let edit = reopened
        .mint(&actor.cap, "doc:xyz", Rights::USE)
        .await
        .expect("mint after reopen");
    let claim = facet_claim(actor.uuid, 9);
    reopened
        .sign_as(&edit, &claim)
        .await
        .expect("sign after reopen");
}

#[tokio::test]
async fn cancelled_actor_provisioning_recovers_on_reopen() {
    let (store, _dir, path) = store_and_path().await;
    let (repo, root) = AuthorityRepo::boot(Arc::clone(&store), &path)
        .await
        .expect("boot");
    // Provision a key directly, then create the actor after reopen: registry survives.
    let actor = repo.create_actor(&root, "svc").await.expect("actor");
    let (reopened, _root2) = AuthorityRepo::boot(store, &path).await.expect("reopen");
    let edit = reopened
        .mint(&actor.cap, "doc:z", Rights::USE)
        .await
        .expect("mint");
    let claim = facet_claim(actor.uuid, 3);
    reopened.sign_as(&edit, &claim).await.expect("sign");
}

#[tokio::test]
async fn unknown_and_forged_caps_are_refused() {
    let (store, _dir, path) = store_and_path().await;
    let (repo, _root) = AuthorityRepo::boot(store, &path).await.expect("boot");
    assert!(matches!(
        repo.verify_issued(&[9u8; 16]).await,
        Err(AuthorityError::InvalidCap)
    ));
    assert!(matches!(
        repo.actor_of(&[9u8; 16]).await,
        Err(AuthorityError::InvalidCap)
    ));
}

#[tokio::test]
async fn actor_publication_fault_is_typed_and_recoverable() {
    use super::provisioning::provisioning_test_faults;
    let (store, _dir, path) = store_and_path().await;
    let (repo, root) = AuthorityRepo::boot(Arc::clone(&store), &path)
        .await
        .expect("boot");
    provisioning_test_faults::fail_actor_publication();
    let failed = repo.create_actor(&root, "svc").await;
    assert!(failed.is_err(), "{failed:?}");
    repo.reconcile_pending().await.expect("reconcile");
    let (reopened, root2) = AuthorityRepo::boot(store, &path).await.expect("reopen");
    let actor = reopened
        .create_actor(&root2, "svc")
        .await
        .expect("actor after recovery");
    assert!(actor.url.ends_with("/svc"));
}
