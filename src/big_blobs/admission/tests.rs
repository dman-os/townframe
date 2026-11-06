use super::*;
use big_cabinet_core::authority::AuthorityMessage;
use big_cabinet_core::authority::AuthorityProtocol;
use futures::future::BoxFuture;
use service_utils_rs::InlineRoute;

fn database() -> Database {
    sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap()
}

fn inventory() -> DocId {
    DocId::new([83; 32])
}
fn digest() -> BlobId {
    BlobId::new([84; 32])
}
fn peer() -> PeerKey {
    PeerKey::new([85; 32])
}

async fn selected(sql: Database) -> Res<(Admission, service_utils_rs::StopToken)> {
    let (admission, stop) = Admission::boot(sql).await?;
    admission.select(&inventory(), "root", true).await?;
    admission.authorize(&inventory(), true).await?;
    Ok((admission, stop))
}

async fn pin(admission: &Admission, revision: u64) -> Res<()> {
    admission
        .pins(
            &inventory(),
            revision,
            &BTreeMap::from([(digest(), 42)]),
            true,
        )
        .await
}

async fn object(admission: &Admission, revision: u64, present: bool) -> Res<()> {
    admission
        .observe(&inventory(), digest(), &peer(), revision, present, None)
        .await
}

#[tokio::test]
async fn both_orderings_park_durably_and_require_reauthorization_after_restart() -> Res<()> {
    for pin_first in [false, true] {
        let sql = database();
        let (admission, stop) = selected(sql.clone()).await?;
        if pin_first {
            pin(&admission, 1).await?;
        } else {
            object(&admission, 10, true).await?;
        }
        assert!(admission.begin(&inventory(), digest()).await?.is_none());
        stop.stop().await;
        drop(admission);
        let (admission, stop) = Admission::boot(sql).await?;
        admission.recover().await?;
        if pin_first {
            object(&admission, 10, true).await?;
        } else {
            pin(&admission, 1).await?;
        }
        assert!(admission.begin(&inventory(), digest()).await?.is_none());
        admission.authorize(&inventory(), true).await?;
        let attempt = admission.begin(&inventory(), digest()).await?.unwrap();
        assert_eq!(attempt.peer, peer());
        assert!(admission.complete(&attempt, true).await?);
        let state = admission.state(&inventory(), digest()).await?.unwrap();
        assert!(state.advertisable());
        assert!(state.retains_bytes());
        stop.stop().await;
    }
    Ok(())
}

#[tokio::test]
async fn identical_digest_cannot_cross_unlock_another_inventory() -> Res<()> {
    let (admission, stop) = selected(database()).await?;
    let other = DocId::new([86; 32]);
    admission.select(&other, "root", true).await?;
    admission.authorize(&other, true).await?;
    pin(&admission, 1).await?;
    admission
        .observe(&other, digest(), &peer(), 10, true, None)
        .await?;
    assert!(admission.begin(&inventory(), digest()).await?.is_none());
    assert!(admission.begin(&other, digest()).await?.is_none());
    object(&admission, 10, true).await?;
    let attempt = admission.begin(&inventory(), digest()).await?.unwrap();
    assert!(admission.complete(&attempt, true).await?);
    assert!(
        admission
            .state(&other, digest())
            .await?
            .is_some_and(|state| !state.bytes && !state.advertisable())
    );
    assert_eq!(
        admission
            .inventory(&crate::workers::inventory_part(&other))
            .await?,
        Some(other)
    );
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn source_observations_follow_latest_state_across_restart() -> Res<()> {
    let sql = database();
    let (admission, stop) = selected(sql.clone()).await?;
    pin(&admission, 1).await?;
    object(&admission, 10, true).await?;
    object(&admission, 20, false).await?;
    stop.stop().await;
    drop(admission);
    let (admission, stop) = Admission::boot(sql).await?;
    admission.recover().await?;
    admission.authorize(&inventory(), true).await?;
    object(&admission, 10, true).await?;
    assert!(
        admission
            .state(&inventory(), digest())
            .await?
            .unwrap()
            .observed
    );
    object(&admission, 20, false).await?;
    object(&admission, 30, true).await?;
    assert!(admission.begin(&inventory(), digest()).await?.is_some());
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn late_completions_are_fenced_by_selection_removal_authority_and_restart() -> Res<()> {
    for transition in 0..5 {
        let sql = database();
        let (admission, stop) = selected(sql.clone()).await?;
        pin(&admission, 1).await?;
        object(&admission, 10, true).await?;
        let attempt = admission.begin(&inventory(), digest()).await?.unwrap();
        match transition {
            0 => admission.select(&inventory(), "root", false).await?,
            1 => object(&admission, 20, false).await?,
            2 => admission.authorize(&inventory(), false).await?,
            3 => {
                admission
                    .pins(&inventory(), 2, &BTreeMap::new(), true)
                    .await?
            }
            4 => admission.recover().await?,
            _ => unreachable!(),
        }
        assert!(!admission.complete(&attempt, true).await?);
        let state = admission.state(&inventory(), digest()).await?.unwrap();
        assert!(!state.advertisable());
        assert!(!state.bytes);
        stop.stop().await;
    }
    Ok(())
}

#[tokio::test]
async fn incomplete_pins_preserve_facts_and_claims_are_independent() -> Res<()> {
    let (admission, stop) = selected(database()).await?;
    pin(&admission, 1).await?;
    object(&admission, 10, true).await?;
    let attempt = admission.begin(&inventory(), digest()).await?.unwrap();
    admission
        .pins(&inventory(), 2, &BTreeMap::new(), false)
        .await?;
    assert!(admission.complete(&attempt, true).await?);
    admission.select(&inventory(), "second-root", true).await?;
    admission.select(&inventory(), "root", false).await?;
    assert!(
        admission
            .state(&inventory(), digest())
            .await?
            .unwrap()
            .advertisable()
    );
    admission.select(&inventory(), "second-root", false).await?;
    let state = admission.state(&inventory(), digest()).await?.unwrap();
    assert!(!state.advertisable());
    assert!(state.retains_bytes());
    object(&admission, 20, false).await?;
    assert!(
        !admission
            .state(&inventory(), digest())
            .await?
            .unwrap()
            .retains_bytes()
    );
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn writer_only_production_never_schedules_mirror_downloads() -> Res<()> {
    let (admission, stop) = Admission::boot(database()).await?;
    admission
        .produce(&inventory(), digest(), "document", true)
        .await?;
    admission.authorize(&inventory(), true).await?;
    pin(&admission, 1).await?;
    assert!(
        admission
            .state(&inventory(), digest())
            .await?
            .unwrap()
            .advertisable()
    );
    assert!(admission.begin(&inventory(), digest()).await?.is_none());
    admission
        .produce(&inventory(), digest(), "document", false)
        .await?;
    assert!(
        !admission
            .state(&inventory(), digest())
            .await?
            .unwrap()
            .advertisable()
    );
    assert!(admission.begin(&inventory(), digest()).await?.is_none());
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn failed_attempt_cannot_clear_a_later_attempt_or_acknowledge_new_work() -> Res<()> {
    let (admission, stop) = selected(database()).await?;
    pin(&admission, 1).await?;
    object(&admission, 10, true).await?;
    let first = admission.begin(&inventory(), digest()).await?.unwrap();
    admission.fail(&first).await?;
    admission.rearm().await?;
    let second = admission.begin(&inventory(), digest()).await?.unwrap();
    assert!(second.generation > first.generation);
    admission.fail(&first).await?;
    assert!(!admission.complete(&first, true).await?);
    assert!(
        admission
            .state(&inventory(), digest())
            .await?
            .unwrap()
            .fetching
    );
    admission
        .settle(&inventory(), digest(), first.generation)
        .await?;
    assert_eq!(admission.dirty().await?.len(), 1);
    assert!(admission.complete(&second, true).await?);
    admission.available(&inventory(), digest(), false).await?;
    assert!(
        !admission
            .state(&inventory(), digest())
            .await?
            .unwrap()
            .advertisable()
    );
    assert_eq!(admission.length(&inventory(), digest()).await?, Some(42));
    assert_eq!(admission.inventories().await?, vec![inventory()]);
    stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn sources_and_payloadless_observations_are_independent() -> Res<()> {
    let (admission, stop) = selected(database()).await?;
    pin(&admission, 1).await?;
    object(&admission, 10, true).await?;
    let other = PeerKey::new([87; 32]);
    admission
        .observe(&inventory(), digest(), &other, 1, true, None)
        .await?;
    object(&admission, 20, false).await?;
    let attempt = admission.begin(&inventory(), digest()).await?.unwrap();
    assert_eq!(attempt.peer, other);
    assert!(admission.complete(&attempt, true).await?);
    assert!(
        admission
            .state(&inventory(), digest())
            .await?
            .unwrap()
            .advertisable()
    );
    stop.stop().await;
    Ok(())
}

/// A capability authority for tests that never mint client capabilities.
struct UnusedAuthority;

impl InlineRoute<AuthorityProtocol> for UnusedAuthority {
    fn route_inline(self: Arc<Self>, _: AuthorityMessage) -> BoxFuture<'static, ()> {
        Box::pin(async { panic!("these admission tests do not mint client capabilities") })
    }
}

mod discovery;
mod model;
mod producers;
mod project;
mod restart;
mod runtime;
