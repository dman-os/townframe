//! The replay server near its target cap (16,384 per session), at 1,638 and 16,380 targets:
//! opening a session, and answering a round that names every target through the responder.
//! Replacing every target in one update (where the id-reuse check once was quadratic) allocates
//! nothing, so it is the instruction bench's (`replay_replace_every_target`).

use futures::executor::block_on;
use utils_rs::testing::alloc_count::measure;

use super::assert_linear;
use crate::interlude::*;
use crate::kit::scheduler::Instant;
use crate::part_store::AccessLevel;
use crate::part_store::PartStoreWrite;
use crate::part_store::laws::principal;
use crate::part_store::memory::MemoryPartStore;
use crate::replay::responder::Responder;
use crate::replay::responder::Timer;
use crate::replay::server::MAX_TARGETS_PER_SESSION;
use crate::replay::server::ReplayServer;
use crate::replay::wire::PageSession;
use crate::replay::wire::Route;
use crate::replay::wire::TargetId;
use crate::replay::wire::UpdateSession;

const SMALL: usize = MAX_TARGETS_PER_SESSION / 10;

fn part_of(row: usize) -> PartKey {
    PartKey::new(&u32::try_from(row).expect("rows fit u32").to_be_bytes())
}

fn route(row: usize) -> (TargetId, Route) {
    (
        u32::try_from(row).expect("rows fit u32"),
        Route::Part {
            part_key: part_of(row),
        },
    )
}

fn session() -> Uuid {
    Uuid::from_u128(1)
}

fn open(routes: std::ops::Range<usize>) -> UpdateSession {
    UpdateSession {
        session_id: session(),
        generation: 0,
        additions: routes.map(route).collect(),
        removals: vec![],
    }
}

#[test]
fn replay_session_open_is_linear() {
    assert_linear("replay server open a session", SMALL, |n| {
        let mut server = ReplayServer::default();
        let update = open(0..n);
        measure(|| {
            server
                .update(&principal(0), &update, Instant::now(), |_| true)
                .expect("opens");
        })
        .1
    });
}

struct NoTimer;

impl Timer for NoTimer {
    fn sleep(&self, _duration: Duration) -> impl Future<Output = ()> + Send + use<> {
        std::future::pending()
    }
}

/// A part store with `n` parts of one member each, all readable by principal 0.
fn crowd(n: usize) -> MemoryPartStore {
    let store = MemoryPartStore::default();
    block_on(async {
        let mut tx = store.begin();
        store
            .grant_access(
                &mut tx,
                &(0..n)
                    .map(|row| (part_of(row), principal(0), AccessLevel::Read))
                    .collect::<Vec<_>>(),
            )
            .await
            .expect("grants");
        let objs: Vec<_> = (0..n)
            .map(|row| (ObjKey::new(part_of(row).as_bytes()), part_of(row)))
            .collect();
        store
            .set_payloads(
                &mut tx,
                objs.iter()
                    .map(|(obj, _)| (obj.clone(), serde_json::json!(1)))
                    .collect(),
            )
            .await
            .expect("payloads");
        store.add_members(&mut tx, &objs).await.expect("members");
        tx.commit();
    });
    store
}

/// One round that names every target of the session, from cursor 0: admission of the update,
/// then the round's authorized read and its answer.
#[test]
fn replay_round_over_every_target_is_linear() {
    assert_linear("replay responder round over every target", SMALL, |n| {
        let me = principal(0);
        let mut responder = Responder::new(crowd(n), NoTimer);
        let mut server = ReplayServer::default();
        let update = open(0..n);
        {
            let admits = block_on(responder.admission(&me, &update)).expect("admission");
            server
                .update(&me, &update, Instant::now(), admits)
                .expect("opens");
        }
        let request = PageSession {
            session_id: session(),
            targets: (0..n).map(|row| (route(row).0, 0)).collect(),
            limit: 1024,
            hold_ms: 0,
        };
        measure(|| {
            let page = block_on(responder.page(&me, |round| {
                server.resolve_round(&me, &request, Instant::now(), round)
            }))
            .expect("store")
            .expect("answers");
            assert_eq!(page.verdicts.len(), n);
        })
        .1
    });
}
