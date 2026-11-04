use super::*;

mod cleanup;

fn commit(authority: &mut Authority, command: Command) -> (Ack, Vec<Intent>) {
    let Decision::Commit(proposal) = authority.prepare(command).unwrap() else {
        panic!("command must commit")
    };
    let intents = proposal.intents.clone();
    let ack = authority.publish(*proposal);
    (ack, intents)
}

fn submit_command() -> Command {
    Command::Submit {
        key: "job".into(),
        args: "args".into(),
        policy: RetryPolicy::Immediate,
        budget: RetryBudget::default(),
        retention_ms: DEFAULT_RETENTION_MS,
    }
}

fn submit(authority: &mut Authority) {
    commit(authority, submit_command());
}

fn grant(authority: &mut Authority, worker: u64) -> (Lease, Vec<Step>) {
    let (_, intents) = commit(
        authority,
        Command::Grant {
            key: "job".into(),
            incarnation: 1,
            worker,
            now_ms: 0,
        },
    );
    let [Intent::Execute { lease, replay, .. }] = intents.as_slice() else {
        panic!("grant must execute a reconstructed invocation")
    };
    (lease.clone(), replay.clone())
}

fn step(lease: &Lease, id: u64, value: &str) -> Command {
    step_as(lease, id, value, StepKind::Effect, None)
}

fn step_as(lease: &Lease, id: u64, value: &str, kind: StepKind, name: Option<&str>) -> Command {
    Command::CommitStep {
        key: "job".into(),
        lease: lease.clone(),
        step: id,
        attempt: 0,
        value: value.into(),
        kind,
        name: name.map(str::to_owned),
    }
}

#[test]
fn accepted_step_is_the_only_source_of_warm_continuation() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, replay) = grant(&mut authority, 11);
    assert!(replay.is_empty());
    let (ack, intents) = commit(&mut authority, step(&lease, 0, "first"));
    assert_eq!(ack.position, 3);
    let [
        Intent::Continue {
            step: committed, ..
        },
    ] = intents.as_slice()
    else {
        panic!("accepted step must resume its warm invocation")
    };
    assert_eq!(committed.value, "first");
    assert_eq!(authority.jobs()["job"].steps.len(), 1);

    let duplicate = authority.prepare(step(&lease, 0, "first")).unwrap();
    assert_eq!(
        duplicate,
        Decision::Duplicate(Ack {
            position: 3,
            disposition: Disposition::Park,
        })
    );
    assert_eq!(
        authority.prepare(step(&lease, 0, "different")),
        Err(Rejection::PayloadConflict)
    );
    assert_eq!(
        authority.prepare(step(&lease, 2, "future")),
        Err(Rejection::OutOfOrder)
    );
    assert_eq!(authority.revision(), 3);
}

#[test]
fn lost_step_reply_recovers_without_reexecution_or_stale_continuation() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(&mut authority, step(&lease, 0, "persisted"));
    let recovered = Authority::recover(7, 2, None, authority.suffix()).unwrap();
    assert!(recovered.jobs()["job"].lease.is_none());
    assert_eq!(recovered.jobs()["job"].steps.len(), 1);

    let mut recovered = recovered;
    let (fresh, replay) = grant(&mut recovered, 12);
    assert_eq!(fresh.generation, 2);
    assert_eq!(replay.len(), 1);
    assert_eq!(
        recovered.prepare(step(&lease, 0, "persisted")),
        Ok(Decision::Duplicate(Ack {
            position: 3,
            disposition: Disposition::Park,
        }))
    );
    assert_eq!(
        recovered.prepare(step(&lease, 0, "different")),
        Err(Rejection::StaleLease)
    );
    assert_eq!(
        recovered.prepare(step(&fresh, 0, "persisted")),
        Err(Rejection::PayloadConflict)
    );
    assert_eq!(
        recovered.prepare(begin_wait(&fresh, 0, TIMER)),
        Err(Rejection::Nondeterministic)
    );
    assert_eq!(
        recovered.prepare(begin_wait(&fresh, 0, WaitKind::Message)),
        Err(Rejection::Nondeterministic)
    );
    assert_eq!(
        recovered.prepare(step(&lease, 1, "stale")),
        Err(Rejection::StaleLease)
    );
    assert_eq!(recovered.revision(), 4);
}

#[test]
fn revocation_preserves_monotonic_generations() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (first, _) = grant(&mut authority, 11);
    commit(
        &mut authority,
        Command::Revoke {
            key: "job".into(),
            lease: first.clone(),
        },
    );
    let (second, _) = grant(&mut authority, 12);
    assert_eq!(second.generation, 2);
    assert_eq!(
        authority.prepare(step(&first, 0, "stale")),
        Err(Rejection::StaleLease)
    );
}

#[test]
fn snapshot_suffix_and_full_replay_agree() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(&mut authority, step(&lease, 0, "persisted"));
    let snapshot = authority.snapshot();
    commit(
        &mut authority,
        Command::Complete {
            key: "job".into(),
            lease,
            expected_steps: 1,
            value: "done".into(),
            now_ms: 0,
        },
    );
    let full_log = authority.suffix().to_vec();
    let completion = full_log.last().unwrap().clone();
    let from_snapshot = Authority::recover(7, 2, Some(snapshot), &[completion]).unwrap();
    let from_log = Authority::recover(7, 2, None, &full_log).unwrap();
    assert_eq!(from_snapshot.revision(), from_log.revision());
    assert_eq!(from_snapshot.next_job_incarnation(), 1);
    assert!(from_snapshot.jobs().is_empty());
    assert_eq!(from_snapshot.jobs(), from_log.jobs());
    assert_eq!(from_snapshot.receipts(), from_log.receipts());
    assert_eq!(
        from_snapshot.receipts()["job"],
        Receipt {
            args: "args".into(),
            incarnation: 1,
            submission_position: 1,
            policy: RetryPolicy::Immediate,
            budget: RetryBudget::default(),
            retention_ms: DEFAULT_RETENTION_MS,
            steps: 1,
            outcome: Outcome::Completed(Terminal {
                value: "done".into(),
                lease: Lease {
                    partition_incarnation: 7,
                    authority_epoch: 1,
                    job_incarnation: 1,
                    generation: 1,
                    worker: 11,
                    position: 2,
                },
                expires_at_ms: DEFAULT_RETENTION_MS,
                position: 4,
            }),
        }
    );
}

#[test]
fn duplicate_terminal_receipt_is_stable() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    let command = Command::Complete {
        key: "job".into(),
        lease: lease.clone(),
        expected_steps: 0,
        value: "done".into(),
        now_ms: 100,
    };
    let (ack, intents) = commit(&mut authority, command.clone());
    assert_eq!(ack.disposition, Disposition::Completed);
    assert!(matches!(intents.as_slice(), [Intent::Terminal { .. }]));
    assert!(authority.jobs().is_empty(), "settled job moves to receipts");
    assert_eq!(authority.prepare(command), Ok(Decision::Duplicate(ack)));
    assert_eq!(
        authority.prepare(step(&lease, 0, "late")),
        Err(Rejection::StaleLease)
    );
    assert_eq!(
        authority.prepare(Command::Expire {
            key: "job".into(),
            incarnation: 1,
            now_ms: 100 + DEFAULT_RETENTION_MS - 1,
        }),
        Err(Rejection::RetentionPending {
            expires_at_ms: 100 + DEFAULT_RETENTION_MS,
        })
    );
    let (expired, intents) = commit(
        &mut authority,
        Command::Expire {
            key: "job".into(),
            incarnation: 1,
            now_ms: 100 + DEFAULT_RETENTION_MS,
        },
    );
    assert_eq!(
        expired,
        Ack {
            position: 4,
            disposition: Disposition::Completed,
        }
    );
    assert!(intents.is_empty());
    assert!(authority.receipts().is_empty());
    submit(&mut authority);
    assert_eq!(authority.jobs()["job"].incarnation, 2);
    assert_eq!(authority.revision(), 5);
}

fn begin_wait(lease: &Lease, step: u64, kind: WaitKind) -> Command {
    begin_wait_as(lease, step, kind, None)
}

fn begin_wait_as(lease: &Lease, step: u64, kind: WaitKind, name: Option<&str>) -> Command {
    Command::BeginWait {
        key: "job".into(),
        lease: lease.clone(),
        step,
        kind,
        name: name.map(str::to_owned),
    }
}

fn fire(step: u64) -> Command {
    Command::FireTimer {
        key: "job".into(),
        incarnation: 1,
        step,
        value: "null".into(),
    }
}

fn send(id: &str, payload: &str) -> Command {
    Command::SendMessage {
        key: "job".into(),
        id: id.into(),
        payload: payload.into(),
    }
}

const TIMER: WaitKind = WaitKind::Timer { fire_at_ms: 50 };

#[test]
fn timer_wait_survives_recovery_and_resolves_once() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    let (ack, intents) = commit(&mut authority, begin_wait(&lease, 0, TIMER));
    assert_eq!(ack.disposition, Disposition::Park);
    let [Intent::Arm { wait, .. }] = intents.as_slice() else {
        panic!("a new wait must be armed")
    };
    assert_eq!((wait.step, wait.kind, wait.position), (0, TIMER, 3));
    assert_eq!(
        authority.prepare(submit_command()),
        Ok(Decision::Duplicate(Ack {
            position: 1,
            disposition: Disposition::Park,
        }))
    );
    assert_eq!(
        authority.prepare(step(&lease, 0, "racing")),
        Err(Rejection::OutOfOrder)
    );
    // Re-beginning the held timer wait at the same step under a conflicting
    // kind is a divergent replay of the wait, not merely out of order.
    assert_eq!(
        authority.prepare(begin_wait(&lease, 0, WaitKind::Message)),
        Err(Rejection::Nondeterministic)
    );

    let mut recovered = Authority::recover(7, 2, None, authority.suffix()).unwrap();
    assert_eq!(recovered.jobs()["job"].wait.as_ref(), Some(wait));
    let (fresh, replay) = grant(&mut recovered, 12);
    assert!(replay.is_empty());
    // The re-granted invocation re-attaches with its own, later deadline;
    // the durable one stands.
    let later = WaitKind::Timer { fire_at_ms: 90 };
    assert_eq!(
        recovered.prepare(begin_wait(&fresh, 0, later)),
        Ok(Decision::Duplicate(Ack {
            position: 3,
            disposition: Disposition::Park,
        }))
    );
    assert_eq!(
        recovered.prepare(begin_wait(&lease, 0, later)),
        Err(Rejection::StaleLease)
    );

    let (ack, intents) = commit(&mut recovered, fire(0));
    assert_eq!(
        ack,
        Ack {
            position: 5,
            disposition: Disposition::Continue
        }
    );
    let [Intent::Continue { step: resolved, .. }] = intents.as_slice() else {
        panic!("the parked invocation must continue")
    };
    assert_eq!((resolved.id, resolved.value.as_str()), (0, "null"));
    assert_eq!(
        resolved.lease, lease,
        "the step records the lease that began the wait"
    );
    assert!(recovered.jobs()["job"].wait.is_none());
    assert_eq!(recovered.prepare(fire(0)), Ok(Decision::Duplicate(ack)));
    assert_eq!(
        recovered.prepare(Command::FireTimer {
            key: "job".into(),
            incarnation: 1,
            step: 0,
            value: "other".into(),
        }),
        Err(Rejection::PayloadConflict)
    );
    assert_eq!(recovered.prepare(fire(1)), Err(Rejection::NotWaiting));
    assert_eq!(
        recovered.prepare(begin_wait(&fresh, 0, later)),
        Ok(Decision::Duplicate(Ack {
            position: 5,
            disposition: Disposition::Continue,
        }))
    );
    assert_eq!(
        recovered.prepare(begin_wait(&fresh, 0, WaitKind::Message)),
        Err(Rejection::Nondeterministic)
    );
    assert_eq!(
        recovered.prepare(step(&fresh, 0, "null")),
        Err(Rejection::Nondeterministic)
    );
    commit(&mut recovered, step(&fresh, 1, "after"));
}

#[test]
fn unleased_timer_resolution_reaches_the_next_grant_through_replay() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(&mut authority, begin_wait(&lease, 0, TIMER));
    commit(
        &mut authority,
        Command::Revoke {
            key: "job".into(),
            lease,
        },
    );
    let (ack, intents) = commit(&mut authority, fire(0));
    assert_eq!(ack.disposition, Disposition::Continue);
    assert!(intents.is_empty(), "no invocation is parked on the wait");
    let (_, replay) = grant(&mut authority, 12);
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].value, "null");
}

#[test]
fn messages_queue_deduplicate_and_resolve_waits() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    let (queued, intents) = commit(&mut authority, send("m1", "first"));
    assert_eq!(
        queued,
        Ack {
            position: 3,
            disposition: Disposition::Admitted
        }
    );
    assert!(intents.is_empty());
    assert_eq!(
        authority.prepare(send("m1", "first")),
        Ok(Decision::Duplicate(queued))
    );
    assert_eq!(
        authority.prepare(send("m1", "other")),
        Err(Rejection::PayloadConflict)
    );

    // A queued message resolves the wait in the same commit that begins it.
    let (ack, intents) = commit(&mut authority, begin_wait(&lease, 0, WaitKind::Message));
    assert_eq!(
        ack,
        Ack {
            position: 4,
            disposition: Disposition::Continue
        }
    );
    let [Intent::Continue { step: resolved, .. }] = intents.as_slice() else {
        panic!("a queued message must continue the invocation")
    };
    assert_eq!(resolved.value, "first");
    assert!(authority.jobs()["job"].wait.is_none());
    assert_eq!(authority.prepare(fire(0)), Err(Rejection::NotWaiting));

    let (ack, intents) = commit(&mut authority, begin_wait(&lease, 1, WaitKind::Message));
    assert_eq!(ack.disposition, Disposition::Park);
    assert!(matches!(intents.as_slice(), [Intent::Arm { .. }]));
    assert_eq!(authority.prepare(fire(1)), Err(Rejection::NotWaiting));
    let (delivered, intents) = commit(&mut authority, send("m2", "second"));
    assert_eq!(
        delivered,
        Ack {
            position: 6,
            disposition: Disposition::Admitted
        }
    );
    let [Intent::Continue { step: resolved, .. }] = intents.as_slice() else {
        panic!("a delivered message must continue the waiting invocation")
    };
    assert_eq!((resolved.id, resolved.value.as_str()), (1, "second"));
    assert_eq!(
        authority.prepare(send("m2", "second")),
        Ok(Decision::Duplicate(delivered))
    );
    assert_eq!(
        authority.prepare(begin_wait(&lease, 1, TIMER)),
        Err(Rejection::Nondeterministic)
    );
    assert_eq!(
        authority.prepare(step(&lease, 1, "second")),
        Err(Rejection::Nondeterministic)
    );
    let job = &authority.jobs()["job"];
    assert!(job.inbox.is_empty());
    assert_eq!(job.messages.len(), 2);

    commit(
        &mut authority,
        Command::Complete {
            key: "job".into(),
            lease,
            expected_steps: 2,
            value: "done".into(),
            now_ms: 0,
        },
    );
    assert_eq!(
        authority.prepare(send("m3", "late")),
        Err(Rejection::AlreadyTerminal)
    );
    let from_log = Authority::recover(7, 2, None, authority.suffix()).unwrap();
    assert_eq!(from_log.jobs(), authority.jobs());
    assert_eq!(from_log.receipts(), authority.receipts());
}

#[test]
fn waiting_job_cannot_complete() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(&mut authority, begin_wait(&lease, 0, WaitKind::Message));
    assert_eq!(
        authority.prepare(Command::Complete {
            key: "job".into(),
            lease,
            expected_steps: 0,
            value: "done".into(),
            now_ms: 0,
        }),
        Err(Rejection::OutOfOrder)
    );
}

#[derive(Debug, Clone)]
enum Operation {
    Submit,
    Grant(u64),
    Step(String),
    Complete,
    Revoke,
    Recover(bool),
    Wait(bool),
    Fire,
    Send(u8),
    /// A transient failure under the current lease, with host-supplied jitter.
    Retry(u8),
    /// A cleanup step under the current lease; `WrongPhase` outside cleanup.
    Defer(String),
    /// An operator cancel; needs no lease, so it also hits parked jobs.
    Cancel,
    /// A terminal failure reported by the current lease holder.
    Fail,
    /// Expires a settled receipt once its retention deadline is due.
    Expire,
}

fn operation() -> impl proptest::strategy::Strategy<Value = Operation> {
    use proptest::prelude::*;
    prop_oneof![
        1 => Just(Operation::Submit),
        3 => any::<u64>().prop_map(Operation::Grant),
        4 => "[a-z]{0,8}".prop_map(Operation::Step),
        1 => Just(Operation::Complete),
        1 => Just(Operation::Revoke),
        2 => any::<bool>().prop_map(Operation::Recover),
        2 => any::<bool>().prop_map(Operation::Wait),
        1 => Just(Operation::Fire),
        2 => (0..3_u8).prop_map(Operation::Send),
        3 => (0..4_u8).prop_map(Operation::Retry),
        2 => "[a-z]{0,8}".prop_map(Operation::Defer),
        1 => Just(Operation::Cancel),
        1 => Just(Operation::Fail),
        1 => Just(Operation::Expire),
    ]
}

fn assert_recovery_equivalent(authority: &Authority, durable: &[Transition], epoch: u64) {
    let from_log = Authority::recover(7, epoch, None, durable).unwrap();
    let from_snapshot = Authority::recover(7, epoch, Some(authority.snapshot()), &[]).unwrap();
    assert_eq!(from_log.revision(), from_snapshot.revision());
    assert_eq!(
        from_log.next_job_incarnation(),
        from_snapshot.next_job_incarnation()
    );
    assert_eq!(from_log.jobs(), from_snapshot.jobs());
    assert_eq!(from_log.receipts(), from_snapshot.receipts());
    let encoded = serde_json::to_vec(&authority.snapshot()).unwrap();
    let decoded: Snapshot = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, authority.snapshot());
}

/// The command `operation` issues against the job's current state, if the
/// job is in a state where a well-behaved caller could issue it. `now_ms` is
/// the host clock the operation is issued at.
fn command_for(authority: &Authority, operation: Operation, now_ms: u64) -> Option<Command> {
    let job = authority.jobs().get("job");
    let lease = job.and_then(|job| job.lease.clone());
    let key = String::from("job");
    match operation {
        // Bounded budgets, so the sampled runs exhaust them: forward retries
        // into cleanup, cleanup retries into a rejection, restarts into an
        // authority-side fault.
        Operation::Submit => Some(Command::Submit {
            key,
            args: "args".into(),
            policy: RetryPolicy::Immediate,
            budget: RetryBudget {
                max_retries: Some(1),
                max_restarts: Some(2),
            },
            retention_ms: 8,
        }),
        Operation::Grant(worker) => job.map(|job| Command::Grant {
            key,
            incarnation: job.incarnation,
            worker,
            now_ms,
        }),
        Operation::Step(value) => lease.map(|lease| Command::CommitStep {
            key,
            lease,
            step: job.unwrap().steps.len() as u64,
            attempt: 0,
            value,
            kind: StepKind::Effect,
            name: None,
        }),
        Operation::Defer(value) => lease.map(|lease| Command::CommitStep {
            key,
            lease,
            step: job.unwrap().steps.len() as u64,
            attempt: 0,
            value,
            kind: StepKind::Defer,
            name: None,
        }),
        Operation::Complete => lease.map(|lease| Command::Complete {
            key,
            lease,
            expected_steps: job.unwrap().steps.len() as u64,
            value: "done".into(),
            now_ms,
        }),
        Operation::Revoke => lease.map(|lease| Command::Revoke { key, lease }),
        Operation::Wait(timer) => lease.map(|lease| Command::BeginWait {
            key,
            lease,
            step: job.unwrap().steps.len() as u64,
            kind: if timer {
                WaitKind::Timer { fire_at_ms: 5 }
            } else {
                WaitKind::Message
            },
            name: None,
        }),
        Operation::Fire => job.and_then(|job| {
            job.wait.as_ref().map(|wait| Command::FireTimer {
                key,
                incarnation: job.incarnation,
                step: wait.step,
                value: "null".into(),
            })
        }),
        Operation::Send(id) => Some(Command::SendMessage {
            key,
            id: id.to_string(),
            payload: format!("payload-{id}"),
        }),
        Operation::Retry(jitter) => lease.map(|lease| Command::Retry {
            key,
            lease,
            error: "transient".into(),
            policy: None,
            now_ms,
            jitter_ms: u64::from(jitter) * 3,
        }),
        Operation::Cancel => job.map(|job| Command::Cancel {
            key,
            incarnation: job.incarnation,
            reason: "operator".into(),
        }),
        Operation::Fail => lease.map(|lease| Command::Fail {
            key,
            lease,
            error: "terminal".into(),
        }),
        Operation::Expire => authority
            .receipts()
            .get("job")
            .map(|receipt| Command::Expire {
                key,
                incarnation: receipt.incarnation,
                now_ms,
            }),
        Operation::Recover(_) => unreachable!("recovery replaces the authority"),
    }
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

    #[test]
    fn randomized_recovery_preserves_the_committed_projection(
        operations in proptest::collection::vec(operation(), 0..24)
    ) {
        let mut authority = Authority::new(7, 1);
        let mut durable = Vec::new();
        for (index, operation) in operations.into_iter().enumerate() {
            // A slowly advancing host clock, so a scheduled retry is sometimes
            // due at the next grant and sometimes still pending.
            let now_ms = index as u64 * 4;
            if let Operation::Recover(from_snapshot) = operation {
                let epoch = authority.authority_epoch() + 1;
                authority = if from_snapshot {
                    Authority::recover(7, epoch, Some(authority.snapshot()), &[]).unwrap()
                } else {
                    Authority::recover(7, epoch, None, &durable).unwrap()
                };
            } else if let Some(command) = command_for(&authority, operation, now_ms)
                && let Ok(Decision::Commit(proposal)) = authority.prepare(command)
            {
                durable.push(proposal.transition.clone());
                authority.publish(*proposal);
            }
            assert_recovery_equivalent(&authority, &durable, authority.authority_epoch() + 1);
        }
    }
}
