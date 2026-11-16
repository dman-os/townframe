use super::*;
use wflow_wdk::StepKind;
use wflow_wdk::types::RetryPolicy;

mod cancellation;

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
            payload: PayloadFingerprint::of("args"),
            incarnation: 1,
            submission_position: 1,
            policy: RetryPolicy::Immediate,
            budget: RetryBudget::default(),
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
                position: 4,
            }),
        }
    );
}

#[test]
fn terminal_receipt_is_permanent_and_submission_stays_idempotent() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    let command = Command::Complete {
        key: "job".into(),
        lease: lease.clone(),
        expected_steps: 0,
        value: "done".into(),
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
        authority.prepare(submit_command()),
        Ok(Decision::Duplicate(Ack {
            position: 1,
            disposition: Disposition::Completed,
        }))
    );
    let mut conflicting = submit_command();
    let Command::Submit { args, .. } = &mut conflicting else {
        unreachable!();
    };
    *args = "different".into();
    assert_eq!(
        authority.prepare(conflicting),
        Err(Rejection::PayloadConflict)
    );
    // The receipt's payload fingerprint covers the arguments; the policy and
    // budget it keeps as values remain part of the same identity check.
    let mut repriced = submit_command();
    let Command::Submit { policy, .. } = &mut repriced else {
        unreachable!();
    };
    *policy = RetryPolicy::Fixed { delay_ms: 3 };
    assert_eq!(authority.prepare(repriced), Err(Rejection::PayloadConflict));
    let mut rebudgeted = submit_command();
    let Command::Submit { budget, .. } = &mut rebudgeted else {
        unreachable!();
    };
    *budget = RetryBudget {
        max_retries: Some(1),
        max_restarts: None,
    };
    assert_eq!(
        authority.prepare(rebudgeted),
        Err(Rejection::PayloadConflict)
    );
    assert_eq!(authority.next_job_incarnation(), 1);
    assert_eq!(authority.revision(), 3);
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
        }),
        Err(Rejection::OutOfOrder)
    );
}

fn fail(lease: &Lease, failure: JobFailure) -> Command {
    Command::Fail {
        key: "job".into(),
        lease: lease.clone(),
        failure,
    }
}

/// A job parked on a durable wait is re-granted on every restart of its
/// authority without ever starting a fresh run, so no re-grant may charge the
/// restart budget: otherwise a job that never failed exhausts it and settles
/// `RestartsExhausted`.
#[test]
fn parked_job_regrants_do_not_charge_the_restart_budget() {
    for kind in [WaitKind::Message, TIMER] {
        let mut authority = Authority::new(7, 1);
        commit(
            &mut authority,
            Command::Submit {
                key: "job".into(),
                args: "args".into(),
                policy: RetryPolicy::Immediate,
                // One restart is allowed; the old rule exhausted it on the
                // third boot of this parked job.
                budget: RetryBudget {
                    max_retries: None,
                    max_restarts: Some(1),
                },
            },
        );
        let (lease, _) = grant(&mut authority, 11);
        let (ack, _) = commit(&mut authority, begin_wait(&lease, 0, kind));
        assert_eq!(ack.disposition, Disposition::Park);
        let mut durable = authority.suffix().to_vec();

        for boot in 0..4 {
            let mut authority = Authority::recover(7, boot + 2, None, &durable).unwrap();
            assert_eq!(
                authority.jobs()["job"].restarts,
                0,
                "{kind:?} charged a restart while parked at boot {boot}"
            );
            let (lease, replay) = grant(&mut authority, 20 + boot);
            assert!(replay.is_empty());
            // The re-granted invocation replays up to the held wait and
            // re-attaches to it.
            assert_eq!(
                authority.prepare(begin_wait(&lease, 0, kind)),
                Ok(Decision::Duplicate(Ack {
                    position: 3,
                    disposition: Disposition::Park,
                }))
            );
            assert_eq!(authority.jobs()["job"].restarts, 0);
            assert!(
                authority.receipts().is_empty(),
                "{kind:?} settled at boot {boot}"
            );
            durable = authority.suffix().to_vec();
        }
    }
}

/// Settles a freshly granted job with `failure` and proves the fault survives
/// full-log replay and snapshot restore unchanged.
fn assert_fault_survives_recovery(budget: RetryBudget, failure: JobFailure) {
    let mut authority = Authority::new(7, 1);
    commit(
        &mut authority,
        Command::Submit {
            key: "job".into(),
            args: "args".into(),
            policy: RetryPolicy::Immediate,
            budget,
        },
    );
    let (lease, _) = grant(&mut authority, 11);
    let (ack, intents) = commit(&mut authority, fail(&lease, failure.clone()));
    assert_eq!(ack.disposition, Disposition::Completed);
    let [Intent::Fault { fault, .. }] = intents.as_slice() else {
        panic!("a fault must settle its invocation")
    };
    assert_eq!(fault.failure, failure);

    let from_log = Authority::recover(7, 2, None, authority.suffix()).unwrap();
    let from_snapshot = Authority::recover(7, 2, Some(authority.snapshot()), &[]).unwrap();
    for recovered in [&from_log, &from_snapshot] {
        assert!(recovered.jobs().is_empty());
        assert_eq!(recovered.receipts(), authority.receipts());
    }
    // A resent failure of the same activation is the same settlement; any
    // other failure would overlay it.
    assert_eq!(
        from_log.prepare(fail(&lease, failure.clone())),
        Ok(Decision::Duplicate(Ack {
            position: fault.position,
            disposition: Disposition::Completed,
        }))
    );
    assert_eq!(
        from_log.prepare(fail(&lease, JobFailure::Panicked("other".into()))),
        Err(Rejection::PayloadConflict)
    );
}

#[test]
fn every_lease_holder_fault_survives_replay_and_snapshot_restore() {
    for (budget, failure) in [
        (
            RetryBudget {
                max_retries: Some(0),
                max_restarts: None,
            },
            JobFailure::RetriesExhausted {
                retries: 0,
                max_retries: 0,
                last_error: "transient".into(),
            },
        ),
        (RetryBudget::default(), JobFailure::Panicked("boom".into())),
        (
            RetryBudget::default(),
            JobFailure::ReplayDiverged {
                step: 0,
                replayed: StepKind::Recv,
            },
        ),
    ] {
        assert_fault_survives_recovery(budget, failure);
    }
}

#[test]
fn every_authority_fault_survives_replay_and_snapshot_restore() {
    // Cancellation settles without a lease.
    let mut cancelled = Authority::new(7, 1);
    submit(&mut cancelled);
    let (lease, _) = grant(&mut cancelled, 11);
    commit(&mut cancelled, begin_wait(&lease, 0, WaitKind::Message));
    commit(
        &mut cancelled,
        Command::Cancel {
            key: "job".into(),
            incarnation: 1,
            reason: "operator".into(),
        },
    );

    // Restart exhaustion is charged by the grant that would have re-run the
    // job without progress.
    let mut exhausted = Authority::new(7, 1);
    commit(
        &mut exhausted,
        Command::Submit {
            key: "job".into(),
            args: "args".into(),
            policy: RetryPolicy::Immediate,
            budget: RetryBudget {
                max_retries: None,
                max_restarts: Some(0),
            },
        },
    );
    let (lease, _) = grant(&mut exhausted, 11);
    commit(
        &mut exhausted,
        Command::Revoke {
            key: "job".into(),
            lease,
        },
    );
    let (ack, intents) = commit(
        &mut exhausted,
        Command::Grant {
            key: "job".into(),
            incarnation: 1,
            worker: 12,
            now_ms: 0,
        },
    );
    assert_eq!(ack.disposition, Disposition::Completed);
    assert!(matches!(intents.as_slice(), [Intent::Fault { .. }]));

    for authority in [&cancelled, &exhausted] {
        let durable = authority.suffix().to_vec();
        let from_log = Authority::recover(7, 2, None, &durable).unwrap();
        let from_snapshot = Authority::recover(7, 2, Some(authority.snapshot()), &[]).unwrap();
        for recovered in [&from_log, &from_snapshot] {
            assert!(recovered.jobs().is_empty());
            assert_eq!(recovered.receipts(), authority.receipts());
            let Outcome::Faulted(fault) = &recovered.receipts()["job"].outcome else {
                panic!("these jobs settle as faults")
            };
            assert!(matches!(
                fault.failure,
                JobFailure::Cancelled { .. } | JobFailure::RestartsExhausted { .. }
            ));
        }
    }
}

#[test]
fn a_superseded_activation_cannot_fault_a_live_job() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (stale, _) = grant(&mut authority, 11);
    commit(
        &mut authority,
        Command::Revoke {
            key: "job".into(),
            lease: stale.clone(),
        },
    );
    let (current, _) = grant(&mut authority, 12);

    for failure in [
        JobFailure::Panicked("late".into()),
        JobFailure::ReplayDiverged {
            step: 0,
            replayed: StepKind::Recv,
        },
        JobFailure::RetriesExhausted {
            retries: 0,
            max_retries: 0,
            last_error: "late".into(),
        },
    ] {
        assert_eq!(
            authority.prepare(fail(&stale, failure)),
            Err(Rejection::StaleLease)
        );
    }
    assert!(authority.receipts().is_empty());

    commit(
        &mut authority,
        fail(&current, JobFailure::Panicked("current".into())),
    );
    // The live activation settled the job; the superseded one can neither
    // resettle it with another failure nor claim the settlement as its own.
    assert_eq!(
        authority.prepare(fail(&stale, JobFailure::Panicked("other".into()))),
        Err(Rejection::PayloadConflict)
    );
}

#[test]
fn a_fault_the_durable_log_cannot_recompute_is_rejected() {
    let mut authority = Authority::new(7, 1);
    commit(
        &mut authority,
        Command::Submit {
            key: "job".into(),
            args: "args".into(),
            policy: RetryPolicy::Immediate,
            budget: RetryBudget {
                max_retries: Some(1),
                max_restarts: Some(1),
            },
        },
    );
    let (lease, _) = grant(&mut authority, 11);

    for failure in [
        // The retry budget has not been spent, so it cannot be exhausted.
        JobFailure::RetriesExhausted {
            retries: 1,
            max_retries: 1,
            last_error: "invented".into(),
        },
        // The restart budget is charged at grant time, never by an activation.
        JobFailure::RestartsExhausted {
            restarts: 2,
            max_restarts: 1,
        },
        // Cancellation is an operator fact and names no lease.
        JobFailure::Cancelled {
            reason: "invented".into(),
        },
    ] {
        assert_eq!(
            authority.prepare(fail(&lease, failure)),
            Err(Rejection::InvalidFault)
        );
    }
    assert!(authority.receipts().is_empty());
}

#[test]
fn the_durable_panic_message_is_bounded() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    let (ack, _) = commit(
        &mut authority,
        fail(
            &lease,
            JobFailure::Panicked("x".repeat(MAX_FAULT_TEXT_BYTES * 2)),
        ),
    );
    assert_eq!(ack.disposition, Disposition::Completed);
    let Outcome::Faulted(fault) = &authority.receipts()["job"].outcome else {
        panic!("the panic settles as a fault")
    };
    let JobFailure::Panicked(message) = &fault.failure else {
        panic!("the fault keeps its panic")
    };
    assert_eq!(message.len(), MAX_FAULT_TEXT_BYTES);

    let recovered = Authority::recover(7, 2, None, authority.suffix()).unwrap();
    assert_eq!(recovered.receipts(), authority.receipts());
    assert!(matches!(
        recovered.prepare(fail(
            &lease,
            JobFailure::Panicked("x".repeat(MAX_FAULT_TEXT_BYTES * 3))
        )),
        Ok(Decision::Duplicate(_))
    ));
}

/// The fingerprint is written into permanent receipts, so the construction is
/// part of the durable format: this vector must not change without a receipt
/// and snapshot format bump.
#[test]
fn the_payload_fingerprint_is_frozen() {
    assert_eq!(
        PayloadFingerprint::of("args"),
        PayloadFingerprint([0x4977_0ec0_4256_f321, 0xbfc0_ee11_1b8d_85db])
    );
    assert_ne!(
        PayloadFingerprint::of("args"),
        PayloadFingerprint::of("argt")
    );
}

#[test]
fn a_snapshot_without_a_format_version_is_rejected() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let mut snapshot = authority.snapshot();
    snapshot.format = 0;
    let error = Authority::recover(7, 2, Some(snapshot), &[]).unwrap_err();
    assert!(
        error.to_string().contains("snapshot format"),
        "unexpected error: {error}"
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
    /// A classified failure the current lease holder reports.
    Fail(u8),
    /// An operator cancel; needs no lease, so it also hits parked jobs.
    Cancel,
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
        1 => (0..3_u8).prop_map(Operation::Fail),
        1 => Just(Operation::Cancel),
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
        // Bounded budgets exercise retry and restart exhaustion.
        Operation::Submit => Some(Command::Submit {
            key,
            args: "args".into(),
            policy: RetryPolicy::Immediate,
            budget: RetryBudget {
                max_retries: Some(1),
                max_restarts: Some(2),
            },
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
        Operation::Complete => lease.map(|lease| Command::Complete {
            key,
            lease,
            expected_steps: job.unwrap().steps.len() as u64,
            value: "done".into(),
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
        Operation::Fail(which) => lease.map(|lease| Command::Fail {
            key,
            lease,
            failure: match which {
                0 => JobFailure::Panicked("boom".into()),
                1 => JobFailure::ReplayDiverged {
                    step: job.unwrap().steps.len() as u64,
                    replayed: StepKind::Sleep,
                },
                _ => JobFailure::RetriesExhausted {
                    retries: job.unwrap().retry_attempts,
                    max_retries: 1,
                    last_error: "transient".into(),
                },
            },
        }),
        Operation::Cancel => job.map(|job| Command::Cancel {
            key,
            incarnation: job.incarnation,
            reason: "operator".into(),
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
