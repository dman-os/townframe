use super::*;

fn cancel(reason: &str) -> Command {
    Command::Cancel {
        key: "job".into(),
        incarnation: 1,
        reason: reason.into(),
    }
}

fn fail(lease: &Lease, error: &str) -> Command {
    Command::Fail {
        key: "job".into(),
        lease: lease.clone(),
        error: error.into(),
    }
}

fn revoke(lease: &Lease) -> Command {
    Command::Revoke {
        key: "job".into(),
        lease: lease.clone(),
    }
}

fn complete(lease: &Lease, expected_steps: u64) -> Command {
    Command::Complete {
        key: "job".into(),
        lease: lease.clone(),
        expected_steps,
        value: "done".into(),
        now_ms: 0,
    }
}

fn defer(lease: &Lease, id: u64, value: &str) -> Command {
    step_as(lease, id, value, StepKind::Defer, None)
}

fn grant_at(worker: u64, now_ms: u64) -> Command {
    Command::Grant {
        key: "job".into(),
        incarnation: 1,
        worker,
        now_ms,
    }
}

fn cancelled(reason: &str, steps: u64, lease: Option<&Lease>, position: u64) -> Cleanup {
    Cleanup {
        cause: CleanupCause::Cancelled {
            reason: reason.into(),
        },
        steps,
        lease: lease.cloned(),
        position,
    }
}

/// Commits `command`, which must move the job into exactly `expected`, and
/// checks the one intent and the projected job agree with it.
fn enter_cleanup(
    authority: &mut Authority,
    command: Command,
    expected: &Cleanup,
    disposition: Disposition,
) {
    let (ack, intents) = commit(authority, command);
    assert_eq!(
        ack,
        Ack {
            position: expected.position,
            disposition,
        }
    );
    assert_eq!(
        intents,
        vec![Intent::Cleanup {
            key: "job".into(),
            cleanup: expected.clone(),
        }]
    );
    let job = &authority.jobs()["job"];
    assert_eq!(job.cleanup.as_ref(), Some(expected));
    assert!(job.wait.is_none(), "a pending wait is abandoned");
    let held = match expected.cause {
        CleanupCause::RetriesExhausted { .. } => None,
        CleanupCause::Cancelled { .. } | CleanupCause::Failed { .. } => expected.lease.clone(),
    };
    assert_eq!(
        job.lease, held,
        "entering cleanup keeps the lease unless an exhausted retry released it"
    );
    assert_eq!(
        job.retry_attempts, 0,
        "entering cleanup starts the retry budget over"
    );
    assert_eq!(job.steps.len() as u64, expected.steps);
}

#[test]
fn cancel_while_waiting_abandons_the_wait_and_interrupts_the_lease() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(&mut authority, begin_wait(&lease, 0, TIMER));
    let expected = cancelled("operator", 0, Some(&lease), 4);
    enter_cleanup(
        &mut authority,
        cancel("operator"),
        &expected,
        Disposition::Admitted,
    );
    assert_recovery_equivalent(&authority, authority.suffix(), 2);

    assert_eq!(authority.prepare(fire(0)), Err(Rejection::NotWaiting));
    assert_eq!(
        authority.prepare(begin_wait(&lease, 0, TIMER)),
        Err(Rejection::WrongPhase)
    );
    assert_eq!(
        authority.prepare(begin_wait(&lease, 0, WaitKind::Message)),
        Err(Rejection::WrongPhase)
    );
    let (ack, intents) = commit(&mut authority, defer(&lease, 0, "release"));
    assert_eq!(
        ack,
        Ack {
            position: 5,
            disposition: Disposition::Continue,
        }
    );
    let [
        Intent::Continue {
            step: committed, ..
        },
    ] = intents.as_slice()
    else {
        panic!("a cleanup step resumes the interrupted invocation")
    };
    assert_eq!((committed.id, committed.kind), (0, StepKind::Defer));
    assert_eq!(
        authority.prepare(step(&lease, 1, "forward")),
        Err(Rejection::WrongPhase)
    );
    assert_eq!(
        authority.prepare(begin_wait(&lease, 1, TIMER)),
        Err(Rejection::WrongPhase)
    );
    let (ack, intents) = commit(&mut authority, complete(&lease, 1));
    assert_eq!(ack.disposition, Disposition::Completed);
    assert!(matches!(intents.as_slice(), [Intent::Terminal { .. }]));
    assert!(authority.jobs().is_empty());
    assert_eq!(authority.receipts()["job"].steps, 1);
    assert_recovery_equivalent(&authority, authority.suffix(), 2);
}

#[test]
fn in_flight_effect_commits_once_after_cancel() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(&mut authority, step(&lease, 0, "first"));
    let expected = cancelled("operator", 1, Some(&lease), 4);
    enter_cleanup(
        &mut authority,
        cancel("operator"),
        &expected,
        Disposition::Admitted,
    );

    let (ack, intents) = commit(&mut authority, step(&lease, 1, "in-flight"));
    assert_eq!(
        ack,
        Ack {
            position: 5,
            disposition: Disposition::Continue,
        }
    );
    assert!(matches!(intents.as_slice(), [Intent::Continue { .. }]));
    assert_eq!(
        authority.prepare(step(&lease, 1, "in-flight")),
        Ok(Decision::Duplicate(Ack {
            position: 5,
            disposition: Disposition::Park,
        }))
    );
    assert_eq!(
        authority.prepare(step(&lease, 2, "second")),
        Err(Rejection::WrongPhase)
    );
    assert_recovery_equivalent(&authority, authority.suffix(), 2);

    commit(&mut authority, defer(&lease, 2, "release"));
    assert_eq!(
        authority.prepare(step(&lease, 3, "third")),
        Err(Rejection::WrongPhase)
    );
    let kinds: Vec<StepKind> = authority.jobs()["job"]
        .steps
        .iter()
        .map(|step| step.kind)
        .collect();
    assert_eq!(kinds, [StepKind::Effect, StepKind::Effect, StepKind::Defer]);
    commit(&mut authority, complete(&lease, 3));
    assert!(authority.jobs().is_empty());
    assert_eq!(authority.receipts()["job"].steps, 3);
    assert_recovery_equivalent(&authority, authority.suffix(), 2);
}

#[test]
fn cancel_of_unleased_job_grants_for_cleanup() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(&mut authority, step(&lease, 0, "first"));
    commit(&mut authority, revoke(&lease));
    let expected = cancelled("operator", 1, None, 5);
    enter_cleanup(
        &mut authority,
        cancel("operator"),
        &expected,
        Disposition::Admitted,
    );
    assert_eq!(
        authority.prepare(cancel("again")),
        Ok(Decision::Duplicate(Ack {
            position: 5,
            disposition: Disposition::Park,
        }))
    );
    assert_recovery_equivalent(&authority, authority.suffix(), 2);

    let (fresh, replay) = grant(&mut authority, 12);
    assert_eq!(fresh.generation, 2);
    assert_eq!(replay.len(), 1);
    let job = &authority.jobs()["job"];
    assert_eq!(job.restarts, 0, "entering cleanup is progress");
    assert_eq!(
        job.progress,
        Some(Progress {
            steps: 1,
            waiting: None,
            cleanup: true,
        })
    );
    // Nothing was in flight when an unleased job was cancelled, so no
    // forward effect at all is admitted under the cleanup grant.
    assert_eq!(
        authority.prepare(step(&fresh, 1, "forward")),
        Err(Rejection::WrongPhase)
    );
    assert_eq!(
        authority.prepare(step(&lease, 1, "forward")),
        Err(Rejection::StaleLease)
    );
    commit(&mut authority, defer(&fresh, 1, "release"));
    commit(&mut authority, complete(&fresh, 2));
    assert_recovery_equivalent(&authority, authority.suffix(), 2);
}

#[test]
fn cancel_of_retrying_job_grants_before_the_retry_is_due() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (first, _) = grant(&mut authority, 11);
    commit(&mut authority, revoke(&first));
    let (second, _) = grant(&mut authority, 12);
    assert_eq!(authority.jobs()["job"].restarts, 1);
    let (ack, intents) = commit(
        &mut authority,
        Command::Retry {
            key: "job".into(),
            lease: second,
            error: "transient".into(),
            policy: Some(RetryPolicy::Fixed { delay_ms: 1_000 }),
            now_ms: 0,
            jitter_ms: 0,
        },
    );
    assert_eq!(ack.disposition, Disposition::Park);
    assert!(intents.is_empty());
    assert_eq!(
        authority.prepare(grant_at(13, 10)),
        Err(Rejection::RetryPending { fire_at_ms: 1_000 })
    );

    let expected = cancelled("operator", 0, None, 6);
    enter_cleanup(
        &mut authority,
        cancel("operator"),
        &expected,
        Disposition::Admitted,
    );
    let job = &authority.jobs()["job"];
    assert!(job.retry.is_some(), "the retry stays parked until granted");
    assert_eq!(
        authority.prepare(cancel("again")),
        Ok(Decision::Duplicate(Ack {
            position: 6,
            disposition: Disposition::Park,
        }))
    );
    assert_recovery_equivalent(&authority, authority.suffix(), 2);

    let (ack, intents) = commit(&mut authority, grant_at(13, 10));
    assert_eq!(ack.disposition, Disposition::Continue);
    let [Intent::Execute { lease, replay, .. }] = intents.as_slice() else {
        panic!("the cleanup grant executes the job")
    };
    assert!(replay.is_empty());
    assert_eq!(lease.generation, 3);
    let job = &authority.jobs()["job"];
    assert!(job.retry.is_none(), "the early grant consumes the retry");
    assert_eq!(job.restarts, 1, "a retry re-run is not a restart");
    assert_eq!(
        job.retry_attempts, 0,
        "the forward retry parked before cleanup is not charged to the cleanup budget"
    );
    assert_eq!(
        job.progress,
        Some(Progress {
            steps: 0,
            waiting: None,
            cleanup: true,
        })
    );
    assert_eq!(
        authority.prepare(step(lease, 0, "forward")),
        Err(Rejection::WrongPhase)
    );
    commit(&mut authority, defer(lease, 0, "release"));
    assert_eq!(authority.jobs()["job"].retry_attempts, 0);
    assert_recovery_equivalent(&authority, authority.suffix(), 2);
}

#[test]
fn duplicate_cancel_acks_without_a_second_intent() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    let expected = cancelled("first", 0, Some(&lease), 3);
    enter_cleanup(
        &mut authority,
        cancel("first"),
        &expected,
        Disposition::Admitted,
    );
    let acked = Ack {
        position: 3,
        disposition: Disposition::Continue,
    };
    assert_eq!(
        authority.prepare(cancel("second")),
        Ok(Decision::Duplicate(acked))
    );
    assert_eq!(
        authority.prepare(fail(&lease, "boom")),
        Ok(Decision::Duplicate(acked))
    );
    assert_eq!(
        authority.jobs()["job"].cleanup.as_ref(),
        Some(&expected),
        "the first cause stands"
    );
    assert_eq!(authority.revision(), 3);
    assert_eq!(
        authority.prepare(Command::Cancel {
            key: "other".into(),
            incarnation: 1,
            reason: "lost".into(),
        }),
        Err(Rejection::UnknownJob)
    );
    assert_eq!(
        authority.prepare(Command::Cancel {
            key: "job".into(),
            incarnation: 2,
            reason: "lost".into(),
        }),
        Err(Rejection::IncarnationConflict)
    );

    commit(&mut authority, complete(&lease, 0));
    assert_eq!(
        authority.prepare(cancel("late")),
        Err(Rejection::AlreadyTerminal)
    );
    assert_eq!(
        authority.prepare(fail(&lease, "late")),
        Err(Rejection::StaleLease)
    );
    assert_recovery_equivalent(&authority, authority.suffix(), 2);
}

#[test]
fn fail_records_the_cause_before_cleanup_steps() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(&mut authority, step(&lease, 0, "first"));
    commit(&mut authority, begin_wait(&lease, 1, TIMER));
    assert_eq!(
        authority.prepare(fail(&lease, "boom")),
        Err(Rejection::OutOfOrder)
    );
    commit(&mut authority, fire(1));

    let expected = Cleanup {
        cause: CleanupCause::Failed {
            error: "boom".into(),
        },
        steps: 2,
        lease: Some(lease.clone()),
        position: 6,
    };
    enter_cleanup(
        &mut authority,
        fail(&lease, "boom"),
        &expected,
        Disposition::Continue,
    );
    assert_recovery_equivalent(&authority, authority.suffix(), 2);
    assert_eq!(
        authority.prepare(fail(&lease, "boom")),
        Ok(Decision::Duplicate(Ack {
            position: 6,
            disposition: Disposition::Continue,
        }))
    );
    assert_eq!(
        authority.prepare(cancel("operator")),
        Ok(Decision::Duplicate(Ack {
            position: 6,
            disposition: Disposition::Continue,
        }))
    );
    // A terminal failure interrupts nothing in flight: the lease that
    // reported it admits no forward effect at the boundary.
    assert_eq!(
        authority.prepare(step(&lease, 2, "forward")),
        Err(Rejection::WrongPhase)
    );
    assert_eq!(
        authority.prepare(begin_wait(&lease, 2, WaitKind::Message)),
        Err(Rejection::WrongPhase)
    );
    commit(&mut authority, defer(&lease, 2, "release"));

    commit(&mut authority, revoke(&lease));
    assert_eq!(
        authority.prepare(fail(&lease, "boom")),
        Err(Rejection::StaleLease)
    );
    let (fresh, replay) = grant(&mut authority, 12);
    assert_eq!(replay.len(), 3);
    assert_eq!(
        authority.prepare(fail(&fresh, "boom")),
        Ok(Decision::Duplicate(Ack {
            position: 6,
            disposition: Disposition::Continue,
        })),
        "a replayed deterministic failure continues with the durable cause"
    );
    assert_eq!(
        authority.prepare(step(&fresh, 3, "forward")),
        Err(Rejection::WrongPhase)
    );
    commit(&mut authority, defer(&fresh, 3, "release-more"));
    assert_recovery_equivalent(&authority, authority.suffix(), 2);
}

#[test]
fn cleanup_resets_the_restart_budget_once() {
    let mut authority = Authority::new(7, 1);
    commit(
        &mut authority,
        Command::Submit {
            key: "job".into(),
            args: "args".into(),
            policy: RetryPolicy::Immediate,
            budget: RetryBudget {
                max_retries: None,
                max_restarts: Some(1),
            },
            retention_ms: DEFAULT_RETENTION_MS,
        },
    );
    let (first, _) = grant(&mut authority, 11);
    assert_eq!(authority.jobs()["job"].restarts, 0);
    commit(&mut authority, revoke(&first));
    let (second, _) = grant(&mut authority, 12);
    assert_eq!(authority.jobs()["job"].restarts, 1);

    let expected = cancelled("operator", 0, Some(&second), 5);
    enter_cleanup(
        &mut authority,
        cancel("operator"),
        &expected,
        Disposition::Admitted,
    );
    commit(&mut authority, revoke(&second));
    let (third, _) = grant(&mut authority, 13);
    assert_eq!(
        authority.jobs()["job"].restarts,
        0,
        "entering cleanup resets the restart budget"
    );
    commit(&mut authority, revoke(&third));
    let (fourth, _) = grant(&mut authority, 14);
    assert_eq!(
        authority.jobs()["job"].restarts,
        1,
        "a second crash in cleanup is a restart again"
    );
    commit(&mut authority, revoke(&fourth));
    assert_recovery_equivalent(&authority, authority.suffix(), 2);

    let fault = Fault {
        failure: JobFailure::RestartsExhausted {
            restarts: 2,
            max_restarts: 1,
        },
        expires_at_ms: DEFAULT_RETENTION_MS,
        position: 11,
    };
    let (ack, intents) = commit(&mut authority, grant_at(15, 0));
    assert_eq!(
        ack,
        Ack {
            position: 11,
            disposition: Disposition::Completed,
        }
    );
    assert_eq!(
        intents,
        vec![Intent::Fault {
            key: "job".into(),
            fault: fault.clone(),
        }]
    );
    assert!(authority.jobs().is_empty());
    assert_eq!(authority.receipts()["job"].outcome, Outcome::Faulted(fault));
    assert_eq!(
        authority.prepare(cancel("late")),
        Err(Rejection::AlreadyTerminal)
    );
    assert_eq!(
        authority.prepare(grant_at(16, 0)),
        Err(Rejection::AlreadyTerminal)
    );
    assert_recovery_equivalent(&authority, authority.suffix(), 2);
}

#[test]
fn crash_mid_cleanup_recovers_into_cleanup() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(&mut authority, step(&lease, 0, "acquire"));
    let expected = cancelled("operator", 1, Some(&lease), 4);
    enter_cleanup(
        &mut authority,
        cancel("operator"),
        &expected,
        Disposition::Admitted,
    );
    commit(&mut authority, defer(&lease, 1, "release-a"));

    let mut from_log = Authority::recover(7, 2, None, authority.suffix()).unwrap();
    let mut from_snapshot = Authority::recover(7, 2, Some(authority.snapshot()), &[]).unwrap();
    assert_eq!(from_log.jobs(), from_snapshot.jobs());
    for recovered in [&mut from_snapshot, &mut from_log] {
        let job = &recovered.jobs()["job"];
        assert_eq!(job.cleanup.as_ref(), Some(&expected));
        assert!(job.lease.is_none());
        assert_eq!(job.steps.len(), 2);

        let (fresh, replay) = grant(recovered, 12);
        assert_eq!((fresh.authority_epoch, fresh.generation), (2, 2));
        let kinds: Vec<StepKind> = replay.iter().map(|step| step.kind).collect();
        assert_eq!(kinds, [StepKind::Effect, StepKind::Defer]);
        assert_eq!(recovered.jobs()["job"].restarts, 0);
        // The interrupted lease's in-flight slot was already taken, and the
        // new lease never had anything in flight.
        assert_eq!(
            recovered.prepare(step(&fresh, 2, "forward")),
            Err(Rejection::WrongPhase)
        );
        assert_eq!(
            recovered.prepare(begin_wait(&fresh, 2, TIMER)),
            Err(Rejection::WrongPhase)
        );
        assert_eq!(
            recovered.prepare(step(&lease, 2, "forward")),
            Err(Rejection::StaleLease)
        );
        assert_eq!(
            recovered.prepare(defer(&lease, 1, "release-a")),
            Ok(Decision::Duplicate(Ack {
                position: 5,
                disposition: Disposition::Park,
            }))
        );
        commit(recovered, defer(&fresh, 2, "release-b"));
        commit(recovered, complete(&fresh, 3));
        assert!(recovered.jobs().is_empty());
        assert!(recovered.receipts().contains_key("job"));
    }
    assert_eq!(from_log.jobs(), from_snapshot.jobs());
    assert_eq!(from_log.receipts(), from_snapshot.receipts());
    assert_recovery_equivalent(&from_log, from_log.suffix(), 3);
}

#[test]
fn send_during_cleanup_is_rejected() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    let (queued, _) = commit(&mut authority, send("m1", "first"));
    commit(&mut authority, begin_wait(&lease, 0, WaitKind::Message));
    let (ack, _) = commit(&mut authority, begin_wait(&lease, 1, WaitKind::Message));
    assert_eq!(ack.disposition, Disposition::Park);

    let expected = cancelled("operator", 1, Some(&lease), 6);
    enter_cleanup(
        &mut authority,
        cancel("operator"),
        &expected,
        Disposition::Admitted,
    );
    assert_eq!(
        authority.prepare(send("m2", "second")),
        Err(Rejection::WrongPhase)
    );
    assert_eq!(
        authority.prepare(send("m1", "first")),
        Ok(Decision::Duplicate(queued))
    );
    assert_eq!(
        authority.prepare(send("m1", "other")),
        Err(Rejection::PayloadConflict)
    );
    // The abandoned wait cannot be re-attached by the interrupted invocation.
    assert_eq!(
        authority.prepare(begin_wait(&lease, 1, WaitKind::Message)),
        Err(Rejection::WrongPhase)
    );
    let job = &authority.jobs()["job"];
    assert!(job.inbox.is_empty());
    assert_eq!(job.messages.len(), 1);
    assert_eq!(authority.revision(), 6);
    commit(&mut authority, defer(&lease, 1, "release"));
    commit(&mut authority, complete(&lease, 2));
    assert_eq!(
        authority.prepare(send("m3", "late")),
        Err(Rejection::AlreadyTerminal)
    );
    assert_recovery_equivalent(&authority, authority.suffix(), 2);
}

#[test]
fn cancel_after_a_recovery_dropped_the_lease_replays_from_the_log() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    grant(&mut authority, 11);
    // Recovery drops the lease without logging a revocation, so the cancel
    // records no interrupted lease even though the log still shows a grant.
    let mut recovered = Authority::recover(7, 2, None, authority.suffix()).unwrap();
    assert!(recovered.jobs()["job"].lease.is_none());
    let expected = cancelled("operator", 0, None, 3);
    enter_cleanup(
        &mut recovered,
        cancel("operator"),
        &expected,
        Disposition::Admitted,
    );
    assert_recovery_equivalent(&recovered, recovered.suffix(), 3);
}

#[test]
fn defer_before_cleanup_is_wrong_phase() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    assert_eq!(
        authority.prepare(defer(&lease, 0, "release")),
        Err(Rejection::WrongPhase)
    );
    commit(&mut authority, step(&lease, 0, "first"));
    assert_eq!(
        authority.prepare(defer(&lease, 1, "release")),
        Err(Rejection::WrongPhase)
    );
    commit(&mut authority, revoke(&lease));
    assert_eq!(
        authority.prepare(defer(&lease, 1, "release")),
        Err(Rejection::StaleLease)
    );
    let (fresh, _) = grant(&mut authority, 12);
    assert_eq!(
        authority.prepare(defer(&fresh, 1, "release")),
        Err(Rejection::WrongPhase)
    );
    assert_eq!(authority.jobs()["job"].steps.len(), 1);
    assert_eq!(authority.revision(), 5);
}

fn retry(lease: &Lease, error: &str) -> Command {
    Command::Retry {
        key: "job".into(),
        lease: lease.clone(),
        error: error.into(),
        policy: None,
        now_ms: 0,
        jitter_ms: 0,
    }
}

#[test]
fn exhausted_forward_retries_enter_cleanup_on_a_fresh_budget() {
    let mut authority = Authority::new(7, 1);
    commit(
        &mut authority,
        Command::Submit {
            key: "job".into(),
            args: "args".into(),
            policy: RetryPolicy::Immediate,
            budget: RetryBudget {
                max_retries: Some(1),
                max_restarts: None,
            },
            retention_ms: DEFAULT_RETENTION_MS,
        },
    );
    let (first, _) = grant(&mut authority, 11);
    commit(&mut authority, step(&first, 0, "acquire"));
    let (ack, intents) = commit(&mut authority, retry(&first, "flaked"));
    assert_eq!(ack.disposition, Disposition::Park);
    assert!(intents.is_empty());
    assert_eq!(authority.jobs()["job"].retry_attempts, 1);
    let (second, replay) = grant(&mut authority, 12);
    assert_eq!(replay.len(), 1);

    // The budget is spent: the next transient failure moves the job into
    // cleanup instead of parking another retry, releasing the lease so the
    // grant that runs the compensation needs no retry deadline.
    let expected = Cleanup {
        cause: CleanupCause::RetriesExhausted {
            retries: 1,
            max_retries: 1,
            last_error: "flaked again".into(),
        },
        steps: 1,
        lease: Some(second.clone()),
        position: 6,
    };
    enter_cleanup(
        &mut authority,
        retry(&second, "flaked again"),
        &expected,
        Disposition::Park,
    );
    let job = &authority.jobs()["job"];
    assert!(job.retry.is_none(), "cleanup is not parked behind a retry");
    assert_eq!(
        authority.prepare(retry(&second, "flaked again")),
        Err(Rejection::StaleLease),
        "the released lease reports nothing further"
    );
    assert_eq!(
        authority.prepare(cancel("operator")),
        Ok(Decision::Duplicate(Ack {
            position: 6,
            disposition: Disposition::Park,
        })),
        "a cancel acknowledges the cleanup already under way"
    );
    assert_recovery_equivalent(&authority, authority.suffix(), 2);

    let (third, replay) = grant(&mut authority, 13);
    assert_eq!(replay.len(), 1);
    let job = &authority.jobs()["job"];
    assert_eq!(job.restarts, 0, "entering cleanup is progress");
    assert_eq!(
        job.progress,
        Some(Progress {
            steps: 1,
            waiting: None,
            cleanup: true,
        })
    );
    assert_eq!(
        authority.prepare(step(&third, 1, "forward")),
        Err(Rejection::WrongPhase)
    );
    commit(&mut authority, defer(&third, 1, "release"));

    // The cleanup phase has a retry budget of its own: one transient failure
    // parks a retry, the next is rejected and the lease holder settles.
    let (ack, intents) = commit(&mut authority, retry(&third, "release flaked"));
    assert_eq!(ack.disposition, Disposition::Park);
    assert!(intents.is_empty());
    assert_eq!(authority.jobs()["job"].retry_attempts, 1);
    let (fourth, replay) = grant(&mut authority, 14);
    assert_eq!(replay.len(), 2);
    assert_eq!(
        authority.prepare(retry(&fourth, "release flaked again")),
        Err(Rejection::RetriesExhausted {
            retries: 1,
            max_retries: 1,
        })
    );
    let job = &authority.jobs()["job"];
    assert_eq!(
        job.lease.as_ref(),
        Some(&fourth),
        "the rejected retry keeps the lease"
    );
    assert_eq!(job.cleanup.as_ref(), Some(&expected));
    assert_recovery_equivalent(&authority, authority.suffix(), 2);
    let (ack, _) = commit(&mut authority, complete(&fourth, 2));
    assert_eq!(ack.disposition, Disposition::Completed);
    assert_recovery_equivalent(&authority, authority.suffix(), 2);
}
