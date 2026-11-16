use super::*;
use wflow_wdk::StepKind;
use wflow_wdk::types::RetryPolicy;

fn cancel(reason: &str) -> Command {
    Command::Cancel {
        key: "job".into(),
        incarnation: 1,
        reason: reason.into(),
        now_ms: 0,
    }
}

fn retry(lease: &Lease, error: &str) -> Command {
    Command::Retry {
        key: "job".into(),
        lease: lease.clone(),
        error: error.into(),
        policy: None,
        now_ms: 0,
    }
}

#[test]
fn cancellation_atomically_faults_and_fences_all_late_work() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(&mut authority, begin_wait(&lease, 0, TIMER));

    let fault = Fault {
        failure: JobFailure::Cancelled {
            reason: "operator".into(),
        },
        position: 4,
    };
    let (ack, intents) = commit(&mut authority, cancel("operator"));
    assert_eq!(
        ack,
        Ack {
            position: 4,
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
        authority.prepare(step(&lease, 0, "late")),
        Err(Rejection::StaleLease)
    );
    assert_eq!(
        authority.prepare(begin_wait(&lease, 0, TIMER)),
        Err(Rejection::StaleLease)
    );
    assert_eq!(
        authority.prepare(Command::Complete {
            key: "job".into(),
            lease: lease.clone(),
            expected_steps: 0,
            value: "late".into(),
            now_ms: 0,
        }),
        Err(Rejection::StaleLease)
    );
    assert_eq!(
        authority.prepare(retry(&lease, "late")),
        Err(Rejection::StaleLease)
    );
    assert_eq!(
        authority.prepare(Command::Revoke {
            key: "job".into(),
            lease,
        }),
        Err(Rejection::StaleLease)
    );
    assert_eq!(authority.prepare(fire(0)), Err(Rejection::AlreadyTerminal));
    assert_eq!(
        authority.prepare(send("late", "late")),
        Err(Rejection::AlreadyTerminal)
    );
    assert_eq!(
        authority.prepare(Command::Grant {
            key: "job".into(),
            incarnation: 1,
            worker: 12,
            now_ms: 0,
        }),
        Err(Rejection::AlreadyTerminal)
    );
    assert_eq!(
        authority.prepare(cancel("second")),
        Err(Rejection::AlreadyTerminal)
    );
    assert_eq!(authority.revision(), 4);
}

#[test]
fn cancellation_receipt_is_permanent_and_recovers() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(&mut authority, retry(&lease, "transient"));
    commit(&mut authority, cancel("operator"));

    assert_eq!(
        authority.prepare(submit_command()),
        Ok(Decision::Duplicate(Ack {
            position: 1,
            disposition: Disposition::Completed,
        }))
    );
    assert_eq!(authority.next_job_incarnation(), 1);

    let recovered = Authority::recover(7, 2, None, authority.suffix()).unwrap();
    assert!(recovered.jobs().is_empty());
    assert_eq!(recovered.receipts(), authority.receipts());
    assert_eq!(
        recovered.prepare(cancel("after restart")),
        Err(Rejection::AlreadyTerminal)
    );
}

#[test]
fn cancel_after_success_is_already_terminal() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);
    commit(
        &mut authority,
        Command::Complete {
            key: "job".into(),
            lease,
            expected_steps: 0,
            value: "done".into(),
            now_ms: 0,
        },
    );

    assert_eq!(
        authority.prepare(cancel("too late")),
        Err(Rejection::AlreadyTerminal)
    );
}

#[test]
fn retry_exhaustion_leaves_the_lease_held_for_settlement() {
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
            retention_ms: RETENTION,
        },
    );
    let (first, _) = grant(&mut authority, 11);
    commit(&mut authority, retry(&first, "first"));
    let (second, _) = grant(&mut authority, 12);
    let revision = authority.revision();

    assert_eq!(
        authority.prepare(retry(&second, "exhausted")),
        Err(Rejection::RetriesExhausted {
            retries: 1,
            max_retries: 1,
        })
    );
    let job = &authority.jobs()["job"];
    assert_eq!(job.lease.as_ref(), Some(&second));
    assert!(job.retry.is_none());
    assert_eq!(job.retry_attempts, 1);
    assert_eq!(authority.revision(), revision);

    commit(
        &mut authority,
        Command::Fail {
            key: "job".into(),
            lease: second,
            failure: JobFailure::RetriesExhausted {
                retries: 1,
                max_retries: 1,
                last_error: "exhausted".into(),
            },
            now_ms: 0,
        },
    );
    assert!(authority.receipts().contains_key("job"));
}

#[test]
fn commit_step_accepts_effects_only() {
    let mut authority = Authority::new(7, 1);
    submit(&mut authority);
    let (lease, _) = grant(&mut authority, 11);

    for kind in [StepKind::Sleep, StepKind::Recv] {
        assert_eq!(
            authority.prepare(step_as(&lease, 0, "value", kind, None)),
            Err(Rejection::Nondeterministic)
        );
    }
    assert!(authority.jobs()["job"].steps.is_empty());
    commit(&mut authority, step(&lease, 0, "effect"));
}
