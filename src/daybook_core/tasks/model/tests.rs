//! Behavioural tests for the pure protocol model.
//!
//! Each test asserts a consumer-visible contract: merge order and idempotence,
//! collision rejection, monotonic success, and the local-producer invariants.
//! None of them inspect source text, mock echoes, or wiring.

use super::super::test_util::*;
use super::*;

/// The same inputs derive the same id on two isolated nodes, and no two
/// distinct tuples collide. This is the property that lets independent triage
/// nodes deduplicate obligations without coordination.
#[test]
fn derived_task_ids_are_stable_and_distinct_per_input() {
    let first = derived_task("G1");
    let second = derived_task("G1");
    assert_eq!(first, second, "same inputs must derive the same id");

    let other_generation = derived_task("G2");
    assert_ne!(first, other_generation);

    let other_slot = derive_pool_task_id(PoolTaskIdInputs {
        domain: &domain(),
        pool: &pool(),
        slot: &CoordinationSlotKey::from_label("photo-7/other-processor"),
        generation: &WorkGeneration::from_label("G1"),
    });
    assert_ne!(first, other_slot);

    let other_domain = derive_pool_task_id(PoolTaskIdInputs {
        domain: &DomainId::from_label("agent"),
        pool: &pool(),
        slot: &CoordinationSlotKey::from_label("photo-7/describe-image"),
        generation: &WorkGeneration::from_label("G1"),
    });
    assert_ne!(first, other_domain);
}

/// Two publishers asserting the same obligation publish different envelopes and
/// different publisher identities; merging keeps both evidence records and does
/// not change the canonical meaning. This is the equivalent-publishers case.
#[test]
fn equivalent_publishers_merge_evidence_without_changing_meaning() {
    let declaration = declaration("G1");
    let mut local = TaskTicket::new(declaration.clone(), evidence(node(1), 1));

    let mut remote = TaskTicket::new(declaration.clone(), evidence(node(2), 2));
    remote.terminal.insert(
        node(2),
        terminal_lane(node(2), 1, success_fact(7, "report://a")),
    );

    let disposition = local
        .merge(&remote)
        .expect("equivalent publishers must merge");
    assert_eq!(disposition, MergeDisposition::Changed);
    assert_eq!(local.declaration.publishers.len(), 2);
    assert_eq!(local.declaration.declaration, declaration);
    assert!(
        local
            .declaration
            .publishers
            .iter()
            .any(|evidence| evidence.publisher == node(2)),
        "remote publisher evidence must survive the merge"
    );
    assert_eq!(
        local.terminal_summary(),
        Some(TerminalSummary::Succeeded {
            attempt_id: PoolAttemptId::new(7),
            result_ref: Some(OpaqueResultRef::from_label("report://a")),
        }),
        "a merged remote success must be observed locally"
    );
}

/// Merge is commutative and idempotent: merging A into B and B into A reach the
/// same terminal lanes, and re-merging changes nothing.
#[test]
fn merge_is_commutative_and_idempotent() {
    let declaration = declaration("G1");
    let mut alpha = TaskTicket::new(declaration.clone(), evidence(node(1), 1));
    alpha.terminal.insert(
        node(1),
        terminal_lane(node(1), 3, cancelled_fact("superseded")),
    );

    let mut beta = TaskTicket::new(declaration.clone(), evidence(node(2), 2));
    beta.terminal.insert(
        node(2),
        terminal_lane(node(2), 1, success_fact(9, "report://b")),
    );

    let mut alpha_then_beta = alpha.clone();
    assert_eq!(
        alpha_then_beta.merge(&beta).expect("valid merge"),
        MergeDisposition::Changed
    );
    let mut beta_then_alpha = beta.clone();
    assert_eq!(
        beta_then_alpha.merge(&alpha).expect("valid merge"),
        MergeDisposition::Changed
    );
    assert_eq!(
        alpha_then_beta.terminal_summary(),
        beta_then_alpha.terminal_summary()
    );
    assert_eq!(alpha_then_beta.terminal, beta_then_alpha.terminal);
    assert_eq!(
        alpha_then_beta.version_digest(),
        beta_then_alpha.version_digest()
    );
    assert_ne!(alpha_then_beta.version_digest(), alpha.version_digest());

    assert_eq!(
        alpha_then_beta.merge(&beta).expect("replay merges"),
        MergeDisposition::Unchanged,
        "re-merging the same ticket must be a no-op"
    );
    assert_eq!(
        alpha_then_beta.merge(&alpha).expect("replay merges"),
        MergeDisposition::Unchanged
    );
    assert_eq!(alpha_then_beta.terminal, beta_then_alpha.terminal);
    assert_eq!(
        alpha_then_beta.version_digest(),
        beta_then_alpha.version_digest()
    );
}

/// A newer sequence for one writer replaces that writer's lane; an older one
/// does not, so a stale replica cannot roll terminal state backwards.
#[test]
fn terminal_lanes_take_the_greatest_writer_sequence() {
    let mut local = ticket("G1");
    local.terminal.insert(
        node(1),
        terminal_lane(node(1), 4, cancelled_fact("cancelled")),
    );

    let mut newer = ticket("G1");
    newer.terminal.insert(
        node(1),
        terminal_lane(node(1), 5, success_fact(11, "report://c")),
    );
    let previous_version = local.version_digest();
    assert_eq!(
        local.merge(&newer).expect("newer lane wins"),
        MergeDisposition::Changed
    );
    assert_eq!(
        local.terminal[&node(1)].writer_seq,
        5,
        "the greater writer sequence must win"
    );
    assert_ne!(local.version_digest(), previous_version);

    let mut older = ticket("G1");
    older
        .terminal
        .insert(node(1), terminal_lane(node(1), 2, cancelled_fact("old")));
    let snapshot = local.clone();
    assert_eq!(
        local.merge(&older).expect("older lane is ignored"),
        MergeDisposition::Unchanged
    );
    assert_eq!(local, snapshot, "a stale sequence must not change the lane");
}

#[test]
fn same_writer_terminal_merge_is_a_monotonic_join() {
    let facts = [
        (1, cancelled_fact("early")),
        (2, success_fact(10, "report://first")),
        (3, success_fact(11, "report://latest")),
        (4, cancelled_fact("late")),
    ]
    .map(|(seq, fact)| {
        let mut value = ticket("G1");
        value
            .terminal
            .insert(node(1), terminal_lane(node(1), seq, fact));
        value
    });
    for left in &facts {
        let mut replay = left.clone();
        assert_eq!(replay.merge(left).unwrap(), MergeDisposition::Unchanged);
        assert_eq!(&replay, left);
        for right in &facts {
            let mut lr = left.clone();
            lr.merge(right).unwrap();
            let mut rl = right.clone();
            rl.merge(left).unwrap();
            assert_eq!(lr, rl, "same-writer merge must commute");
            assert_eq!(lr.is_success(), left.is_success() || right.is_success());
            for third in &facts {
                let mut left_associated = lr.clone();
                left_associated.merge(third).unwrap();
                let mut right_pair = right.clone();
                right_pair.merge(third).unwrap();
                let mut right_associated = left.clone();
                right_associated.merge(&right_pair).unwrap();
                assert_eq!(left_associated, right_associated, "merge must associate");
            }
        }
    }
    let mut all = facts[0].clone();
    for fact in &facts[1..] {
        all.merge(fact).unwrap();
    }
    assert_eq!(all.terminal[&node(1)], facts[2].terminal[&node(1)]);
}

#[test]
fn conflicting_success_sequence_rejects_the_whole_merge() {
    let mut local = ticket("G1");
    local.terminal.insert(
        node(1),
        terminal_lane(node(1), 7, success_fact(1, "report://a")),
    );
    let snapshot = local.clone();
    let mut remote = TaskTicket::new(declaration("G1"), evidence(node(3), 3));
    remote.terminal.insert(
        node(1),
        terminal_lane(node(1), 7, success_fact(2, "report://b")),
    );
    remote.terminal.insert(
        node(2),
        terminal_lane(node(2), 8, success_fact(3, "report://c")),
    );
    assert!(matches!(
        local.merge(&remote),
        Err(CollisionRejected::EquivocatedTerminal { writer_seq: 7, .. })
    ));
    assert_eq!(
        local, snapshot,
        "rejection must not add evidence or other lanes"
    );
}

/// A success that has already been observed is never erased by a later merge of
/// stale pending state or a cancellation. This is the monotonicity contract.
#[test]
fn successful_terminal_fact_survives_a_later_pending_merge() {
    let mut local = ticket("G1");
    local.terminal.insert(
        node(1),
        terminal_lane(node(1), 2, success_fact(3, "report://done")),
    );
    let success = local.terminal_summary();
    assert!(local.is_success());

    let stale_pending = ticket("G1");
    assert_eq!(
        local.merge(&stale_pending).expect("pending state is valid"),
        MergeDisposition::Unchanged
    );
    assert_eq!(local.terminal_summary(), success);
    assert!(local.is_success());

    let mut cancellation = ticket("G1");
    cancellation.terminal.insert(
        node(2),
        terminal_lane(node(2), 1, cancelled_fact("late cancel")),
    );
    assert_eq!(
        local.merge(&cancellation).expect("cancellation is valid"),
        MergeDisposition::Changed
    );
    assert_eq!(
        local.terminal_summary(),
        success,
        "a cancellation must not displace an observed success"
    );
    assert_eq!(
        local.terminal[&node(1)].writer_seq,
        2,
        "the success lane must remain intact"
    );
}

/// Unequal canonical declarations under one id are a collision, not sibling
/// revisions. The error reports both sides so a caller can diagnose it.
#[test]
fn unequal_declarations_for_one_id_are_rejected() {
    let mut local = ticket("G1");
    let mut hostile = ticket("G1");
    hostile.declaration.declaration.effect_policy = EffectPolicy::AcceptsDuplicates;

    let error = local
        .merge(&hostile)
        .expect_err("unequal declarations must collide");
    match error {
        CollisionRejected::UnequalDeclaration {
            task_id,
            existing,
            incoming,
        } => {
            assert_eq!(task_id, local.task_id);
            assert_eq!(existing.effect_policy, EffectPolicy::Idempotent);
            assert_eq!(incoming.effect_policy, EffectPolicy::AcceptsDuplicates);
        }
        other => panic!("unexpected rejection: {other:?}"),
    }

    assert_eq!(
        local.declaration.declaration.effect_policy,
        EffectPolicy::Idempotent,
        "a rejected remote declaration must not mutate local state"
    );
}

/// Two writers reporting different facts at the same sequence is equivocation
/// and is rejected rather than silently preferred.
#[test]
fn equivocated_terminal_sequence_is_rejected() {
    let mut local = ticket("G1");
    local.terminal.insert(
        node(1),
        terminal_lane(node(1), 7, success_fact(1, "report://x")),
    );

    let mut hostile = ticket("G1");
    hostile.terminal.insert(
        node(1),
        terminal_lane(node(1), 7, cancelled_fact("equivocation")),
    );

    let error = local
        .merge(&hostile)
        .expect_err("equivocation must collide");
    assert_eq!(
        error,
        CollisionRejected::EquivocatedTerminal {
            task_id: local.task_id,
            writer: node(1),
            writer_seq: 7,
        }
    );
}

/// Unknown retained schema versions are unsupported data, not cancellation or
/// permission to prune.
#[test]
fn unknown_persisted_schema_is_rejected() {
    let mut local = ticket("G1");
    let mut from_the_future = ticket("G1");
    from_the_future.schema = TASK_TICKET_PAYLOAD_SCHEMA + 1;

    let error = local
        .merge(&from_the_future)
        .expect_err("unknown persisted schema must be rejected");
    assert_eq!(
        error,
        CollisionRejected::SchemaMismatch {
            schema: TASK_TICKET_PAYLOAD_SCHEMA + 1,
        }
    );
}

/// Merging two different task identities is a programming error at the call
/// site, and is reported rather than silently unioning lanes.
#[test]
fn mismatched_identity_merge_is_rejected() {
    let mut local = ticket("G1");
    let other = ticket("G2");
    let error = local.merge(&other).expect_err("identity mismatch");
    assert_eq!(
        error,
        CollisionRejected::MismatchedIdentity {
            existing: derived_task("G1"),
            incoming: derived_task("G2"),
        }
    );
}

/// The canonical digest ignores publisher evidence and local hints but changes
/// with any execution meaning.
#[test]
fn canonical_digest_covers_only_execution_meaning() {
    let declaration = declaration("G1");
    let digest = declaration.canonical_digest();

    let different_envelope = TaskTicket::new(declaration.clone(), evidence(node(3), 99));
    assert_eq!(
        different_envelope
            .declaration
            .declaration
            .canonical_digest(),
        digest,
        "publisher evidence must not affect canonical meaning"
    );

    let mut different_meaning = declaration.clone();
    different_meaning.handler = HandlerRef::from_label("ocr");
    assert_ne!(different_meaning.canonical_digest(), digest);

    let mut different_producer = declaration.clone();
    different_producer.producer = Some(node(4));
    assert_ne!(
        different_producer.canonical_digest(),
        digest,
        "origin is canonical meaning, not publisher evidence"
    );
}

/// A local producer with an inverted window or a ticket-authoritative record
/// with no execution deadline is an invariant break and is reported as such.
#[test]
fn local_declaration_validation_catches_producer_invariants() {
    let mut inverted = declaration("G1");
    inverted.not_before_secs = Some(100);
    inverted.not_after_secs = Some(50);
    assert_eq!(
        inverted.validate_local(),
        DeclarationViolation::WindowInverted {
            not_before_secs: 100,
            not_after_secs: 50,
        }
    );

    let mut horizon_without_deadline = declaration("G1");
    horizon_without_deadline.result_retention = ResultRetention::TicketAuthoritative {
        retain_until: Some(100),
    };
    assert_eq!(
        horizon_without_deadline.validate_local(),
        DeclarationViolation::MissingExecutionDeadline
    );

    let mut valid = declaration("G1");
    valid.result_retention = ResultRetention::TicketAuthoritative {
        retain_until: Some(100),
    };
    valid.not_after_secs = Some(100);
    assert!(valid.validate_local().is_valid());
}

/// A local producer that violates an invariant is a programming error in this
/// process and fails loudly, rather than being reported as rejected remote input.
#[test]
#[should_panic(expected = "local task declaration violates a producer invariant")]
fn local_producer_violation_fails_loudly() {
    let mut invalid = declaration("G1");
    invalid.not_before_secs = Some(10);
    invalid.not_after_secs = Some(5);
    let _ticket = TaskTicket::declare_local(invalid, evidence(node(1), 1));
}

/// A writer's own later cancellation does not displace its earlier success:
/// success is monotonic per writer regardless of sequence.
#[test]
fn a_writers_own_cancellation_does_not_displace_its_success() {
    let mut local = ticket("G1");
    local.terminal.insert(
        node(1),
        terminal_lane(node(1), 1, success_fact(4, "report://done")),
    );
    let success = local.terminal_summary();

    let mut later_cancel = ticket("G1");
    later_cancel.terminal.insert(
        node(1),
        terminal_lane(node(1), 9, cancelled_fact("too late")),
    );
    assert_eq!(
        local.merge(&later_cancel).expect("valid merge"),
        MergeDisposition::Unchanged,
        "a cancellation from the success's own writer must not replace it"
    );
    assert_eq!(local.terminal_summary(), success);
    assert!(local.is_success());

    // Commutative in the other order too.
    let mut cancelled_first = later_cancel.clone();
    cancelled_first
        .merge(&{
            let mut other = ticket("G1");
            other.terminal.insert(
                node(1),
                terminal_lane(node(1), 1, success_fact(4, "report://done")),
            );
            other
        })
        .expect("valid merge");
    assert_eq!(cancelled_first.terminal_summary(), success);
}

/// A rejected merge leaves local state untouched: no partial ingestion of a
/// malformed remote payload.
#[test]
fn a_rejected_merge_is_atomic() {
    let mut local = ticket("G1");

    // One valid lane plus one equivocated lane: the whole payload must be
    // rejected, and the valid lane must not be retained.
    local.terminal.insert(
        node(1),
        terminal_lane(node(1), 7, success_fact(1, "report://x")),
    );

    let mut hostile = ticket("G1");
    hostile.terminal.insert(
        node(1),
        terminal_lane(node(1), 7, cancelled_fact("equivocation")),
    );
    hostile.terminal.insert(
        node(2),
        terminal_lane(node(2), 1, success_fact(2, "report://y")),
    );
    hostile.declaration.publishers.push(evidence(node(5), 5));

    let before = local.clone();
    assert!(
        local.merge(&hostile).is_err(),
        "equivocation must be rejected"
    );
    assert_eq!(
        local, before,
        "a rejected payload must not have mutated local state"
    );
    assert!(
        !local.terminal.contains_key(&node(2)),
        "the malformed payload's other lanes must not be retained"
    );
}

/// Publisher evidence is a set of records: different envelopes merge to the same
/// set regardless of order, and re-merging is a no-op.
#[test]
fn evidence_converges_independently_of_merge_order() {
    let declaration = declaration("G1");

    let alpha = TaskTicket::new(declaration.clone(), evidence(node(1), 1));
    let beta = TaskTicket::new(declaration.clone(), evidence(node(2), 2));

    let mut alpha_then_beta = alpha.clone();
    assert_eq!(
        alpha_then_beta.merge(&beta).expect("valid merge"),
        MergeDisposition::Changed
    );
    let mut beta_then_alpha = beta.clone();
    assert_eq!(
        beta_then_alpha.merge(&alpha).expect("valid merge"),
        MergeDisposition::Changed
    );
    assert_eq!(
        alpha_then_beta.declaration.publishers, beta_then_alpha.declaration.publishers,
        "evidence must converge to one canonical set"
    );
    assert_eq!(alpha_then_beta.declaration.publishers.len(), 2);
    assert_eq!(
        alpha_then_beta.merge(&beta).expect("replay"),
        MergeDisposition::Unchanged,
        "re-merging the same evidence must be a no-op"
    );
}

/// Two distinct envelopes from the same publisher both survive, because evidence
/// is keyed by content rather than by publisher alone.
#[test]
fn distinct_envelopes_from_one_publisher_both_survive() {
    let declaration = declaration("G1");
    let mut local = TaskTicket::new(declaration.clone(), evidence(node(1), 1));
    let mut remote = TaskTicket::new(declaration.clone(), evidence(node(1), 2));

    assert_eq!(
        local.merge(&remote).expect("valid merge"),
        MergeDisposition::Changed
    );
    assert_eq!(
        local.declaration.publishers.len(),
        2,
        "both envelopes are evidence of the same declaration"
    );

    remote.merge(&local).expect("valid merge");
    assert_eq!(remote.declaration.publishers, local.declaration.publishers);
}

/// A terminal summary is a pure function of lane contents, including the
/// choice among concurrent cancellations, so replicas agree.
#[test]
fn terminal_summary_is_deterministic_across_lane_orderings() {
    let mut left = ticket("G1");
    left.terminal
        .insert(node(1), terminal_lane(node(1), 1, cancelled_fact("one")));
    left.terminal
        .insert(node(2), terminal_lane(node(2), 1, cancelled_fact("two")));

    let mut right = ticket("G1");
    right
        .terminal
        .insert(node(2), terminal_lane(node(2), 1, cancelled_fact("two")));
    right
        .terminal
        .insert(node(1), terminal_lane(node(1), 1, cancelled_fact("one")));

    assert_eq!(left.terminal_summary(), right.terminal_summary());
    assert_eq!(left.version_digest(), right.version_digest());
    assert_eq!(
        left.terminal_summary(),
        Some(TerminalSummary::Cancelled {
            reason: OpaqueReason::from_label("one"),
        }),
        "the lowest writer key breaks the tie deterministically"
    );
}

#[test]
fn canonical_input_collision_rejects_all_remote_evidence() {
    let mut local = ticket("G1");
    let before = local.clone();
    let mut remote = TaskTicket::new(declaration("G1"), evidence(node(8), 8));
    remote.declaration.declaration.input.push(0);
    remote.terminal.insert(
        node(8),
        terminal_lane(node(8), 1, success_fact(8, "new-input-result")),
    );
    assert_ne!(
        local.declaration.declaration.canonical_digest(),
        remote.declaration.declaration.canonical_digest()
    );
    assert!(matches!(
        local.merge(&remote),
        Err(CollisionRejected::UnequalDeclaration { .. })
    ));
    assert_eq!(local, before);
}

#[test]
fn indefinite_and_finite_retention_have_distinct_deadline_contracts() {
    let mut indefinite = declaration("G1");
    indefinite.result_retention = ResultRetention::TicketAuthoritative { retain_until: None };
    assert_eq!(indefinite.validate_local(), DeclarationViolation::Valid);
    let mut finite = indefinite.clone();
    finite.result_retention = ResultRetention::TicketAuthoritative {
        retain_until: Some(100),
    };
    assert_eq!(
        finite.validate_local(),
        DeclarationViolation::MissingExecutionDeadline
    );
    finite.not_after_secs = Some(100);
    assert!(finite.validate_local().is_valid());
    indefinite.not_after_secs = Some(100);
    assert_ne!(finite.canonical_digest(), indefinite.canonical_digest());
    finite.not_after_secs = Some(101);
    assert_eq!(
        finite.validate_local(),
        DeclarationViolation::RetentionPrecedesDeadline {
            not_after_secs: 101,
            retain_until: 100,
        }
    );
}

#[test]
fn malformed_remote_declarations_are_rejected_before_any_mutation() {
    let mut invalid_placement = declaration("G1");
    invalid_placement.effect_policy = EffectPolicy::AuthoritativePlacement;
    invalid_placement.placement = Preference::PreferOrigin(node(1));
    let mut invalid_retention = declaration("G1");
    invalid_retention.result_retention = ResultRetention::TicketAuthoritative {
        retain_until: Some(100),
    };
    invalid_retention.not_after_secs = Some(101);
    for (declaration, violation) in [
        (
            invalid_placement,
            DeclarationViolation::AuthoritativePlacementRequiresOnly,
        ),
        (
            invalid_retention,
            DeclarationViolation::RetentionPrecedesDeadline {
                not_after_secs: 101,
                retain_until: 100,
            },
        ),
    ] {
        let mut local = ticket("G1");
        let before = local.clone();
        let mut remote = TaskTicket::new(declaration, evidence(node(8), 8));
        remote.terminal.insert(
            node(8),
            terminal_lane(node(8), 1, cancelled_fact("malformed terminal")),
        );
        assert_eq!(
            local.merge(&remote),
            Err(CollisionRejected::InvalidDeclaration {
                task_id: derived_task("G1"),
                violation,
            })
        );
        assert_eq!(local, before);
    }
}

#[test]
#[should_panic(expected = "local task declaration violates a producer invariant")]
fn authoritative_placement_without_only_is_a_local_programming_error() {
    let mut invalid = declaration("G1");
    invalid.effect_policy = EffectPolicy::AuthoritativePlacement;
    invalid.placement = Preference::AnyNode;
    let _ticket = TaskTicket::declare_local(invalid, evidence(node(1), 1));
}

#[test]
fn authoritative_placement_accepts_an_explicit_owner() {
    let mut declaration = declaration("G1");
    declaration.effect_policy = EffectPolicy::AuthoritativePlacement;
    declaration.placement = Preference::Only(node(3));
    assert!(declaration.validate_local().is_valid());
}
