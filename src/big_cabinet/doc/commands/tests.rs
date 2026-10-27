use super::*;

fn command(label: &str) -> CommandId {
    CommandId::try_from(label.to_string()).expect("a valid command id")
}

async fn ledger() -> Commands {
    Commands::open(sqlite_rs::test_support::db(&[]).expect("sqlite").clone())
        .await
        .expect("the ledger opens")
}

async fn begin_pending(commands: &Commands, id: &CommandId) -> Row {
    let row = Row {
        command: id.clone(),
        kind: Kind::Create,
        digest: [7; 32],
        actor: Uuid7::new(),
        document: None,
        placement: Some("drawer:a".to_string()),
        detail: None,
        state: State::Pending,
        heads: Vec::new(),
    };
    commands
        .begin(
            id,
            row.kind,
            row.digest,
            row.actor,
            row.document.as_ref(),
            row.placement.as_deref(),
            row.detail.as_ref(),
        )
        .await
        .expect("begin");
    row
}

fn assert_rows(actual: impl IntoIterator<Item = Row>, expected: impl IntoIterator<Item = Row>) {
    let fields = |row: Row| {
        (
            row.command,
            row.kind,
            row.digest,
            row.actor,
            row.document,
            row.placement,
            row.detail,
            row.state,
            row.heads,
        )
    };
    assert_eq!(
        actual.into_iter().map(fields).collect::<Vec<_>>(),
        expected.into_iter().map(fields).collect::<Vec<_>>(),
    );
}

const COMMITTED_HEADS: &[ChangeHash] = &[ChangeHash([1; 32]), ChangeHash([2; 32])];
const DONE_HEADS: &[ChangeHash] = &[ChangeHash([3; 32])];

#[tokio::test]
async fn migrations_preserve_existing_command_states_and_receipts() {
    let sql = sqlite_rs::test_support::db(&[]).unwrap().clone();
    sql.execute_batch(include_str!("../sqlite/migrations/001_commands.sql"))
        .await
        .unwrap();
    let commands = Commands { sql: sql.clone() };
    let mut rows = Vec::new();
    for state in [State::Pending, State::Committed, State::Done] {
        let mut row = begin_pending(&commands, &command(state.as_str())).await;
        if state != State::Pending {
            commands
                .advance(&row.command, state, DONE_HEADS)
                .await
                .unwrap();
            row.state = state;
            row.heads = DONE_HEADS.to_vec();
        }
        rows.push(row);
    }
    for _ in 0..2 {
        let reopened = Commands::open(sql.clone()).await.unwrap();
        for row in &rows {
            assert_rows(reopened.get(&row.command).await.unwrap(), [row.clone()]);
        }
        assert_eq!(
            sql.migration_status_for(sqlite::MIGRATION_TABLE, sqlite::MIGRATIONS)
                .await
                .unwrap(),
            vec![(sqlite::MIGRATIONS[0], sqlite_rs::MigrationState::Applied)],
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum Op {
    SetDoc,
    Advance(State),
    Discard,
}

utils_rs::table_tests! {
    begin_cases tokio,
    (kind, document, placement, detail),
    {
        let commands = ledger().await;
        let id = command("cmd-begin");
        assert_rows(commands.get(&id).await.expect("get missing"), []);
        let row = Row {
            command: id.clone(), kind, digest: [7; 32], actor: Uuid7::new(),
            document, placement: placement.map(str::to_string), detail,
            state: State::Pending, heads: Vec::new(),
        };
        commands
            .begin(&id, row.kind, row.digest, row.actor, row.document.as_ref(), row.placement.as_deref(), row.detail.as_ref())
            .await
            .expect("begin");
        let reopened = Commands::open(commands.sql.clone()).await.expect("reopen");
        assert_rows(reopened.get(&id).await.expect("get"), [row]);
    },
}

begin_cases! {
    create_before_reservation: (Kind::Create, None, Some("drawer:a"), None),
    write_with_document: (Kind::Write, Some(DocId::new([9; 32])), None::<&str>, None),
    branch_before_reservation: (
        Kind::Branch,
        None,
        Some("drawer:a"),
        Some(Detail::Branch { origin: DocId::new([8; 32]), path: "/draft".into(), stealth: false, owner_id: Some(Uuid7::new()) }),
    ),
    merge_with_source_heads: (
        Kind::Merge,
        Some(DocId::new([9; 32])),
        None::<&str>,
        Some(Detail::Merge { source: DocId::new([8; 32]), heads: COMMITTED_HEADS.to_vec() }),
    ),
}

utils_rs::table_tests! {
    begin_conflicts tokio,
    (state, digest),
    {
        let commands = ledger().await;
        let id = command("cmd-conflict");
        let mut row = begin_pending(&commands, &id).await;
        if state != State::Pending {
            commands.advance(&id, state, COMMITTED_HEADS).await.expect("advance");
            row.state = state;
            row.heads = COMMITTED_HEADS.to_vec();
        }
        assert_eq!(
            commands.begin(&id, Kind::Write, digest, Uuid7::new(), Some(&DocId::new([9; 32])), Some("drawer:b"), None).await,
            Err(DocsError::CommandInFlight { command: id.clone() }),
        );
        assert_rows(commands.get(&id).await.expect("get"), [row]);
    },
}

begin_conflicts! {
    pending_same_request_digest: (State::Pending, [7; 32]),
    pending_different_request_digest: (State::Pending, [8; 32]),
    committed_same_request_digest: (State::Committed, [7; 32]),
    committed_different_request_digest: (State::Committed, [8; 32]),
    done_same_request_digest: (State::Done, [7; 32]),
    done_different_request_digest: (State::Done, [8; 32]),
}

utils_rs::table_tests! {
    transition_cases tokio,
    (ops, expected),
    {
        let ops: &[Op] = ops;
        let expected: Option<(State, Option<DocId>, &[ChangeHash])> = expected;
        let commands = ledger().await;
        let id = command("cmd-transition");
        let initial = begin_pending(&commands, &id).await;
        for op in ops {
            match op {
                Op::SetDoc => commands
                    .set_document(&id, &DocId::new([3; 32]))
                    .await
                    .expect("set_document"),
                Op::Advance(state) => {
                    let heads = if *state == State::Done { DONE_HEADS } else { COMMITTED_HEADS };
                    commands.advance(&id, *state, heads).await.expect("advance");
                }
                Op::Discard => commands.discard(&id).await.expect("discard"),
            }
        }
        let done_at = sqlite::command_done_at(&commands.sql, id.as_str().to_owned())
            .await
            .expect("completion timestamp");
        assert_eq!(done_at.map(|timestamp| timestamp.is_some()), expected.as_ref().map(|(state, _, _)| *state == State::Done));
        assert_rows(
            commands.get(&id).await.expect("get"),
            expected.map(|(state, document, heads)| Row { state, document, heads: heads.to_vec(), ..initial }),
        );
    },
    enable_tracing: false,
}

transition_cases! {
    straight_to_done: (&[Op::Advance(State::Done)], Some((State::Done, None, DONE_HEADS))),
    commit_then_done_replaces_heads: (&[Op::Advance(State::Committed), Op::Advance(State::Done)], Some((State::Done, None, DONE_HEADS))),
    set_document_then_commit: (&[Op::SetDoc, Op::Advance(State::Committed)], Some((State::Committed, Some(DocId::new([3; 32])), COMMITTED_HEADS))),
    discard_pending: (&[Op::Discard], None),
    discard_committed: (&[Op::Advance(State::Committed), Op::Discard], None),
    discard_missing_is_idempotent: (&[Op::Discard, Op::Discard], None),
    done_survives_discard: (&[Op::Advance(State::Done), Op::Discard], Some((State::Done, None, DONE_HEADS))),
    advance_to_pending [should_panic(expected = "rows start pending")]: (&[Op::Advance(State::Pending)], None),
    recommit_after_commit [should_panic(expected = "advances from")]: (&[Op::Advance(State::Committed), Op::Advance(State::Committed)], None),
    commit_after_done [should_panic(expected = "advances from")]: (&[Op::Advance(State::Done), Op::Advance(State::Committed)], None),
    done_after_done [should_panic(expected = "advances from")]: (&[Op::Advance(State::Done), Op::Advance(State::Done)], None),
    advance_after_discard [should_panic(expected = "advances from")]: (&[Op::Discard, Op::Advance(State::Committed)], None),
    set_document_twice [should_panic(expected = "a pending creating row is ours")]: (&[Op::SetDoc, Op::SetDoc], None),
    set_document_after_commit [should_panic(expected = "a pending creating row is ours")]: (&[Op::Advance(State::Committed), Op::SetDoc], None),
    set_document_after_done [should_panic(expected = "a pending creating row is ours")]: (&[Op::Advance(State::Done), Op::SetDoc], None),
}

utils_rs::table_tests! {
    unresolved_cases tokio,
    (states, expected_order),
    {
        let states: &[(State, i64)] = states;
        let expected_order: &[usize] = expected_order;
        let commands = ledger().await;
        let mut rows = Vec::new();
        for (index, &(state, created_at)) in states.iter().enumerate() {
            let mut row = begin_pending(&commands, &command(&format!("cmd-{index}"))).await;
            let document = DocId::new([index as u8; 32]);
            commands.set_document(&row.command, &document).await.expect("set_document");
            row.document = Some(document);
            if state != State::Pending {
                commands.advance(&row.command, state, COMMITTED_HEADS).await.expect("advance");
                row.state = state;
                row.heads = COMMITTED_HEADS.to_vec();
            }
            sqlite::set_command_created_at(&commands.sql, created_at, row.command.as_str().to_owned())
                .await
                .expect("set creation order");
            rows.push(row);
        }
        assert_rows(
            commands.unresolved().await.expect("unresolved"),
            expected_order.iter().map(|&index| rows[index].clone()),
        );
    },
}

unresolved_cases! {
    empty_ledger: (&[], &[]),
    all_done: (&[(State::Done, 1), (State::Done, 0)], &[]),
    unresolved_in_creation_order: (&[(State::Committed, 2), (State::Pending, 1)], &[1, 0]),
    mixed_states_exclude_done_and_preserve_order: (&[(State::Pending, 30), (State::Done, 10), (State::Committed, 20), (State::Pending, 0)], &[3, 2, 0]),
}

utils_rs::table_tests! {
    purge_done_cases tokio,
    (state, age, survives),
    {
        let age: Option<i64> = age;
        let commands = ledger().await;
        let id = command("cmd-purge");
        let mut row = begin_pending(&commands, &id).await;
        if state != State::Pending {
            commands.advance(&id, state, DONE_HEADS).await.expect("advance");
            row.state = state;
            row.heads = DONE_HEADS.to_vec();
        }
        let now = now_micros();
        sqlite::set_command_timestamps(
            &commands.sql,
            now - 2 * DONE_TTL_MICROS,
            age.map(|age| now - age),
            id.as_str().to_owned(),
        )
        .await
        .expect("backdate");
        commands.purge_done().await.expect("purge");
        assert_rows(commands.get(&id).await.expect("get"), survives.then_some(row));
        if !survives {
            let replacement = begin_pending(&commands, &id).await;
            assert_rows(commands.get(&id).await.expect("get reused id"), [replacement]);
        }
    },
}

purge_done_cases! {
    fresh_done_ignores_old_creation_time: (State::Done, Some(0), true),
    recent_done_survives: (State::Done, Some(DONE_TTL_MICROS - 60_000_000), true),
    expired_done_is_purged_and_id_reusable: (State::Done, Some(DONE_TTL_MICROS + 1), false),
    pending_without_done_at_survives: (State::Pending, None, true),
    committed_without_done_at_survives: (State::Committed, None, true),
    pending_with_old_done_at_survives: (State::Pending, Some(DONE_TTL_MICROS + 1), true),
    committed_with_old_done_at_survives: (State::Committed, Some(DONE_TTL_MICROS + 1), true),
}
