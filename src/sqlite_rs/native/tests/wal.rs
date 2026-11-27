use super::*;

#[test]
fn a_connection_that_loses_the_race_to_enter_wal_waits_for_the_winner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("race.sqlite");
    // A write transaction on the still-rollback-mode file is what the
    // winner of a conversion race holds: the opener's switch reads the
    // header under it and is then refused the upgrade without a wait.
    let winner = RusConnection::open(&path).expect("open winner");
    winner
        .execute_batch("BEGIN IMMEDIATE; CREATE TABLE converted (id INTEGER);")
        .expect("winner write transaction");
    let opener = std::thread::spawn({
        let path = path.clone();
        move || ConnSlot::open(&path, &RuntimeConfig::default(), Arc::default(), true).map(drop)
    });
    std::thread::sleep(Duration::from_millis(100));
    winner.execute_batch("COMMIT").expect("winner commit");
    opener
        .join()
        .expect("opener panicked")
        .expect("the loser waits for the winner instead of failing busy");
    drop(winner);
    let mode: String = RusConnection::open(&path)
        .expect("reopen")
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("journal mode");
    assert_eq!(mode, "wal");
}

#[test]
fn concurrent_openers_converge_on_wal_without_failing_busy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("openers.sqlite");
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    ConnSlot::open(&path, &RuntimeConfig::default(), Arc::default(), true).map(drop)
                })
            })
            .collect();
        for handle in handles {
            handle
                .join()
                .expect("opener panicked")
                .expect("concurrent opener failed");
        }
    });
    let mode: String = RusConnection::open(&path)
        .expect("reopen")
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("journal mode");
    assert_eq!(mode, "wal");
}

/// Contention is told apart by SQLite's result code, never by the wording of its message.
#[test]
fn busy_and_locked_result_codes_are_a_typed_busy_error() {
    for code in [rusqlite::ffi::SQLITE_BUSY, rusqlite::ffi::SQLITE_LOCKED] {
        let error = rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None);
        assert!(matches!(SqlError::from(error), SqlError::Busy(_)), "{code}");
    }
    let constraint = rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
        Some("database is locked".into()),
    );
    assert!(
        matches!(SqlError::from(constraint), SqlError::InvalidQuery(_)),
        "a message that says locked is not contention"
    );
}
