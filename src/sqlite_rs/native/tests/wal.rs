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
