//! What `Inline` promises beyond what every executor does.

use super::*;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

/// Poll `future` once and return what it produced. Panics if it is pending: under `Inline`
/// nothing a statement does may wait, so a pending poll means a thread hop crept back in.
fn ready<F: Future>(future: F) -> F::Output {
    match std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("an inline future was not ready on its first poll"),
    }
}

fn inline_db(migrations: &[Migration<'static>]) -> test_support::TestDb {
    test_support::db_on(Execution::Inline, migrations).expect("test db")
}

/// No executor of any kind: a test that is not even `async` runs the whole database.
#[test]
fn every_call_is_ready_on_its_first_poll() -> SqlResult<()> {
    let db = inline_db(MIGRATIONS);
    ready(db.migrate())?;
    let mut tx = ready(db.begin_write())?;
    let inserted = ready(tx.execute("INSERT INTO items (name) VALUES ('a') RETURNING id", ()))?;
    assert_eq!(inserted.result_set.one::<i64>()?, 1);
    {
        let part = ready(tx.savepoint("part"))?;
        ready(part.execute("INSERT INTO items (name) VALUES ('b')", ()))?;
        ready(part.rollback())?;
    }
    ready(tx.commit())?;
    let snapshot = ready(db.begin_read())?;
    ready(db.execute("INSERT INTO items (name) VALUES ('later')", ()))?;
    let names = ready(snapshot.query("SELECT name FROM items", ()))?.all::<String>()?;
    assert_eq!(names, vec!["a"]);
    ready(snapshot.rollback())?;
    assert_eq!(
        ready(db.query("SELECT COUNT(*) FROM items", ()))?.one::<i64>()?,
        2
    );
    ready(db.drain_workers());
    Ok(())
}

#[test]
fn a_dropped_transaction_is_rolled_back_before_drop_returns() -> SqlResult<()> {
    let db = inline_db(MIGRATIONS);
    let tx = ready(db.begin_write())?;
    ready(tx.execute("INSERT INTO items (name) VALUES ('pending')", ()))?;
    assert_eq!(db.inner.writer_gate.available_permits(), 0);
    drop(tx);
    assert_eq!(db.inner.writer_gate.available_permits(), 1);
    let tx = ready(db.begin_write())?;
    assert!(
        ready(tx.query("SELECT name FROM items", ()))?
            .rows
            .is_empty()
    );
    ready(tx.rollback())?;
    Ok(())
}

/// The gate's one real wait: a writer asks while another writer holds the connection, and
/// proceeds the moment that one finishes, with nothing but the polling thread to do it.
#[test]
fn a_waiting_writer_proceeds_when_the_holder_finishes() -> SqlResult<()> {
    let db = inline_db(MIGRATIONS);
    let holder = ready(db.begin_write())?;
    let mut waiting = Box::pin(db.begin_write());
    let mut context = Context::from_waker(Waker::noop());
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    ready(holder.commit())?;
    let Poll::Ready(second) = waiting.as_mut().poll(&mut context) else {
        panic!("the waiting writer was not admitted after the holder committed");
    };
    ready(second?.rollback())?;
    Ok(())
}

/// There is no background task on `Inline`, so idle connections are closed by the next lease.
#[test]
fn idle_connections_close_when_the_next_lease_is_taken() -> SqlResult<()> {
    let runtime = runtime_on(Execution::Inline);
    let idle = runtime.ephemeral(MIGRATIONS).expect("ephemeral");
    let busy = runtime.ephemeral(MIGRATIONS).expect("ephemeral");
    ready(idle.execute("INSERT INTO items (name) VALUES ('kept')", ()))?;
    assert_eq!(runtime.open_connection_count(), 1);
    std::thread::sleep(Duration::from_millis(120));
    ready(busy.execute("INSERT INTO items (name) VALUES ('other')", ()))?;
    // `idle`'s connection was closed by the sweep that came with `busy`'s lease.
    assert_eq!(runtime.open_connection_count(), 1);
    let names = ready(idle.query("SELECT name FROM items", ()))?.all::<String>()?;
    assert_eq!(names, vec!["kept"]);
    Ok(())
}

#[test]
fn the_capability_flag_and_executor_are_reported() {
    let db = inline_db(&[]);
    assert!(db.supports_concurrent_readers());
    assert_eq!(db._runtime.execution(), Execution::Inline);
}
