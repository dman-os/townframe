//! Correlated command outbox extracted from `BigSyncMachine`.
//!
//! The existing machine keeps `cmds: VecDeque<(Uuid, Command, Option<CursorIndex>, PeerId)>`;
//! the driver peeks the front command, executes it, and reports success by id
//! via `handle_cmd_success`, which pops the front entry and panics unless the
//! reported id matches — "cmds must be performed serially". The attached
//! metadata (cursor + peer) is returned on completion so the machine can
//! route completion events (e.g. cursor-lane settlement).
//!
//! This module extracts exactly that contract with no I/O and no runtime
//! dependency.

use std::collections::VecDeque;

use utils_rs::prelude::Uuid;

/// A queued command awaiting execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingCmd {
    id: Uuid,
}

impl PendingCmd {
    pub fn id(&self) -> Uuid {
        self.id
    }
}

/// Ordered command buffer with completion correlation.
#[derive(Debug)]
pub struct Outbox<Cmd, Meta = ()> {
    queue: VecDeque<(PendingCmd, Cmd, Meta)>,
}

impl<Cmd, Meta> Default for Outbox<Cmd, Meta> {
    fn default() -> Self {
        Self {
            queue: Default::default(),
        }
    }
}

impl<Cmd, Meta> Outbox<Cmd, Meta> {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<Cmd, Meta> Outbox<Cmd, Meta> {
    /// Enqueue a command; returns its correlation token.
    pub fn push(&mut self, cmd: Cmd, meta: Meta) -> PendingCmd {
        let pending = PendingCmd { id: Uuid::new_v4() };
        self.queue.push_back((pending, cmd, meta));
        pending
    }

    /// The front command, if any — what the driver should execute next.
    pub fn front(&self) -> Option<(PendingCmd, &Cmd)> {
        let (pending, cmd, _) = self.queue.front()?;
        Some((*pending, cmd))
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Iterate the queued commands in order without touching the queue.
    /// Introspection for tests and diagnostics — execution still goes
    /// through `front`/`complete` and stays strictly serial.
    pub fn iter(&self) -> impl Iterator<Item = (PendingCmd, &Cmd, &Meta)> {
        self.queue
            .iter()
            .map(|(pending, cmd, meta)| (*pending, cmd, meta))
    }

    /// The queued commands in order (test/diagnostic view).
    pub fn queued(&self) -> impl Iterator<Item = &Cmd> {
        self.queue.iter().map(|(_, cmd, _)| cmd)
    }

    /// Drain every queued command in order (test helper: asserts the exact
    /// command sequence a reducer emitted).
    #[cfg(test)]
    pub fn drain(&mut self) -> Vec<(PendingCmd, Cmd, Meta)> {
        self.queue.drain(..).collect()
    }

    /// Report successful execution of the front command. Returns the command
    /// and its metadata for completion routing.
    ///
    /// Panics on an unknown id or an out-of-order report — preserving
    /// `handle_cmd_success`'s invariants:
    /// - "success for a cmd that wasn't sent"
    /// - "unexpected cmd success, cmds must be performed serially"
    pub fn complete(&mut self, id: Uuid) -> (Cmd, Meta) {
        let found_id = self
            .queue
            .front()
            .map(|(found, _, _)| found.id)
            .expect("success for a cmd that wasn't sent");
        if id != found_id {
            panic!("unexpected cmd success, cmds must be performed serially");
        }
        let (_, cmd, meta) = self
            .queue
            .pop_front()
            .expect("success for a cmd that wasn't sent");
        (cmd, meta)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn introspection_views_do_not_disturb_the_queue() {
        let mut outbox: Outbox<Cmd, ()> = Outbox::new();
        outbox.push(
            Cmd::PersistCursor {
                part: "a",
                cursor: 1,
            },
            (),
        );
        outbox.push(Cmd::WriteStore, ());

        // queued()/iter() observe without mutating; front is untouched.
        assert_eq!(outbox.queued().count(), 2);
        assert_eq!(outbox.iter().count(), 2);
        assert!(matches!(
            outbox.front(),
            Some((_, Cmd::PersistCursor { .. }))
        ));
        assert_eq!(outbox.len(), 2);

        // drain() yields the exact ordered command sequence.
        let drained = outbox.drain();
        assert_eq!(drained.len(), 2);
        assert!(matches!(drained[0].1, Cmd::PersistCursor { .. }));
        assert!(matches!(drained[1].1, Cmd::WriteStore));
        assert!(outbox.is_empty());
    }

    #[derive(Debug, Clone, PartialEq)]
    enum Cmd {
        PersistCursor { part: &'static str, cursor: u64 },
        WriteStore,
    }

    #[test]
    fn commands_execute_serially_and_report_metadata() {
        let mut outbox = Outbox::new();
        let a = outbox.push(
            Cmd::PersistCursor {
                part: "p",
                cursor: 3,
            },
            ("peer-a", Some(3u64)),
        );
        outbox.push(Cmd::WriteStore, ("peer-a", None));

        // driver peeks the front, executes, reports success by id
        let (pending, cmd) = outbox.front().expect("queued");
        assert_eq!(pending.id(), a.id());
        assert_eq!(
            cmd,
            &Cmd::PersistCursor {
                part: "p",
                cursor: 3
            }
        );

        // completion returns the command + metadata for event routing
        let (cmd, (peer, cursor)) = outbox.complete(a.id());
        assert_eq!(
            cmd,
            Cmd::PersistCursor {
                part: "p",
                cursor: 3
            }
        );
        assert_eq!((peer, cursor), ("peer-a", Some(3)));

        // then the next one becomes the front
        assert!(matches!(outbox.front(), Some((_, Cmd::WriteStore))));
        assert_eq!(outbox.len(), 1);
    }

    #[test]
    #[should_panic(expected = "unexpected cmd success, cmds must be performed serially")]
    fn out_of_order_completion_panics() {
        let mut outbox: Outbox<Cmd, ()> = Outbox::new();
        outbox.push(Cmd::WriteStore, ());
        outbox.push(Cmd::WriteStore, ());
        // completing the second while the first is still front violates the
        // serial-execution invariant (`handle_cmd_success`).
        let bogus_second = PendingCmd { id: Uuid::new_v4() };
        outbox.complete(bogus_second.id());
    }

    #[test]
    #[should_panic(expected = "unexpected cmd success, cmds must be performed serially")]
    fn genuine_out_of_order_completion_panics_without_consuming_head() {
        let mut outbox: Outbox<Cmd, ()> = Outbox::new();
        outbox.push(Cmd::WriteStore, ());
        let second = outbox.push(Cmd::WriteStore, ());
        // Completing with the second command's real id while the first is
        // still front is the genuine out-of-order case — and must not pop
        // the head before panicking.
        outbox.complete(second.id());
    }
}
