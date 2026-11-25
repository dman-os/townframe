//! The outbox's tests: a `law_N_...` per law of the [parent module](super), the machine against
//! its naive oracle, and the driver over a memory table, with a deterministic simulation of
//! the whole.

use std::sync::LazyLock;

use super::*;
use crate::kit::scheduler::Instant;

mod bench;
mod differential;
mod driver;
mod laws;
mod sim;
mod table;
mod world;

pub(super) fn obj(n: u8) -> ObjKey {
    ObjKey::new(&[n])
}

pub(super) fn peer(n: u8) -> PeerKey {
    PeerKey::new(&[n])
}

pub(super) fn identity(n: u8) -> ByteKey {
    ByteKey::new(&[n])
}

/// A want with the given version at `identity(version)`, advertised by `providers`.
pub(super) fn want(version: u64, queued_at: u64, priority: u8, providers: &[u8]) -> Want {
    Want {
        version,
        queued_at,
        priority,
        identity: identity(version as u8),
        providers: providers.iter().map(|&p| peer(p)).collect(),
    }
}

static T0: LazyLock<Instant> = LazyLock::new(Instant::now);

pub(super) fn t(secs: u64) -> Instant {
    *T0 + Duration::from_secs(secs)
}

/// The machine, a clock, and every command it has issued.
pub(super) struct Rig {
    pub outbox: Outbox,
    pub now: Instant,
}

impl Rig {
    pub(super) fn new(config: Config) -> Self {
        Self {
            outbox: Outbox::new(config),
            now: t(0),
        }
    }

    pub(super) fn rows(&mut self, rows: &[(u8, Option<Want>)]) {
        self.outbox
            .rows(rows.iter().map(|(n, row)| (obj(*n), row.clone())));
    }

    pub(super) fn flush(&mut self) -> Vec<Command> {
        let mut out = vec![];
        self.outbox.flush(self.now, &mut out);
        out
    }

    pub(super) fn end(&mut self, call: CallId, ended: Ended) {
        self.outbox.fetch_ended(self.now, call, ended);
    }
}

/// The fetches among `cmds`, as (object, provider) with their call ids.
pub(super) fn fetches(cmds: &[Command]) -> Vec<(CallId, u8, u8, Version)> {
    cmds.iter()
        .filter_map(|cmd| match cmd {
            Command::Fetch { call, job } => Some((
                *call,
                job.obj.as_bytes()[0],
                job.provider.as_bytes()[0],
                job.version,
            )),
            _ => None,
        })
        .collect()
}

pub(super) fn cancels(cmds: &[Command]) -> Vec<CallId> {
    cmds.iter()
        .filter_map(|cmd| match cmd {
            Command::Cancel { call } => Some(*call),
            _ => None,
        })
        .collect()
}

pub(super) fn lands(cmds: &[Command]) -> Vec<(u8, Version)> {
    cmds.iter()
        .filter_map(|cmd| match cmd {
            Command::Land { obj, version } => Some((obj.as_bytes()[0], *version)),
            _ => None,
        })
        .collect()
}

pub(super) fn quick() -> Config {
    Config {
        max_fetches: 4,
        max_in_flight: 8,
        min_backoff: Duration::from_secs(2),
        max_backoff: Duration::from_secs(16),
        aging: Duration::from_secs(60),
    }
}

pub(super) const DURABLE: Ended = Ended::Fetched(Fetched::Durable);
