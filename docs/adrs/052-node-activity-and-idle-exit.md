# ADR 052: Node activity and idle exit

**Status:** Accepted (operator, 2026-11-24), from the operator's ask for a TTL on auto-started servers. Proposed by the intxt lane the same day. Its number collides with `052-the-node-hosts-micros` and is fixed when it lands. Open questions 1–3 stay open; their defaults are what this ADR states.

**Related:**
- [ADR 033: what a node is](033-what-a-node-is.md) (the node directory, its lock and socket, the background server)
- [ADR 038: node orientations and the relay alpha](038-node-orientations-and-the-relay-alpha.md) (which nodes must never idle out)
- [ADR 043: client pairing and operator authentication](043-client-pairing-and-operator-authentication.md)
- intxt layer 42 (`stack/intxt-42-capture-autostart`): a capture hook starts the node's server in the background when none serves.

## Context

Since layer 42, a capture hook that finds no server for its node starts one (`day node start --bg`), and that server stays up until `day node stop` or reboot. The operator wants it to go away by itself when nothing needs it: a time to live for servers nobody started on purpose. The same mechanism is what socket activation needs later: a server systemd starts on the first connection should exit when idle, and systemd starts it again on the next one (the units ship with the package; `day` only accepts the socket it is handed through `LISTEN_FDS`).

"Idle" cannot mean "no client is connected". A node that is up does work nobody is waiting on:

- **sync:** local changes not yet sent to the peers and relays it syncs with, and remote changes being pulled (`big_sync`, the keyhive pull);
- **blobs:** pins and transfers in flight (`big_blobs` pin and part workers);
- **storage upkeep:** checkpoints and frontier writes (`big_repo`'s causal checkpoint and Automerge frontier workers);
- **guests:** a cartridge's handler or routine still running (`big_nodule_wasm`), a micro's call;
- **requests:** a dispatch call in flight on a connection that is otherwise idle.

Stopping in the middle of these loses nothing that was acknowledged, since writes are durable before they are acknowledged. It does delay their effect: a recording made just before an idle exit would not reach the relay until the node next runs.

The code already has one narrow notion of idleness. A process that boots the node to run its own command (`LockRole::Command`) closes its listener when the command ends and waits until the clients it accepted are gone (`FdClients::wait_until_idle` and its HTTP twin in `big_node/listeners.rs`), then stops. That counts connections only.

## Decision

### 1. Activity is reported, not inferred

The node keeps one activity registry, created at boot and handed to every subsystem with the node's bindings. A subsystem that starts work the node should finish before it stops by itself takes a **hold** from the registry and drops it when the work is done. A hold is a guard (drop releases it), so an early return, a `?` or a panic cannot leak one. Each hold names its kind and carries a short label for display.

Kinds:

| Kind | Held while | Taken by |
|---|---|---|
| `Client` | a connection is open: the Unix socket, the descriptor socket, TCP XRPC | the listeners (today's open-connection counters become holds) |
| `Call` | a dispatch call is in flight | the router |
| `Sync` | local changes are unacknowledged by a reachable peer or relay the node is configured to sync with, or a pull is in progress | `big_sync`, the keyhive pull |
| `Blob` | a pin or a transfer is in flight | `big_blobs` |
| `Upkeep` | a checkpoint, compaction or frontier write is running | `big_repo` |
| `Guest` | a cartridge handler or routine, or a micro's call, is running | the nodule executor, the micro host |

Standing loops are not holds: pollers, janitors, listeners, peer dial loops and the keyhive pull loop never finish, and they are how a node waits. A loop takes a hold only for each unit of work it starts.

Work that cannot progress releases its hold. Unacknowledged changes for a peer that is unreachable are not a `Sync` hold, since the node would never become idle on a laptop that is offline. The changes are sent the next time the node runs and the peer answers.

### 2. Idle is no holds for the whole idle period

A node is idle when the registry has had no hold for the configured period, continuously: any hold taken restarts the clock. The registry exposes this as a future the server awaits, and as a snapshot (current holds by kind and label, time since the last release) for display.

### 3. Who exits when idle

- `day node start --idle-exit <duration>` exits after that much idleness. Without the flag, a server started on purpose runs until stopped, as today.
- A server a capture hook starts (layer 42) passes `--idle-exit` with the default period, 10 minutes until measured otherwise.
- A server started by socket activation (`LISTEN_FDS` set) idles out by default, since systemd starts it again on demand.
- Nodes whose orientation makes them always-on (a relay, ADR 038) never idle out, whatever they are passed.
- An app that holds its node in-process (the desktop and phone shells) lives as long as its window; it ignores idleness.

### 4. Exiting without a race

On idle, the server:

1. stops accepting: it closes its listeners and, unless the socket came from systemd, unlinks its socket file;
2. checks the registry once more, because a client that connected just before the listener closed holds a `Client`, and that client is served and waited for;
3. stops the node.

A client that lost the race finds no socket, or a refused connection, and takes the layer 42 path again: it starts a server and retries once. Under socket activation there is no race: systemd keeps the socket, queues the connection, and starts the service.

### 5. Visible

`day node status` lists the current holds by kind and label and, for a server that idles out, the time left until it does. The server logs, at `info`, when it exits for idleness and how long it was idle, and, at `debug`, which hold last reset the clock. A node that never idles out is a hold to find in `status`, not a mystery.

## Consequences

- Each subsystem that does finite work has to take holds for it. A forgotten hold does not lose data, since writes are durable before they are acknowledged; it lets the node stop with the work unfinished, which delays its effect until the next run. Starting with `Client`, `Call`, `Sync` and `Blob` covers what a recording node does. `Upkeep` and `Guest` follow as their owners add them.
- Most of the sources are nodules' crates (`big_sync`, `big_blobs`, `big_repo`, `big_nodule_wasm`). The micro host is claytxt's. intxt builds the registry's use in `day` (the flag, the hook's default, status) and the client's retry.
- The registry is plain counters and labels with no engine types, so the browser host can use it too.

## Alternatives considered

- **No connected clients means idle.** It is what the in-process command holder does today, and it stops a server with unsent changes the moment the last hook disconnects. Rejected.
- **Each subsystem answers "anything pending?" when asked.** Pull-based probes have to be kept consistent with the work they describe, and they race with work that starts just after the answer. Guards cannot be forgotten on an error path, and a hold taken just before the exit check is seen by it. Rejected in favour of holds.
- **Leave it to systemd.** systemd can stop an idle service only if the service reports idleness, which is this ADR's question. It also does not exist on macOS or in the apps.
- **A fixed time to live since start.** Simple, but it cuts off work in flight just as readily. Rejected.

## Open questions

1. The default idle period: 10 minutes is a placeholder.
2. Does a hold need a maximum age, after which it is logged as stuck and ignored, so one wedged transfer cannot keep a laptop's node up forever?
3. Should a recording that has not reached a relay hold the node a little longer, or is "sent at the next run" acceptable for intxt?
