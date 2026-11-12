# ADR 043: Client pairing and operator authentication

**Status:** Proposed (manager, 2026-11-11), from ledger D42 and D43. To be reviewed with the operator. Sequenced with ADR 037 step (b), channel identity.

**Related:**
- [ADR 022: authority, actors, capabilities and grants](022-authority-actors-capabilities-and-grants.md)
- [ADR 037: holder-bound capabilities and channel identity](037-holder-bound-capabilities-and-channel-identity.md)
- [ADR 042: byte bodies and local-only methods](042-byte-bodies-and-local-only-methods.md)

## Context

Today the node's Unix socket admits only the uid the node runs as. Every request from that uid without a bearer token is the operator (`big_node/listeners.rs`: `Anonymous::Acts(operator)`). `day` in daemon mode, intxt's Claude Code plugin, and any script run by the user all hold the operator's full authority by default.

The operator rejects this (D42, D43):
- Nobody programming against the node should default to the operator. Code written that way doesn't port to nodes with stricter subsystems.
- The CLI is nobody by default. Being the operator takes access to a protected secret: the system keyring, or sudo-level access.
- A client such as the plugin identifies itself, waits for the operator's approval, then holds a persistent identity. The plugin is a subsystem actor under its app's actor (intxt's), not an agent under the operator.

On a single-user machine, all of these processes share a uid. The keyring is the only boundary between them, and it is imperfect, which is accepted. What this buys is that the default is unprivileged and every privilege was consented to.

## Decision

### 1. Same uid is admitted, but as nobody

The Unix socket still admits only the node's uid; other uids are closed. An unauthenticated request from that uid is **anonymous**, with no authority. It may call only methods that need none: pairing requests (§3) and the node's public status. `Anonymous::Acts(operator)` goes away from the socket listener.

### 2. The operator is whoever can read the operator credential in the keyring

At `day node init` the node creates an **operator key pair** and stores the private key in the system keyring, next to the wrapping key it already keeps there (`big_cabinet.authority` / `operator.<node id>`). The public key is bound to the operator actor.

A client authenticates as the operator by signing a request token with that key (§4). A `day` invocation that can't read the keyring entry is not the operator.

In-process mode is consistent with this. A `day` that boots the node itself must already unwrap the node's secrets with the keyring's wrapping key, so it has the access the operator credential requires. It acts as the operator by construction.

### 3. Pairing: a client asks, the operator approves

1. **Request.** The client generates a key pair, keeps the private key in its own keyring entry, and calls `pairing.request { name, publicKey, parent }`. `parent` names the app actor it claims to belong to, such as intxt's. The method is local-only (ADR 042) and callable anonymously. The node records a **pending** request and returns a short code for the client to show its user.
2. **Approval.** The operator sees pending requests (`day pairing ls`; later a dayframe prompt) and approves one, confirming the short code when the client shows one (`day pairing approve <code>`). Approval is privileged. Approving opens a new actor under `parent` (ADR 037 a0's introduction), binds the client's public key to it, and writes the durable grants the operator chooses (ADR 037 §5).
3. **Use.** From then on the client authenticates with its key (§4). Its capabilities are minted from its grants, per connection, and bound to it as holder (ADR 037 §2).
4. **Revocation.** Deleting the key binding ends the identity, and deleting the grants ends the authority. Both are operator actions.

Pending requests expire, and the node caps how many it holds, so anonymous callers can't fill the table.

**Delegated approval (operator, 2026-11-12).** The operator isn't the only approver. A client that holds an approved identity can approve requests for subsystem identities under its own actor, the same way the keyring operator approves. For example, intxt's approved identity approves its Claude Code plugin as a subsystem of intxt. This is ADR 037 a0's introduction rule: a holder may introduce only live strict descendants of its own actor, needs `DELEGATE` to do so, and can grant only what it holds, narrowed. So approval authority is scoped by the actor tree. The operator approves top-level clients, and each approved client approves its own subsystems without asking the operator.

Precedents: the OAuth device authorization grant (RFC 8628: request, show a code, approve elsewhere, poll); ssh `authorized_keys` (a key bound to an identity, no shared secret); Bluetooth pairing (a numeric comparison code); polkit `auth_admin_keep` (approve once, keep).

### 4. How a client proves its key on each request

A client sends `Authorization: Bearer <token>`. The token is a short-lived JWT signed by the client's key, with `iss` = its actor, `aud` = the node id, and `exp` a few minutes out. This is atproto's inter-service auth (a JWT signed by the caller's key, with `aud` naming the service). Only the edge transport verifies it, and the verified actor becomes the call's channel identity, which is ADR 037 (b). A hop never inherits it (ADR 042 §3). The client's signer never enters a guest (D39).

Precedent check: atproto service auth also binds `lxm` (the method NSID) into the token. Binding the method too, so a captured token can't be replayed against another method, is cheap and follows that precedent.

### 5. Where a client's key lives and how `day` picks it

Most clients call the node through `day` (intxt's capture is `day x intxt capture`, run by a hook). So `day` holds the keys, not each client. A paired identity is a named keyring entry on the user's machine (`day.identity` / `<node id>.<name>`, for example `intxt/claude-code`). `day` signs with the identity named by `--as <name>` or the `DAY_AS` environment variable, which a hook sets once. That's the kubectl-context / `gh auth` shape. Pairing from `day` (`day pairing request --as intxt/claude-code --parent <intxt actor>`) generates and stores the key. Without `--as`, `day` is the operator if it can read the operator entry, and nobody otherwise.

A client that talks to the socket directly, not through `day`, keeps its own key and signs its own tokens (§4).

### 6. Landing order

§1 (same uid is nobody) lands in the same change as §2–§5, never before. Otherwise every same-uid client loses its authority before it has a way to get it back, including capture's alpha loop.

### 7. Identity is not authority

The key binding says *who* is calling and nothing else. Every authority comes from grants, as it does today. A paired client with no grants can call only what anonymous callers can.

## Consequences

- `big_node::listeners` drops the operator default. `day` reads the operator key from the keyring and signs its requests; without it, `day` commands that need authority fail with a message naming the keyring entry.
- intxt's plugin pairs once and is then a subsystem actor under intxt with explicit grants. The planned act-as env var (`<operator>/agents/<harness>/<session>`) is not built.
- Tests that relied on the anonymous-is-operator socket now authenticate as the operator explicitly.
- The `day` blackbox tests also have to clean up the new keyring entry, as they already do for the node's existing entries (`big_node::delete_secrets`).

- Signing must stay cheap for short-lived callers: intxt's capture runs `day` from hook processes many times a minute. Each call is one keyring read (a Secret Service lookup on an already unlocked collection, no prompt) plus one signature. If the read shows up in measurements, the precedent is an agent that holds unlocked keys (ssh-agent), and in daemon mode that's the node's own process. Measure before adding one.

## Open questions

1. Settled 2026-11-12: the parent approves its own subsystems (delegated approval, §3). The operator's approval is needed only for identities directly under the operator.
5. **Naming (operator, 2026-11-12: "pairing might be the wrong noun").** Pairing is two devices bonding as peers. This is a client asking an authority for an identity, which prior art calls **enrollment**: X.509 certificate signing requests and EST ("Enrollment over Secure Transport", RFC 7030), Kubernetes `CertificateSigningRequest` (approve/deny), SPIFFE/SPIRE registration and attestation, and MDM device enrollment. Proposed: `enrollment.request`, and `day enroll …` to request; `day enrollments ls|approve|deny|revoke` to decide. The ADR is renamed to match once the operator agrees.
2. Approval UX on a headless node, such as a relay: `day pairing approve` only, or also approval from a sister node?
3. Token lifetime and whether the token binds the method (`lxm`).
4. Should anonymous callers see anything besides pairing and node status?
