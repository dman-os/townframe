# Windows bring-up

Status: scratch list (operator, 2026-11-06). Collected on Linux and delivered later as one PR on a Windows machine.

## Node transport and admission
- No local transport rule yet. `big_node` serves its router over a Unix socket and admits the operator through `SO_PEERCRED` (`OwnerOnly`), with a 0700 socket directory. None of that exists on Windows.
- Candidate: a named pipe with a security descriptor that admits only the current user, plus a check of the client's process token (`GetNamedPipeClientProcessId` → `OpenProcessToken` → user SID). AF_UNIX exists on Windows 10+, but it has no peer-credential API.
- `XrpcClient::unix` needs a named-pipe counterpart.
- The socket-directory rules (0700, refuse a 0755 parent, replace a stale socket, refuse a live one) need Windows equivalents: an ACL on the pipe, and a probe for an existing live pipe.

## Secrets
- `secrets_rs` wraps `keyring_core`. Check its Windows Credential Manager backend: entry size limits and the behaviour when nobody is logged in interactively.

## Builds and tests
- No Nix on Windows: document a plain `cargo` toolchain setup (nightly pin, wasm32-wasip3 target for the nodule test guest).
- `./x/*.ts` scripts assume systemd cgroups and a FIFO jobserver (`x/agent-build.ts`); decide what runs on Windows.
- `big_node` e2e tests that use the UDS listener (`e2e::listeners`, `e2e::dispatch`) need the named-pipe path.
- Paths: anything that builds paths by string concatenation or assumes `/` separators; temp-dir cleanup under Windows file locking (open SQLite files cannot be deleted).

## Later
- Windows service management for the node daemon (the CLI's daemon start/stop), the counterpart of a systemd user unit.
