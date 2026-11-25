# Protocol headers catalog

Status: research note, nothing decided; the proposals are marked as such
Owner: nodules lane
Updated: 2026-11-24
Scope: every HTTP header our protocols define or read, and the nodule specifics `service_core_rs` carries

Research note, no code moves. Every HTTP header our protocols define or read, found by grepping
the workspace at the tip of `stack/node-76-cleanup` (consts named `*_HEADER`, `http::header::*`
constants and header string literals) across `service_core_rs`, `service_client_core_rs`,
`service_client_native_rs`, `service_utils_rs`, `big_dispatch`, `big_node`, `big_nodule*`,
`big_ndk*`, `day_cli`, `native_utils_rs`, `mltools` and `btress_sysadmin`. `clay_*` crates define
and read no HTTP headers (their `header` hits are HTML elements). Tests and fixtures are not
listed as readers.

Protocol key: **XRPC** = HTTP/XRPC (also carried over the Iroh XRPC transport and the in-process
envelope), **dispatch** = `big_dispatch`'s `Invoke` plus `HttpMetadata`, **guest** = the
host to Wasm guest boundary (a `wasi:http` request the host delivers and `big_ndk` serves),
**fd** = the descriptor socket (`service_core_rs/descriptor.rs`), which carries no HTTP headers at
all. Its frame fields (`operation`, `parameters`, `fd_count`) take the place of the HTTP context
headers, and the listener fills `HttpMetadata.authorization` itself with the operator credential
(`big_node/listeners.rs`).

## Context headers (defined in `service_core_rs/xrpc.rs`)

The XRPC server reads them with `service_core_rs::xrpc::request_context`; clients write them with
`service_client_core_rs::xrpc::write_context_headers`.

| Header | Defined | Set by | Read by | Protocol |
| --- | --- | --- | --- | --- |
| `x-request-id` (`REQUEST_ID_HEADER`) | `service_core_rs/xrpc.rs:9` | `write_context_headers` (every XRPC client: `service_client_native_rs`, `big_ndk` outgoing calls, `big_nodule::wire::delivery_headers`); `service_utils_rs` router and `big_ndk/serve.rs` echo it on responses | `request_context` (router, `big_ndk::cx::from_headers`); `big_ndk/serve.rs` | XRPC, guest |
| `traceparent` (`TRACEPARENT_HEADER`) | `service_core_rs/xrpc.rs:10` | `write_context_headers` from `RequestContext::trace` | `request_context` into `TraceContext::from_headers` | XRPC, guest |
| `tracestate` (literal, no const) | `service_core_rs/xrpc.rs` (`request_context`), `service_core_rs/context.rs:48,86`, `service_client_core_rs/xrpc.rs` | `write_context_headers` | `request_context`, `TraceContext::from_headers` | XRPC, guest |
| `authorization` (`AUTHORIZATION_HEADER`), value `Bearer <token>` | `service_core_rs/xrpc.rs:11` | clients (`bearer_header_value`: day_cli, `service_client_native_rs`, `big_ndk` as the nodule's capability, `big_nodule::wire` as the nodule's id); `big_dispatch` forwards `caller.authorization.header_value()`; the fd listener sets the operator credential | `request_context` into `HttpMetadata::authorization`; `big_dispatch/xrpc.rs` (`bearer_token`) | XRPC, dispatch, guest |
| `idempotency-key` (`IDEMPOTENCY_KEY_HEADER`) | `service_core_rs/xrpc.rs:12` | `write_context_headers` from `HttpMetadata::idempotency_key` | `request_context` into `HttpMetadata::idempotency_key`; the methods that honour it | XRPC, dispatch |
| `x-timeout-ms` (`TIMEOUT_HEADER`) | `service_core_rs/xrpc.rs:13` | `write_context_headers` from the remaining `RequestContext` budget | `request_context` (becomes the time budget); the guest transport `big_ndk/wasi.rs:109` (a guest has no timer, so the host's first-byte timeout is this value) | XRPC, guest |
| `range` (literal) | `service_core_rs/xrpc.rs:114`, `service_client_core_rs/xrpc.rs:223` | `write_context_headers` from `HttpMetadata::range`; `day_cli/client.rs:379,403` (`http::header::RANGE`) | `request_context` into `HttpMetadata::range` (`ByteRange::from_header`) | XRPC, dispatch |

## Nodule delivery headers (defined in `service_core_rs/xrpc.rs`)

All six are set by the host in `big_nodule::wire::delivery_headers` and read by
`big_ndk::cx::from_headers`. `NODULE_CAPABILITY_HEADER` is also set by `big_ndk` on a guest's
outgoing calls (`big_ndk/cx.rs:343`) and read back by `big_nodule/egress.rs`, which is how the
host knows which nodule is calling out. `big_nodule/e2e/executor/routines.rs` asserts it.
Guest boundary only; no other protocol carries them.

| Header | Defined | Meaning |
| --- | --- | --- |
| `x-nodule-capability` (`NODULE_CAPABILITY_HEADER`) | `service_core_rs/xrpc.rs:16` | The nodule's own dispatch capability as a bare id, set on every delivery to a nodule that was granted methods |
| `x-nodule-config-doc` (`NODULE_CONFIG_DOC_HEADER`) | `:18` | The nodule's cartridge config document id |
| `x-nodule-config-capability` (`NODULE_CONFIG_CAPABILITY_HEADER`) | `:20` | Capability to read the config document and write declared settings datums |
| `x-nodule-drawer-capability` (`NODULE_DRAWER_CAPABILITY_HEADER`) | `:22` | The drawer capability for the routine's granted drawer set, when present |
| `x-nodule-blobs-capability` (`NODULE_BLOBS_CAPABILITY_HEADER`) | `:24` | The blobs capability the routine's methods need (ADR 049 §6.2), when present |
| `x-nodule-actor-capability` (`NODULE_ACTOR_CAPABILITY_HEADER`) | `service_core_rs/xrpc.rs` | The nodule's own actor identity capability (`OpenedActor.capability`), for `OpenActor.parent` and `AttenuateCapability`; set on every delivery whose handles were minted |

## Response metadata (`ResponseMetadata::write_headers` / `read_headers`, `service_core_rs/xrpc.rs:131-185`)

Literals, no consts. Written by `write_headers` on the server (`service_utils_rs` router) from
`ResponseMetadata`, read by `read_headers` on the client (`service_client_*`). Protocol: XRPC.

| Header | Notes |
| --- | --- |
| `etag`, `location`, `cache-control`, `retry-after` | one value each |
| `warning` | zero or more, appended |

## Standard HTTP headers the protocols rely on

| Header | Set by | Read by | Protocol |
| --- | --- | --- | --- |
| `content-type` | clients for JSON bodies (`service_client_core_rs/xrpc.rs:264` `application/json`, `day_cli/client.rs:312,373,408`, `big_ndk/cx.rs:272`), the server for answers (`service_utils_rs/xrpc/router.rs:802`, `big_ndk/serve.rs:86,93`), `big_nodule_wasm` for guest egress (`lib.rs:598,723`) | `service_utils_rs/xrpc/router.rs:734,986` (also rejects duplicate values), `service_client_native_rs/xrpc/{client,iroh}.rs`, `big_ndk/wasi.rs:183,195,263,267`, `big_nodule_wasm/lib.rs:619,684,745,765`, `day_cli/client.rs:384,421` (the mime of a byte body) | XRPC, guest |
| `content-length` | `router.rs:808,1112` and `big_ndk/serve.rs:95` for byte answers, `big_nodule_wasm/lib.rs:601,726` | `router.rs:741` (`ByteBody::with_length`), `big_ndk/wasi.rs:199,271`, `big_nodule_wasm/lib.rs:624,770`, `native_utils_rs/downloader.rs` | XRPC, guest |
| `content-range`, `accept-ranges: bytes` | `service_utils_rs/xrpc/router.rs:806-814` on a ranged byte answer (`206`) | `day_cli/client.rs:385,422` | XRPC |
| `connection`, `upgrade: websocket`, `sec-websocket-key`, `sec-websocket-accept` | `router.rs:955-960` (server accept); `service_client_native_rs/xrpc/iroh.rs:388` sets `connection: close` | `router.rs:847-857` | XRPC event streams |
| `authorization`, `cookie` marked sensitive | `btress_sysadmin/server.rs:66` (tower `sensitive_headers`, so tracing redacts them) | tracing | the sysadmin server (not a nodule protocol) |
| `authorization` for a model download | `mltools/lib.rs:677` (reqwest, an API token) | upstream host | outside our protocols |

## Not found

`atproto-proxy` (and the other `atproto-*` request headers) appear nowhere in the code or ADRs;
only atproto's Lexicon, error-name and data-model conventions are referenced. If proxying
between services is ever added, it would live next to the context headers above. Likewise no
code reads `accept`, `if-*`, `user-agent`, `host`, `origin` or `x-forwarded-*`.

## Everything else `service_core_rs` knows about nodules

The crate is the shared base of every XRPC server and every guest, yet it carries these
nodule and host-boundary specifics. The proposal puts each where the other half of the contract
already lives. `big_nodule_core` is the host-neutral nodule model, `big_ndk` is the guest side.

| What | Where now | Proposed home | Why |
| --- | --- | --- | --- |
| The five `NODULE_*_HEADER` consts | `service_core_rs/xrpc.rs:14-24` | A `headers` module in `big_nodule_core`, with `big_nodule` (host) and `big_ndk` (guest) both importing it. Today's only two users are those, plus `big_nodule/egress.rs` | They name capabilities of the nodule model (config doc, drawer, blobs) and are read by no other protocol |
| `DOMAIN_ERROR_FIELD` (`"operationError"`) and `DOMAIN_ERROR_STATUS` (400), `domain_error_envelope` | `service_core_rs/xrpc.rs:25-26,72-84` | Stays in `service_core_rs`, but the field name should follow the "operation" to "method" rename in the XRPC docs. It is an XRPC convention shared by the router, the clients, dispatch and `big_ndk` (and `big_nodule/egress.rs` maps refusals onto it), not a nodule one | Listed because it reads as nodule-specific but is not |
| The descriptor frame (`descriptor.rs`: `DescriptorBody`, `[u32 header_len]...` framing, `SCM_RIGHTS`) and `Body::Descriptors` in `operation.rs:54` | `service_core_rs/descriptor.rs`, `operation.rs` | A `descriptor` crate or module owned by the node listener side (`big_node`) with `service_client_native_rs/fd.rs` as its client. Only `big_node/listeners.rs`, `service_client_native_rs/fd.rs` and `service_utils_rs/lib.rs` use it | It is the local-only call transport of ADR 042, not part of the XRPC contract a guest sees (a guest never holds descriptors) |
| `bearer_header_value` / `bearer_token` and the `Bearer ` scheme | `service_core_rs/xrpc.rs:28-42` | Stays | Shared by every XRPC server and by guests, as its doc says |
| The doc wording tying the context headers to nodules ("set by the host on every delivery to a nodule that was granted methods") | the header consts' docs | Moves with the consts | |

Open points: whether `big_ndk` and `big_nodule` should both depend on `big_nodule_core` just
for the header names (`big_ndk` is a guest and currently depends only on `service_core_rs`), and
whether the guest-to-host calls that `big_nodule/egress.rs` identifies by
`x-nodule-capability` should use the `authorization` bearer instead, which would drop one header.
These are design questions and are not decided here.
