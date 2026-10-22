# btress_auth WASI HTTP compatibility

`btress_auth` intentionally remains on WASI HTTP `0.2.6` until the local JavaScript componentization toolchain can produce a working Preview 3 component.

## Toolchain boundary

The current package versions are:

- `@bytecodealliance/jco`: `1.27.0`
- `@bytecodealliance/jco-std`: `0.1.3`
- `componentize-js`: `0.19.3` (the componentizer fallback used by the installed JCO)
- `hono`: `4.13.0`

The application imports the Hono adapter from JCO's `wasi/0.2.x` surface, and `wit/world.wit` exports `wasi:http/incoming-handler@0.2.6`. The lockfile consequently pins the relevant WASI packages to `0.2.6`.


## Reproduction

From `src/btress_auth`, the current check is:

```text
$ pnpm run typecheck
$ tsc --noEmit

$ pnpm run build
$ pnpm run build:ts && pnpm run build:component
$ rolldown -c rolldown.config.mjs
✔ rolldown v1.2.3 Finished
$ jco componentize -w wit -o dist/btress_auth.wasm dist/component.js
warning Falling back to componentize-js 0.19.3 because this component requests Preview 2 WASI packages older than 0.2.10.
thread '<unnamed>' panicked at crates/spidermonkey-embedding-splicer/src/bindgen.rs:700:47:
not yet implemented
(jco componentize) RuntimeError: unreachable
```

Thus the current P2 artifact is not silently treated as a successful P3 build: TypeScript typechecking succeeds, while componentization fails at the installed StarlingMonkey backend. The accepted resolution is to keep this service out of the P3 artifact set and fail its component build rather than ship a synchronous HTTP or SQLite shim. The WIT version remains pinned to the adapter that the application actually imports (`incoming-handler@0.2.6`).
`pnpm typecheck` passes. A normal `pnpm build` bundles the TypeScript successfully, then fails during `jco componentize` in the SpiderMonkey bindgen path with `RuntimeError: unreachable`. Replacing the world export temporarily with `wasi:http/handler@0.3.0` reaches the same componentization failure. The installed `jco-std@0.1.3` also provides only the `wasi/0.2.x/http/adapters` surface; it has no Preview 3 HTTP adapter for this Hono integration.
## Reproducible Preview 3 boundary

The P3 probe was reduced to two independent cases with the installed JCO 1.27.0:

- StarlingMonkey/componentize-js 0.22.0 fails even for a minimal world exporting only `wasi:http/handler@0.3.0`, panicking in `spidermonkey-embedding-splicer/src/bindgen.rs:865` and reporting `RuntimeError: unreachable`.
- The QuickJS backend (`componentize-qjs` 0.4.3) can produce a minimal P3 handler component, but rejects an imported `async func` (including the real `townframe:sqlite/sqlite-connection` resources) with `instance export ... has the wrong type: type mismatch with async`. Using that backend would therefore require changing the async SQLite contract, which is not acceptable.

The newest published `@bytecodealliance/jco-std` checked here (0.9.0) still contains Hono adapters only under `wasi/0.2.3`, `wasi/0.2.6`, and `wasi/0.2.12`; it has no `wasi/0.3.x` HTTP adapter. JCO's `@bytecodealliance/preview3-shim` provides experimental P3 runtime/types, but it does not solve either componentizer limitation. These are the exact blockers, so retaining `incoming-handler@0.2.6` is an explicit compatibility boundary rather than an untested version choice.


Therefore there is no supported local path to claim HTTP compatibility beyond `0.2.6`: changing the WIT version alone does not produce a component, and adding a synchronous or ad-hoc adapter would not preserve the required async HTTP semantics. The resolved boundary is to keep the WIT and Hono adapter on `0.2.6`, with this document recording the exact versions and failed P3 probe. Revisit the export only when a JCO/JCO-std/componentize-js combination with a working Preview 3 HTTP adapter is available.
