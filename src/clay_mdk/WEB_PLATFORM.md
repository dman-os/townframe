# Web-platform/component notes

These are source-backed working notes, not an adopted browser API specification. They inform
small boundary changes; they do not replace the Dioxus implementation baseline.

## What exists and what is proposed

- **Web IDL** is maintained as a WHATWG standard. It describes web-platform interface surfaces
  and specifies how they appear and convert values in JavaScript. API-specific behavior remains
  in the individual platform specifications. It is not currently a WIT specification.
  Source: https://webidl.spec.whatwg.org/ , introduction and interface-definition language.
- **Mozilla's February 2026 article** explicitly advocates Component Model integration with
  the web platform. It discusses Wasm/ES-module integration, including proposed direct loading
  of core modules with `<script type="module">`. Its component/browser-API examples are expressly
  labelled aspirations, not tutorials or promises; hypothetical `std:web/console` does not exist
  there as an implemented browser interface. Our direction has real industry precedent, but
  component loading and native browser WIT bindings must not be claimed as shipped APIs.
  Source: https://hacks.mozilla.org/2026/02/making-webassembly-a-first-class-language-on-the-web/
- **webidl2wit** converts Web IDL into WIT. Its README labels it WIP and lists unresolved mappings
  for `any`, `object`, callback interfaces, and several other types. It maps dictionaries to
  records, unions to variants, and interfaces to resources or interfaces. This is useful prior
  art for future platform facets, not evidence that the entire DOM can be translated mechanically.
  Source: https://github.com/wasi-gfx/webidl2wit , README type-conversion table.
- **browser.wit** generates browser WIT from Web IDL and supplies browser-component examples
  in several languages. Its example instructions use Jco, JSPI, and explicit browser/WASI shim
  mappings, including a browser-global proxy and poll shim. This is a concrete polyfill path,
  separate from browser-native component integration.
  Sources: https://github.com/MendyBerger/browser.wit , README;
  https://github.com/MendyBerger/browser.wit/blob/main/examples/README.md

We have read these sources, not executed their examples or adopted their runtime implementation.

## Consequences for the current increment

1. Keep Dayframe authority separate from web-platform vocabulary. A guest gets a placement-scoped
   surface, not a global `window`/`document` proxy. Adding a familiar DOM operation must not grant
   access to other micros, application roots, network access, or unrelated platform facilities.
2. Use the existing Dioxus renderer and serialized event conventions before designing new ones.
   The current edits transport carries the pinned interpreter encoding. Event routing now uses
   element identity, name, bubbling, and the existing serialized HTML event payload, rather than
   a second listener registry and hand-designed mouse/input union.
3. Keep synchronous event delivery for default-action decisions. An asynchronous event stream
   is useful for observation but cannot retroactively cancel a browser default action. The
   converter project's `EventHandler -> stream` suggestion alone does not solve that behavior.
4. Host/guest resource lifetimes and guest-local data mirrors remain Dayframe contracts. They
   must work through different component engines; neither Wasmtime objects nor JS objects define
   their portable identity.
5. Incremental platform support needs explicit unsupported results. Blitz is not a complete
   browser. IDL-shaped signatures alone do not establish layout, focus, accessibility, event,
   or string-conversion compatibility. Web IDL's DOMString and WIT's string also should not be
   treated as automatically identical in all cases, notably lone UTF-16 surrogates.
6. A byte-stream mutation interpreter is an external-input boundary. A production adapter must
   enforce encoding compatibility, bounds and surface ownership before edits can affect the
   host renderer. The recording probe does not establish those properties or authorize scripts,
   arbitrary URL loads, or application-global node access.

The next contracts are still derived from real micro needs, not from importing the entire web
platform. A future Web IDL-derived adapter can sit above the same scoped host authority without
turning micro composition into an unrestricted browser-global object graph.
