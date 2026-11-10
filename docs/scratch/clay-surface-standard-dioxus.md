# Clay surface: standard Dioxus inside a micro (claytxt lane, draft)

Status: draft, 2026-11-11. Item 2 of the claytxt week plan. Inputs: the operator's calls of
2026-11-10 (open `data-*`, a scoped stylesheet per mount plus the shared theme sheet, forward
`visible`, commands before paint) and 2026-11-11 (the diff view is plain Dioxus and a micro uses it
like any Dioxus component; `intxt-review-loop.md`, "Operator decisions (2026-11-11)"); W1/W2 on
`spike/pierre-scroll`; `pierre-dioxus-host.md`.

## Goal

A Dioxus component that works in a plain Dioxus app works unchanged inside a micro, as far as the
component uses standard Dioxus: elements and attributes, events, `MountedData`, and a stylesheet.
The diff view (`pierre_diffs_dioxus`) is the test case. Where a micro must differ (no egress, no
JS), the difference is a refusal the component can see, not a different API.

## Today on `feat/dayframe-shell`

| Standard Dioxus | In a micro today |
|---|---|
| any attribute | an allowlist (`clay_surface/render.rs`, `static_attribute`); `data-*` limited to `data-testid`, `data-state`, `data-value` |
| `onmounted` + `MountedData` | delivered with `null` data (`@serialize Mounted`); no scroll, reads or focus |
| `onresize`, `onscroll` | forwarded |
| `onvisible` | not forwarded |
| a stylesheet (`document::Stylesheet`, `<style>`) | none: only allowlisted inline `style` properties |

## 1. `MountedData` over the wire (land the spike)

`spike/pierre-scroll` already has the design, and W1/W2 measured it on web:
- `ElementCommand { Scroll, ScrollBy, SetFocus, Read { request, what } }` and
  `ElementRead { ClientRect, ScrollOffset, ScrollSize }` in `clay_surface::wire`.
- The guest's `GuestElement` implements `RenderedElementBacking` (`clay_mdk/guest/dom.rs`), so
  `onmounted` hands the component a real `MountedData`. Writes are fire-and-forget commands; a read
  is a command with a request id, answered by the host through the `element-reply` event, which
  the guest adapter consumes before Dioxus sees it.
- The host keeps `ElementHandles` (guest element id → the host's own `MountedData`, from the host
  element's `onmounted`) and runs each command against it.

Landing it means rebasing that onto #80 and implementing the host side for every renderer:
- **Dioxus-hosted (Wry desktop, Blitz, web through `clay_dioxus`)**: the host element's
  `MountedData` does the work. On Blitz, scroll and focus come from our dioxus fork `a3e1e68`;
  reads that borrow the document from a handler are a known Blitz panic (gap log), so the host
  issues them from a spawned task, never inside the dispatch.
- **Unsupported reads are a reply with an error**, which the guest surfaces as the standard
  `MountedError`. A component sees the same failure it would see on a renderer that can't measure.

**Ordering contract (host contract 1).** Commands a guest issues while handling an event are
applied in order, after that dispatch's mutations, before the dispatch returns to the host. On web
the dispatch is synchronous on the page's main thread, so they land before paint (W1: 0 frames
late in 2000 events). That makes the read-then-scroll correction jump-free.
- **Not achievable on Wry desktop:** Dioxus desktop's Rust side talks to the webview over IPC, so
  every `MountedData` call is an async round trip, for plain Dioxus apps too. A micro there is as
  good as Dioxus desktop and no better. The contract is stated per backend, not promised
  everywhere.

## 2. Open `data-*`

- **Wire:** the guest may send any attribute named `data-` followed by one or more characters from
  `[a-z0-9-._]` (the HTML rule plus lowercase only, which Dioxus already emits). Values are text
  and inert: no renderer fetches or runs anything because of a `data-*` value.
- **Host:** Dioxus attribute names are `&'static str`, so the host interns each new name once in a
  process-wide interner (leaked, deduplicated). The interner is bounded: past
  **4096 distinct names** in the process, a batch that introduces another one is refused with
  `PresentationError::InvalidBatch`. Pierre emits about 60 names; the bound keeps a hostile micro
  from growing host memory without limit.
- `aria-*` stays an allowlist (it's semantic and finite), and so does everything else.

## 3. A shadow root and stylesheet per mount

Operator call (2026-11-10, reaffirmed 2026-11-11): each micro mount gets a shadow root with its own
sanitized stylesheet, plus the shared theme sheet. Wry and web are the primary backends, and this
is designed for them. Blitz is not a constraint here.

**What Dioxus does today (pinned fork `a3e1e68`, `packages/interpreter/src/ts/core.ts`):** nothing
for shadow DOM. The interpreter creates and appends nodes by id with no shadow-root operation, and
it delegates bubbling events to the root listener, finding the Dioxus node from `event.target`.
An event from inside a shadow tree reaches the root retargeted to the shadow host, so the inner
element's id is lost.

**Fork patch (dman-os/dioxus, the interpreter shared by desktop and web):**
- A mount element the host marks (`data-clay-shadow`) gets `attachShadow({ mode: "open" })` when
  the interpreter creates it, and the interpreter appends that element's children into its shadow
  root instead of its light DOM. Attributes and listeners on the element itself stay on the host
  element.
- The delegated root handler resolves the target with `event.composedPath()[0]` instead of
  `event.target`. For an event that never left a shadow tree this is the same node, so plain
  Dioxus apps don't change.
- Non-composed events (`scroll`, `load`, `toggle`, …) don't cross the shadow boundary, but Dioxus
  already attaches non-bubbling listeners to the element itself, so they keep working. Verify per
  event in the patch's tests.
- The interpreter ships as generated JS (`src/js/*.js` with `hash.txt`), so the patch regenerates
  it. Upstream it if Dioxus wants it: shadow-root support is a gap for any Dioxus web-components
  story.

**Stylesheets:**
- **API:** standard `document::Style` / `document::Stylesheet` in the guest, with inline CSS text
  (an `href` to a URL is egress and is refused). The guest adapter sends it as a new mutation,
  `SetStyleSheet(String)`, once per mount; a later one replaces it.
- **Sanitize (host):** parse with a real CSS parser (`cssparser`), then drop anything that can make
  the renderer fetch: `@import`, `@font-face`, `@namespace`, and every declaration whose value has
  a `url()` token or `image-set()`, `src()` or `-webkit-image-set()`. Unknown at-rules are dropped.
  Anything URL-bearing needs an explicit capability, and none exists yet.
- **Into the shadow root:** the sanitized sheet as one `<style>` element as the root's first child.
  `:host` and `:host(...)` then work as written, which Pierre's stylesheet relies on.
- **The theme sheet:** custom properties inherit through the shadow boundary, so theme tokens reach
  every mount. Tailwind utility rules don't, so the host also puts its shared utility sheet in each
  root. Prefer one constructed `CSSStyleSheet` in `adoptedStyleSheets`, shared by every root
  (parsed once); a `<style>` copy per root is the fallback if the patch can't reach it.
- **Inline `style`:** add `overflow-anchor` to the allowed properties (host contract 3: Chromium's
  scroll anchoring is one of W2's two jump-free paths).

**Blitz, later and not blocking:** no shadow DOM there. When the native backend needs micro
stylesheets, the same sanitized sheet can be wrapped in `@scope ([data-clay-mount="<id>"]) to
([data-clay-mount])`, if Stylo supports it then, or have its selectors rewritten. That's a fallback
for one backend, not the design.

## 4. Forward `visible`

Add `visible` to the forwarded listeners, with `VisibleData` serialized as Dioxus does on web.
Blitz still panics on `onvisible` (`dioxus-native-dom` `convert_visible_data`), so the Blitz host
refuses a `visible` listener with `PresentationError::Unsupported`, which the guest sees when the
listener is added. Same for `resize` on Blitz, which panics the same way today.

## Not in this item

- A coalesced "sizes changed" event (host contract 4): wait for the X5 rerun on the real host.
- Pointer capture, `transitionend`, the focus scope: the kit's needs, not the diff view's.

## Platform primitives first (lessons from Chrome/Edge/Firefox/MDN's `modern-web-guidance`)

The operator pointed at "Get your coding agent off ~2019 JavaScript" (GoogleChrome/
modern-web-guidance, HN 2026-11). What carries over, for the primary (webview) backends:
- **Prefer a platform primitive to hand-rolled behaviour**, because it comes with keyboard handling
  and accessibility built in and needs no JS from the micro. Candidates, each to be checked against
  WebKitGTK (Wry on Linux), Android WebView and current Firefox/Safari before use:
  - `popover` with `popovertarget`, and `<dialog>` opened declaratively with invoker commands
    (`commandfor` / `command="show-modal"`): Escape, light dismiss, the top layer and focus
    handling without JS. Relevant to the kit's sheets and drawers (ledger P1) and to what the
    surface allowlist must admit (`popover`, `popovertarget`, `commandfor`, `command`, and the
    `toggle` / `beforetoggle` events).
  - CSS anchor positioning for popovers, which respects KIT-040 (nothing measures).
  - `content-visibility: auto` with `contain-intrinsic-size` as a measured alternative to some
    JS virtualization cost. Not a replacement for the honest Pierre port; a later X5 comparison.
  - `@scope` (the donut form) for any backend without shadow roots.
- **Cross-browser, not Chrome-only:** the thread's main complaint was guidance that recommends
  APIs only Chrome ships. Check every primitive against the engines we actually run.
- **Guidance vs. instruction for agents:** models take a suggestion as a command. Sonnet briefs
  and `.agents/skills` should say MUST only for real requirements and mark the rest as options.

## Build order (Sonnet, one item each, at most two at once)

1. Element commands: port the spike's wire, guest backing and host side onto #80. Tests: a guest
   component's `scroll`, `get_scroll_offset` and `set_focus` reach the host in order within one
   dispatch; an unsupported read comes back as a `MountedError`. **(Running, 2026-11-11.)**
2. `data-*` interner: grammar, interning, the 4096 bound. Tests: Pierre's 60 names pass, a bad
   name is refused, the 4097th distinct name is refused, a repeated name doesn't grow the interner.
3. `visible` + `overflow-anchor`.
4. Shadow roots: the interpreter patch in the dioxus fork (mount children into the shadow root,
   `composedPath()` targets), with tests on desktop's and web's interpreter.
5. Stylesheet: the `SetStyleSheet` mutation, the sanitizer, the `<style>` in the root and the
   shared theme sheet. Tests: insta snapshots of sanitized output for Pierre's `style.css` and a
   hostile sheet (every fetch path).

## Operator decisions (2026-11-11)

1. Real shadow roots on Wry and web, through the fork patch (section 3). Blitz doesn't block it.
2. The `data-*` interner bound starts at 4096 distinct names per process.
