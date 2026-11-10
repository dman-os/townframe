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

## 3. A stylesheet per mount

**Change from the 2026-11-10 call:** shadow roots aren't available. Every Clay host renders through
Dioxus, which creates elements with no shadow-root operation, and Blitz has no shadow DOM at all
(`:host` never matches; gap log). So scoping is done by rewriting the micro's stylesheet, which
works the same on every backend:

- **API:** standard `document::Style` / `document::Stylesheet` in the guest, with inline CSS text
  (an `href` to a URL is egress and is refused). The guest adapter sends it as a new mutation,
  `SetStyleSheet(String)`, once per mount; a later one replaces it.
- **Sanitize (host):** parse with a real CSS parser (`cssparser`, already in the tree through
  Stylo), then drop anything that can make the renderer fetch: `@import`, `@font-face`, `@namespace`,
  and every declaration whose value contains a `url()` token or `image-set()`, `src()` or
  `-webkit-image-set()`. Allowlist rather than denylist where the grammar allows it: unknown
  at-rules are dropped. The no-egress memory's rule applies: anything URL-bearing needs an explicit
  capability, and none exists yet.
- **Scope (host):** prefix every selector with the mount's root, `[data-clay-mount="<id>"]` (an
  attribute the host sets on the mount's root element), and rewrite `:host` and `:root` to that
  root. Micro styles can't reach outside their mount.
- **The theme sheet** is the host's own global stylesheet. Its custom properties inherit into
  every mount, which is the shared theme the operator asked for, with no copy per mount. Host rules
  can still match inside a mount, as they could with shadow roots that adopt the theme sheet; the
  kit's rules all come from Tailwind utilities on the elements themselves, so this changes nothing
  for them.
- **Inline `style`:** add `overflow-anchor` to the allowed properties (host contract 3: Chromium's
  scroll anchoring is one of W2's two jump-free paths).

What a shadow root would have given and this doesn't: host styles can't be kept out of a mount,
and a micro's `id`s share the host document's namespace. Neither matters for the diff view; both
are noted for the embed-contract design.

## 4. Forward `visible`

Add `visible` to the forwarded listeners, with `VisibleData` serialized as Dioxus does on web.
Blitz still panics on `onvisible` (`dioxus-native-dom` `convert_visible_data`), so the Blitz host
refuses a `visible` listener with `PresentationError::Unsupported`, which the guest sees when the
listener is added. Same for `resize` on Blitz, which panics the same way today.

## Not in this item

- A coalesced "sizes changed" event (host contract 4): wait for the X5 rerun on the real host.
- Pointer capture, `transitionend`, the focus scope: the kit's needs, not the diff view's.

## Build order (Sonnet, one item each, at most two at once)

1. Element commands: rebase the spike's wire, guest backing and web host onto #80; add the
   Dioxus-hosted renderer's side. Tests: a guest component's `scroll`, `get_scroll_offset` and
   `set_focus` reach a fake host in order within one dispatch; an unsupported read comes back as a
   `MountedError`.
2. `data-*` interner: grammar, interning, the bound. Tests: Pierre's 60 names pass, a bad name is
   refused, the 4097th distinct name is refused, a repeated name doesn't grow the interner.
3. `visible` + `overflow-anchor`, with the Blitz refusals.
4. Stylesheet: the mutation, sanitizer and scoper. Tests: insta snapshots of sanitized, scoped
   output for Pierre's `style.css` and for a hostile sheet (every fetch path).

## Questions for the operator

1. Selector scoping in place of shadow roots (section 3): fine?
2. The `data-*` interner bound of 4096 distinct names per process: fine as a start?
