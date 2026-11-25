# Overnight 2026-11-18 (claytxt lane)

Status: report of the night; decisions pending the operator.
Owner: claytxt lane; operator reviews.
Updated: 2026-11-18
Scope: The phone app and what landed overnight 2026-11-18.

What landed while you slept, where it is, and the decisions waiting for you. Test results are filled
in as the mothership runs finish (see "Verification").

## The phone app

The intxt review renders on the phone, and the app now runs in the Android System WebView (Wry) by
default, as you decided. Branch `feat/phone-viewer-app` (pushed, tip `7552c98d`).

What you'll see on the phone:
- Phone layout (360 px wide), no empty band under the status bar.
- System back steps back through the shell (Wry's own handler, which closed the app, is off).
- Micros are styled: `#89` (`feat/clay-standard-dioxus`, adopted shared sheets in micro shadow roots)
  is merged into the phone branch. Blitz hid that this was missing.
- Pages scroll: new kit component `clay_dioxus_kit::PanelPage` (app bar pinned, body scrolls).
  Home, Settings and Layout use it. The intxt lane is wrapping the review screens in it.
- Review lists PR #179; opening it shows the PR body.

Why scrolling never worked: a micro's presentation root is exactly its pane's size with
`overflow: clip` (the Clay mount contract), so a micro must scroll its own body, and none did. Found
by inspecting the live WebView over DevTools (`adb forward` to `webview_devtools_remote_<pid>`).

Fixes on the way, all on the phone branch:
- `big_repo` keyhive storage: Android's SELinux denies `link()`, so files publish with
  `renameat2(RENAME_NOREPLACE)`; one lock for every publish. Reviewed and approved by nodules, with a
  200-round race test.
- `dayframe_mobile`: survives activity recreation.
- `clay_mdk` `Context::call` from a Dioxus resource: it trapped (no component-model task under
  `render_immediate`), then hung (no render pass after the answer). Both fixed, pinned by the
  `clay_node_egress` e2e.
- `dayframe_node`: mirrors every drawer granted to a micro. A plain reader node fetches only drawers
  it mirrors, so the read grants alone never moved any data (nodules found the root cause).
- Android sizing: tao's Android window reports a scale of 1.0 and includes the status bar; the phone
  reads the WebView's size and the density itself (`Viewport` source).

Known, not fixed:
- The intxt card under a PR said "Intxt cannot be read": the review micro's doc capability covered
  the intxt drawer's member documents but not the drawer's own document (its descriptor). Fixed on
  the phone branch (`760cef9c`: the interim capability also reads each granted drawer's document);
  that build is installed but I couldn't check it on screen (the phone left our app). Nodules then
  made a drawer reach cover its own document (node-55, `f6795553`), so drop the workaround once the
  phone base is on node-55.
- A grant made while the phone is connected reaches it only after an app restart. Nodules is
  building one merged grant feed (`awaitCartridgeGrants`) tonight.
- The source node picks a random UDP port and relays are off, so a source restart strands the phone
  until it gets the new ticket. Nodules landed `listenPort` in node.json (#190); the intxt source
  should pin it.
- A fresh install starts the package micros disabled; enable layout and review in Settings → Micro
  lifecycle.
- The scroll offset survives navigating back (a page opens part-scrolled).
- Installs: the phone has ~3 GB free, so a 340 MB debug APK can't install in place. Every reinstall is
  an uninstall, so the node gets a new identity and needs new grants each time.

## The web shell (from the intxt lane's new dayframe_web e2e loop)

- **CSP: no `<style>` elements in shadow roots any more.** The web shell's CSP
  (`style-src-elem 'self'`) refused every inline `<style>`, so pierre's diff styles and every micro's
  own sheet were blocked on the web. The dioxus fork (`1696518a`) now takes a shadow root's own sheet
  from its host's `data-dioxus-shadow-sheet` attribute and adopts it as a constructable sheet; pierre
  and Clay's mounts use it. Blitz (no shadow roots) keeps the light-DOM `<style>`, marked by
  `LightDomStylesheets`, which the Blitz app provides. No CSP change. Pierre also gathers its theme
  `<style>` (it sits below the shadow root, and Pierre names elements in upper case). Verified by the
  intxt lane's `review.spec` on the mothership: PR list, PR view and the intxt flow with the diff
  rendered, asserting zero console errors.
- `wasi:random` for guests in the browser runtime (taken from the intxt lane, with the guest's
  requested length bounded so a guest can't panic the host).
- In progress: a `wasi:http` host for the browser runtime that refuses every request, so a micro
  that calls the node (review) loads on the web and shows its error state instead of faulting.

## Settled work done as lanes

- **Holder crate** (`feat/holder-crate`): `day` and the apps share one find-or-hold-the-node path,
  now in `day_node` (not a new crate; `day_node` was already the shared node-directory crate). The
  phone takes the node lock without serving a socket (Android is always the holder, §1); the new API
  is `NodeDir::try_lock_unserved`.
- **Cartridge micros** (`feat/cartridge-micros`, on phone branch + nodules' grants stack): the node
  host loads a cartridge's micro from the catalog and mints its handles from `uses` ∩ the cartridge's
  grants, through one lowering shared with nodule routines (`big_nodule::lower_uses`). `review.json`
  and `INTERIM_GRANTS` stay only for micros in no cartridge. intxt is publishing intxt as one
  cartridge (nodule + review micro) to this shape.
- **Small items** (`feat/micro-small-items`): the showcase launcher passes `--hot-patch false`; a guest
  stylesheet or attribute that can make the host fetch (`url()`, `@import`, `src`/`href` to a URL)
  refuses the batch. Only fetches refuse, at any depth; rules the sanitizer drops stay dropped.

## Decisions for you

1. **Android end state.** Tonight's port is Dioxus mobile: Rust UI in the app process, the page in
   the WebView, so it pays per-edit IPC like the Wry desktop. The consistent end state is the
   desktop option of `clay-on-the-node.md` §6 applied to Android: the WebView runs the web build of
   the shell (wasm in the page) and talks to the in-process node through a bridge. Noted in §6 as
   undecided.
2. **Cartridge micros** (from the lane):
   1. `package_version` is the envelope's major version, so intxt at `0.1.0` gives 0. Is 0 valid, or
      should the mapping change?
   2. A cartridge micro replaces a bundled package with the same name (`did:example:clay-local/…`
      versus the publisher DID). Is name equality the rule?
   3. How a cartridge micro gets its docs/drawers handles: attached by the host on each call (ADR 049
      §3), or the interim `docCap` config datum?
   4. A micro that fails to load is an installation error and the boot continues. Or fail the boot?
   5. Expose the cartridge's settings document to its micro now?
3. **`review.json` and grants on the phone** stay until intxt's cartridge carries the micro; then
   the phone switches on its own.
4. **Design-language gaps** the intxt lane hit while polishing review (it used the nearest tokens):
   semantic success/warning roles and a danger container (PR state chips, unreadable rows); a
   line-clamp utility (WebView and Blitz); kit-level `Alert` and `Chip` (local in
   `micro_review/view.rs` now) and a mono font token. intxt is bringing these to your GUI session.
5. **`PanelPage`** is new kit surface (`clay_dioxus_kit`): an app bar pinned over a scrolling body,
   because a micro's mount root clips at its pane's size. Every panel micro should use it.

## Verification

- Phone, by hand over adb, in the WebView build: layout, back, styling, scrolling (Settings → Micro
  lifecycle to the last button), Review lists #179, PR body opens.
- `clay_node_egress` e2e: 3/3 pass with the `Context::call` fixes; the same tests timed out before the
  last fix (they pin the hang).
- `big_repo` keyhive tests: 63/63, including `mixed_path_publishes_keep_the_first_writer`.
- Mothership runs in progress at the time of writing: phone branch (`dayframe_node`,
  `clay_node_egress`, `clay_surface`, `micro_layout`, `clay_dioxus_kit`), cartridge branch, holder
  branch. Results below.

Results:
- Phone branch on the mothership (`a7559eea`): 237/238 across dayframe_node, clay_node_egress,
  clay_surface, clay_dioxus, micro_layout, clay_dioxus_kit, clay_rt_wcmp, micro_shell,
  pierre_diffs_dioxus, micro_review. The one failure was the test environment (the intxt guest wasm
  not at its conventional path; mbx prunes a managed target's output between build and test). With
  the guest copied to a stable path and `INTXT_GUEST_WASM` set, that test passes too: 238/238.
- pierre_diffs_dioxus 33/33 after the shadow-sheet change, with the outline snapshot re-blessed.
- dayframe_web `layout.spec` 3/3 locally on the phone tip; intxt's `review.spec` and `shell.spec`
  pass on a merge of it.
- The laptop's NTFS build disk refuses to link the 1.5 GB debug `libmain.so` below roughly 30 GB
  free (`EINVAL`). Lane worktrees take 10 to 15 GB each; delete a lane's target when it's done.
- The mothership's disk filled (my test worktrees, about 30 GB); earlier "empty" test results were
  "no space left on device" aborts. Freed, and runs now share one worktree.
