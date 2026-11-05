# Pierre spikes

Owner: dayframe
Scope: Pierre diff viewer port, micro-fit blockers (P1 style lane)
Status: evidence and options only; no decision
Updated: 2026-11-05

## P1 style

Question: what is the smallest honest way for a micro to render Pierre's DOM contract with Pierre's CSS, on the web build?
Branch `spike/pierre-style`, commits 5cf07402..3d111d56 (probe, caps, option a, option b, option c) over de603884 (spike micro).
Everything below was measured in Chromium 156 against `dayframe_web` (wasm-release host, debug wasm32-wasip3 guest); Blitz was not run.
Evidence for a claim is a command or a file:line; "unverified" otherwise. Screenshots, pixel diffs and logs are in the lane scratchpad
(`.../scratchpad/p1/out/{a,b,c}.{shell,ref,diff}.png`, `{a,b,c}.json`), not in the repo.

**Result in five lines.**
1. Pierre's stylesheet cannot be delivered or matched by the wire today: no `data-*`, no stylesheet channel, and the 217-row batch exceeds three caps.
2. The wire is already more open than its allowlist reads: the plain `style` attribute is accepted and applied as text (probe, below), so custom properties and any CSS already cross.
3. All three prototypes render real Pierre's rows pixel-identically (0 of 2,909,808 pixels differ, max channel delta 0) at 668x4356.
4. Pierre's `useCSSClasses` mode is dead code in 1.5.2 and `style.css` has no class selectors, so "class mode" is a rewrite we own, not a Pierre feature.
5. The binding cost is not style: mounting 217 rows takes ~20-25 s through the wcmp web bridge (16,972 mutations), whatever option delivers the CSS.

### 1. What Pierre 1.5.2 emits

Sources: `~/repos/rust/intest/vendor/pierre_repo/packages/diffs/src` (1.5.2; the earlier research found it identical to tag `diffs-v1.5.2`, not re-checked here; the browser runs use npm `@pierre/diffs@1.5.2`'s `dist`). Measured on a real `FileDiff` render of a 217-row unified diff (`expandUnchanged`, `pierre-dark`/`pierre-light`)
serialized from Chromium 156 (`dump-dark.json`; fixture generator in `src/dayframe_web/e2e/tools/pierre-ref/`).
Citations are `src/`-relative.

**Tags** (rows only: `pre`, `code`, `div`, `span`; 1 + 1 + 436 + 1088 in the measured render, plus 1088 text nodes).
The rest of the viewer adds `slot` (`utils/createFileHeaderElement.ts:45,73,119`, `renderers/UnresolvedFileHunksRenderer.ts:331`),
`style` (`utils/createStyleElement.ts:16,31`; `utils/hostTheme.ts:30`, `utils/createUnsafeCSSStyleNode.ts:4`), `bdi`
(`createFileHeaderElement.ts:87,110`), `button` (`utils/createGutterUtilityElement.ts:7`), `br` (`renderers/FileRenderer.ts:562`,
`DiffHunksRenderer.ts:672`), `svg`+`use` (`utils/hast_utils.ts:61-65`). No `template` element in 1.5.2 (grep, non-test, non-react).

**Attributes.** Everything structural is a `data-*` attribute. Emission sites: pre wrapper `utils/createPreElement.ts:13-46`
(`data-diff|file`, `data-diff-type`, `data-overflow`, `data-disable-line-numbers`, `data-background`, `data-indicators`),
gutter `utils/hast_utils.ts:92,106-108,116,132-134` (`data-gutter`, `data-line-type`, `data-column-number`, `data-line-index`,
`data-line-number-content`, `data-gutter-buffer`, `data-buffer-size`), content column `utils/createContentColumn.ts:13`
(`data-content`), code column `renderers/DiffHunksRenderer.ts:2083-2099` (`data-code`, `data-container-size`, `data-unified`),
line `utils/processLine.ts:20-24` (`data-line`, `data-alt-line`, `data-line-type`, `data-line-index`), header
`createFileHeaderElement.ts:35-169`, interaction state set at runtime `managers/InteractionManager.ts:1744-1832` (`data-selected-line`).
The measured rows carry 17 distinct names: `data-diff, -indicators, -background, -diff-type, -overflow, -code, -unified,
-container-size, -gutter, -line-type (434x), -column-number (217x), -line-index (434x), -line-number-content (217x), -content,
-line (217x), -alt-line (161x), -diff-span (24x)`. Across the source there are ~80 distinct `data-*` names (grep of non-react,
non-editor TypeScript), 78 of which `style.css` selects on.

**Inline styles.** 850 of the 2614 nodes carry `style`: 847 token spans with two custom properties each
(`--diffs-token-dark:#..;--diffs-token-light:#..`; snapshot `test/__snapshots__/FileRenderer.test.ts.snap:411`; 12 distinct pairs in the
measured file), the `pre` with `--diffs-min-number-column-width-default:Nch` (`createPreElement.ts:42`), and `grid-row: span N`
(`hast_utils.ts:135`, `createContentColumn.ts:14`; `min-height: calc(N * 1lh)` on buffers). Runtime inline writes outside the render
path (Virtualizer/CodeView/managers, not rows): `contain` (`components/File.ts:1882`, `CodeView.ts:889,892,1204`), `isolation`, `overflow-anchor`
(`CodeView.ts:893,1197`), `position: sticky`, `display: contents` (`FileDiff.ts:2628`), `--diffs-column-*-width`
(`managers/ResizeManager.ts:447-473`), `height`/`top`/`bottom`/`margin-*`.

**Stylesheet.** `style.css` is 1,843 lines, delivered to a shadow root via `adoptedStyleSheets` (`components/web-components.ts:13-27`)
plus a per-theme `<style data-theme-css>` (449 bytes for `pierre-dark`: `:host{color-scheme:dark;--diffs-dark:..;...}`). It has
**0 class selectors and 0 id selectors** (grep); it selects with 397 attribute selectors (`[data-x]` and `[data-x='v']`, never another
operator; 78 names, 39 name=value pairs), and 2 `:host` rules (`style.css:4,305`). It uses `@layer base, theme, rendered, unsafe`
(`:1`), CSS nesting with `&:where(...)`, `light-dark()` (12), `color-mix(in lab, ...)` (19), `lh` units, `contain` (3), `container-type`
(1) / `cqi`, `scrollbar-gutter` (2), `grid-template-columns` (12), `::-webkit-scrollbar` (4). 126 distinct `--*` custom properties, of
which the `--diffs-*-override` set is the documented theming surface (`style.css:40-76`), set by the embedding page on the host element.

**Class mode.** `useCSSClasses` (`types.ts:434`) reaches `createTransformerWithState(useTokenTransformer, useCSSClasses)`
(`utils/createTransformerWithState.ts:23-26,82-83,105`), but both render paths call it with one argument:
`utils/renderDiffWithHighlighter.ts:486-488` and `utils/renderFileWithHighlighter.ts:46`. **Measured: with `useCSSClasses: true` the
rendered `<pre>` is identical (850 `style` attributes, zero `class` attributes).** Even where it does apply, it only turns the
token `style` into `hl-<n>` classes (Shiki `transformerStyleToClass`, prefix `hl-`) and nothing consumes the generated CSS
(`getCSS` is never called in `src/`); it touches none of the structural `data-*` attributes `style.css` selects on.

### 2. What `clay_surface` and the host reject, and why it was narrow

Citations are the base commit 33c7dcb9 (`git show 33c7dcb9:<path>`), before this lane's edits.

| Pierre needs | Wire today | Where | Failure |
|---|---|---|---|
| `data-*` (~80 names) | only `data-testid`, `data-state`, `data-value` | `clay_surface/render.rs:95-127` (`static_attribute`, name list at `:118`) | `Unsupported("attribute name or namespace")`, `tree/validate.rs:332-336` |
| per-token `style="--diffs-token-dark:..;--diffs-token-light:.."`, `style="grid-row: span N"` | `style` (raw) is allowed; `style:<prop>` namespace takes a 75-property list | `render.rs:99,130-144` | **raw `style` passes (see below)**; `style:--x` rejected |
| `contain`, `isolation`, `overflow-anchor`, `--*` via `style:` namespace | not in the property list | `render.rs:130-144` | `Unsupported` |
| tags `pre`, `code`, `div`, `span` | allowed | `render.rs:266-277` | none |
| `style`, `slot`, `bdi`, `svg > use`, `button` (utility) | `style`/`slot`/`bdi`/`use` not in the tag list | `render.rs:266-277` | `Unsupported("element tag or namespace")`, `validate.rs:291-294` |
| a stylesheet at all | no channel | n/a | n/a |
| one batch of 217 rows | 4096 mutations / 256 KiB / 128K work | `tree.rs:84-94` | `Unsupported("batch mutation limit")` (measured, below) |

A failed `apply` marks the writer revoked (`clay_dioxus/lib.rs:85-89`): the whole surface is withdrawn and the host shows
"Component unavailable: ...", so one rejected name loses the presentation, not the attribute.

**Raw `style` is already an open channel.** `"style"` is in the plain-attribute list (`render.rs:99`). The Dioxus web interpreter
applies the plain `style` attribute with `node.setAttribute("style", value)` after saving and restoring the element's existing
property styles (`dioxus interpreter/src/ts/set_attribute.ts:67-84`; the `style` namespace is the per-property
`style.setProperty` path at `:11-13`). The property list at `render.rs:130-144` therefore bounds only the per-property form;
a guest that writes the `style` attribute as text can set any property, any custom property, and `url()` values. Verified by the
probe presentation (`probe-raw-style`, spec `a guest's raw style attribute is applied unfiltered`, below). The same holds for `class`:
any class the host page defines (the shell ships Tailwind) is nameable.

**Why narrow.** The code states two reasons. (1) A technical one: Dioxus tag and attribute names are `&'static str`, and the host
projection names tags through `rsx!` macro sites (`render.rs:238-277`), so every accepted name must be enumerable
("ingress validation asks this module whether a name is projectable, so a batch can never carry a name the renderer cannot emit",
`render.rs:1-3`). (2) A policy one, in `clay_mdk/WEB_PLATFORM.md:35-38` (consequence 1: "a guest gets a placement-scoped surface,
not a global `window`/`document` proxy; adding a familiar DOM operation must not grant access to other micros, application roots,
network access, or unrelated platform facilities") and `:52-56` (consequence 6: the mutation stream "is an external-input boundary ...
must enforce encoding compatibility, bounds and surface ownership before edits can affect the host renderer"). ADR 015 and ADR 018
do not discuss the attribute vocabulary (grep: no "attribute", "allowlist" or "vocabulary" hit in either about the wire; 015:47
"the manifest *is* the allowlist" is about capabilities). The caps in `tree.rs:84-94` are not argued anywhere I found.

### 3. Prototypes

One spike micro, `src/micro_pierre_spike` (guest-only, wasm32-wasip3; sidecar emitted by its `emit_manifest` example). It embeds rows as data
(`assets/rows.*.json`, a hast tree dumped from real Pierre) and walks them into Dioxus; no Pierre logic. Rows: a real `FileDiff` of
`iterateOverDiff.ts` (190 lines, ~30 edits, `expandUnchanged`), unified, dark: 217 rows, 2,614 nodes in `<pre>`. Each option is a Settings
section of the micro, opened in the shell (`x/pierre-spike-assets.ts` derives fixtures and sheets from `style.css` and the dump).

| commit | option | what it is |
|---|---|---|
| 5cf07402 | probe | `probe-raw-style` presentation; clay_surface test pinning raw `style`; wire-cost test in the micro crate |
| 3d17e9b6 | caps | spike values for batch mutations (32K), bytes (4 MiB), work (4M); two clay_surface tests made cap-relative |
| e6d8c3af | a | `data-*` and `--*` coinable; `contain`/`isolation`/`overflow-anchor`; guest emits Pierre's DOM verbatim; sheet = static host asset (`?pierre-css=verbatim`) |
| c5601117 | b | manifest `stylesheets` member -> host vets, wraps surface in `[data-clay-scope]`, injects `@scope`d `<style>`; guest emits classes only |
| 3d111d56 | c | same class markup as b; sheet = static host asset (`?pierre-css=classes`), `pd-` prefix as the namespace |

Findings that shaped the prototypes:
- **b and c share markup because Pierre's CSS has no class selectors.** The brief's "guest emits class names only" needs a mechanical
  rewrite: `[data-x]` -> `.pd-x`, `[data-x='v']` -> `.pd-x--v` (397 selectors, 78 names, 39 name=value pairs, specificity unchanged),
  `--diffs-token-*` pairs -> `hl-N` (12 classes for this file/theme), `--diffs-min-number-column-width-default` -> `pd-ncw-N`,
  `:host` -> `:scope` (b) or `.pd-host` (c). Attributes no selector names (`data-line-index`, `data-alt-line`) are dropped. b/c are therefore a
  delivery axis (capability vs static) over one markup, a is the other markup over static delivery; verbatim markup + capability delivery (a+b) is the unbuilt cell.
- **Scoping without shadow DOM (b).** The host wraps the surface in `div[data-clay-scope="<micro>/<publication>"]` and emits
  `@scope ([data-clay-scope="..."]) { <sheet> }`. Chromium 156 accepts `@layer` blocks nested in `@scope`, `:scope` is the wrapper (the old
  `:host`), and a probe element outside the wrapper with the same classes stayed unstyled (`scopetest.mjs`, scratch). Vetting is blunt
  (`clay_dioxus/style.rs`: balanced braces outside comments/strings so a rule cannot close the scope, no backslash, no `url(`/`image-set(`/
  `@import`/`@font-face`/`@scope`...); 2 unit tests incl. hostile sheets. A production host needs a CSS parser.
- **Layer names collide in light DOM.** Pierre declares `@layer base, theme, rendered, unsafe`; the shell's Tailwind uses layers named `base` and
  `theme` too, and layers of one name merge across sheets. In a and c the computed `font-family` of every sampled element differed from real
  Pierre (the shell's `pre, code` rule won; Pierre's list is `"SF Mono", Monaco, ...`) while pixels were identical only because this machine
  resolves both stacks to the same font; in b (sheet in the body, later in document order) every sampled property matched. I did not isolate the
  cause beyond document order; it is the shadow-root isolation Pierre's CSS assumes, and it is a theming-fidelity risk (R2) on any machine whose fonts differ.
- **Inline custom properties are the dynamic channel.** Pierre's theme (`--diffs-*-override`, `color-scheme`) and per-file values reach the DOM as
  custom properties; a (and c, via a guest wrapper with raw `style`) can set them per render, b's manifest sheet is static per install (token
  and column-width classes are a finite closed set, themes are not).

### 4. Per option: parity, surface, size, Blitz

Parity method: the shell's `<pre>` element screenshot vs real Pierre (`FileDiff`, same diff, plain page, `#root` width set to the shell's 668 px,
page background set to the shell's) cropped to the same size, compared per pixel in the browser (`pierre-style-parity.ts`). Also computed
style of 7 sample elements (first/added/deleted row, gutter cell, token, code column, pre) over 19 properties.
Mount time: in-page `performance.now()` from the section button's `click()` to a MutationObserver seeing all 217 rows plus one animation frame;
cold = first open (guest fetch, compile, start), warm = 20 reopen cycles. Debug guest; the three runs were not quiet (b and c overlapped native builds/tests).

| | a widened vocabulary | b host stylesheet | c class contract |
|---|---|---|---|
| pixels differing vs real Pierre | 0 / 2,909,808 (max delta 0) | 0 / 2,909,808 (0) | 0 / 2,909,808 (0) |
| computed style vs real Pierre | `font-family` differs (7/7 samples), rest identical | identical (7/7) | `font-family` differs (7/7), rest identical |
| mutations for 217 rows | 16,972 | 15,927 | 15,936 |
| cold first open (ms) | 27,090 | 20,186 | 22,513 |
| warm remount median / p95 (ms), n=20 | 25,382 / 39,922 | 21,923 / 23,662 | 19,288 / 20,903 |
| wire change | `render.rs` +51/-3, tests +42, snapshot +6; interner (process-wide, 512 x 64 B) | none in clay_surface; `clay_dioxus` +165 (style.rs) +29, `clay_mdk/engine.rs` +20; manifest `extra` member | none (micro + static asset) |
| host/tooling change | static css + `?pierre-css` loader in `x/dayframe-web.ts` | none | static css in `x/dayframe-web.ts` |

Caps (all options): at the committed caps the batch fails with `batch mutation limit` for a, b and c (measured: `Unsupported("batch mutation limit")`).
Lifting one cap at a time: after mutations, `batch work limit` (128K; needs between 2M and 4M, quadratic in sibling count: the `code` column has
217 children and every append scans the parent's children), then `batch byte limit` (256 KiB; passes at 2 MiB, fails at 1 MiB); retained bytes (1 MiB) not reached.

Mount time is not style cost. A CPU profile of one a-mount (20.8 s; CDP Profiler, 5 ms sampling) is dominated by the wcmp web bridge:
`WebStore::call_host` 1.9 s self, wasm-to-js 1.3 s, `Calls::direct`/`invoke_in` 0.8 s each, `wrapper::results`/`params` ~1.3 s, `from_js` 0.4 s, `(program)` 3.3 s.
That is per-mutation marshalling across the component boundary (16,972 calls ~ 1.2 ms each); b's/c's slightly lower figures follow from ~1,000 fewer mutations
and are within the noise of unquiet runs. This is lane 3's finding to confirm; it means no option here is interactive at Pierre's window size until batching changes.

**Hostile guest, what each option gives.**
- Already today (probe, spec `a guest's raw style attribute is applied unfiltered`, passes): a guest sets `position:fixed;width:100vw;height:100vh` and a
  `background:url(...)` the page then requests (route-intercepted; the request fired). Source: `style` in the plain list (`render.rs:99` at 33c7dcb9) and
  `set_attribute.ts:67-84`. `class` names any host class (Tailwind). So the `style:` property list is not a boundary.
- a: adds `data-*` and `--*` names. New gains: attribute selectors in the host page's CSS and its `[data-*]` hooks become reachable (any host stylesheet keyed on a
  `data-` attribute can be triggered; the shell's current sheets were not audited), and a global interner a guest can fill (512 names) so other micros' coined names are rejected
  (reject = surface revoked). `contain`/`isolation`/`overflow-anchor` are inert next to what raw `style` already allows.
- b: adds a package-supplied CSS channel. A sheet can restyle anything inside its scope (and, via inheritance/`:scope`, the wrapper) but the vet keeps it from
  closing the scope or fetching; the vet is a blunt scanner, not a parser, and is the new attack surface. The sheet is admission-time data (reviewable before the guest runs).
- c: no wire change and no new channel; the cost is that the host build must carry every micro's sheet statically (reviewed in the host repo), and `pd-` is a convention, not isolation.

**What a later Blitz backend needs** (source reading in the cargo registry only; nothing was run on Blitz):
- Attribute selectors: `blitz-dom 0.3.0-beta.4` implements `attr_matches` (`src/stylo.rs:441`), so a is plausible; c's class selectors are the safest case.
- Inline `style` text and custom properties: handled by `flush_style_attribute` on `style` attribute changes (`src/mutator.rs:362`).
- `@scope` (b): Stylo 0.22 parses it only if pref `layout.css.at-scope.enabled` (`stylo-0.22.0/stylesheets/rule_parser.rs:296`); whether Blitz enables it: not checked. A b fallback is nesting or per-rule selector prefixing done by the host at admission.
- `<style>` as a host-rendered child: not checked on Blitz.
- Pierre's CSS features Blitz must support regardless (not checked): `light-dark()`, `color-mix(in lab)`, `lh`, `container-type`/`cqi`, nesting with `&:where()`, `@layer`.
- Light-DOM isolation (layer-name merge with host sheets) applies to every backend.

### 5. Options and costs (no decision)

| | a | b | c |
|---|---|---|---|
| Pierre markup fidelity | verbatim (oracle rows compare 1:1) | rewritten (normalizer needed; drops `data-line-index`, `data-alt-line`) | rewritten (same as b) |
| New wire/host surface | name families + interner | stylesheet capability + vet + scope wrapper | none |
| Theming (R2, `--diffs-*-override`) | per-render inline custom properties work | static per install; dynamic themes need a runtime channel or finite theme classes | same as b, via host build |
| Widens what ADR 015/WEB_PLATFORM.md narrowed | yes (names), though raw `style` already open | yes (CSS channel) | no |
| Isolation between micros | none (global sheet, shared attribute names) | `@scope` per presentation | prefix convention only |
| Version coupling | sheet in host build | sheet in package | sheet in host build |
| Unbuilt | | verbatim + capability (a+b), runtime `set-stylesheet` for themes | |

Independent of the choice: decide whether raw `style` stays open (closing it breaks a's token spans and Pierre's `grid-row`, which would then use the `style:` namespace),
the caps (lane 3), and the layer-name collision with Tailwind.

Reproduce: `x/dayframe-web.ts` serves the site; `playwright test pierre-style --workers=1` (4 tests pass); `node e2e/tools/pierre-style-parity.ts --ref <pierre-ref dist> --out <dir>`
(reference built from `e2e/tools/pierre-ref` with `npm i && node build.mjs`). Native: `cargo nextest run --config 'resolver.feature-unification="selected"' -p clay_surface -p clay_dioxus -p micro_pierre_spike` (48 pass).
