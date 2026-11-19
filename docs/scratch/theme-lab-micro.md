# Theme lab micro

Status: Slice 1 built (CSS text editor, preview, Apply, host-wide live application,
interim storage). Slice 2 (Automerge Text datum in a theme cartridge's drawer) not started.
Owner: claytxt lane
Scope: the end user's theme editor as a micro of the daytxt app (`micro_theme_lab`), and the
host seam it applies through.
Updated: 2026-11-18

The theme lab used to be a demo inside `dayframe_showcase` (`ThemeRoot`, `ThemeMode`,
`material-theme.json`): a page of specimens one reads, with a light/dark toggle. It becomes an
end-user micro, like Telegram's theme editor, and the home for iterating on the design
language. A theme is one stylesheet of CSS text, edited as text in a plain monospace
`textarea` (no visual or custom editors), previewed on the micro's own surfaces, and applied
to the whole app live, with no restart.

## The layers

A mount's cascade, lowest precedence first. The shell's own document (light DOM) follows the
same order.

1. **Baseline / kit.** Every stylesheet of the host document (Tailwind with the generated
   token variables, the kit's utilities) is flattened by the Dioxus fork's interpreter into
   one constructable sheet inside `@layer host` and adopted into every mount's shadow root
   (`packages/interpreter/src/ts/core.ts`, `rebuildHost`; it watches `<head>`). The fork
   declares `@layer host, user;` first, so the order of those two layers is fixed whatever
   order their rules arrive in. Layered rules lose to every unlayered rule.
2. **Micro.** The micro's own stylesheet, sanitized by `clay_surface` and set on the mount as
   the `data-dioxus-shadow-sheet` attribute of the mount root (`clay_dioxus::MountedSurface`),
   adopted after the kit. It is unlayered, so it outranks the kit.
3. **End-user theme.** `@layer user { ... }`, appended to that same adopted sheet by
   `MountedSurface` from the `clay_dioxus::UserStylesheet` context. It outranks the kit
   (and so Tailwind, the tokens' utilities and any host-wide rule) and loses to a micro's own
   unlayered rules.

Decision to confirm (design question): the operator's wording was "a user sheet last". This
build puts the theme *in a layer* above the kit rather than unlayered after the micro. The
reason is the lab itself: the lab previews its draft as its own micro stylesheet, and with an
unlayered user sheet last the already-applied theme would beat the draft in the lab's own
mount, so editing a property the applied theme sets would show no change. The cost: a micro's
own unlayered rules are not themeable. Micros are styled almost entirely by kit utilities
(layer `kit`), so the theme reaches nearly everything; a micro that hard-codes its own
colours in its own sheet keeps them. Switching to "theme beats micro" is one line in
`MountedSurface` (drop the `@layer user` wrapper) plus an exemption so the lab's own mounts
do not take the applied theme (the preview would otherwise lose to it).

## How a theme is applied host-wide

The applied theme is stored in a configuration facet of the shell
(`micro_shell_manifest::ShellTheme`, value `ThemeSheet { css }`, binding `shell-theme`), in
the same durable document store as every other configuration (the `documents` subspace of the
app's SQLite store), so it survives a restart. The shell `Frame` (`micro_shell/view.rs`,
`use_theme`) observes the facet and, once per change:

- checks the CSS against the fetch rule (below) again, since a facet is a synced document and
  may have been written elsewhere; an unchecked sheet is not applied;
- renders it into the host's own document as a `<style>` of `@layer user { ... }` at the top
  of the frame (the shell and every native, light-DOM micro);
- provides it, as the `clay_dioxus::UserStylesheet` context, to every mount below the frame.
  `MountedSurface` reads that signal, so a change re-renders every live mount, which sets the
  new sheet text as the mount's adopted sheet: shell and micros restyle together, live.
  (Under Blitz, which has no shadow roots, the same composed text is the mount's light-DOM
  `<style>`.)

## How a micro asks the host to apply one

Two typed calls published by the shell, which every package micro can already require (the
catalog binds a package's requirements to the shell's publications by name,
`InstalledMicro::satisfied_by`). They are part of the shell contract, so an alternative shell
implements them:

- `get-theme`: `()` in, `ThemeSheet` out. The CSS applied now.
- `set-theme`: `ThemeSheet` in, `()` out. Validates, writes the facet, and returns once the
  write is settled. Rejected, with the reason, if the CSS could make the host fetch.

There is no generic "run a host action": the contract is exactly these two calls. The theme
lab micro requires `nav-button` (its app bar), `get-theme` and `set-theme`, and contributes one
shell list panel row ("Theme lab") that its manifest declares, so home lists it while it is
stopped.

## How the micro previews on its own surfaces

The lab renders the draft CSS as its own stylesheet (`document::Style`, the guest's standard
head component, which the guest runtime sends the host as the mount's `set-style-sheet`). That
is the micro layer, above the applied theme, so the preview shows the draft and not the
applied sheet, on the lab's own mount only: other micros and the shell are untouched until
Apply. The preview region is the whole lab surface (editor included), which is faithful, since
the draft applies to every element the same way Apply later will. The Preview checkbox turns
the draft off for the lab's own surface (a draft that makes the editor unreadable can be
switched off; Revert also restores the applied theme). Sample kit surfaces (app bar, text
roles, primary/secondary/destructive buttons, list rows, a card) are below the editor, taken
from the showcase's specimens. Apply sends the draft through `set-theme`; Revert returns the
draft to what is applied.

## The fetch rule

Theme CSS may not make the host fetch anything. It is the existing guest rule, which
`clay_surface` enforces on everything a guest hands the host: the wire's attribute vocabulary
(`render.rs`) has no `src`/`srcset`, an SVG `use` takes only a same-document `href`, an inline
`style` value that can fetch is refused, a micro's stylesheet loses every fetch path
(`stylesheet.rs`, `sanitize_stylesheet`), and `GuestDom` refuses `<link>`/`<script>` head
elements (so `document::Stylesheet` and `document::Style { href }` fail). The theme gets the
same rule as a refusal instead of a repair, because a theme is text a person wrote and is about to apply, so a silently dropped `url(` is a
mistake worth showing:

- `clay_surface::refuse_fetching_stylesheet(css) -> Result<(), String>` walks every token at
  any nesting depth and refuses `url(` (quoted, unquoted, escaped, any case), a bad url,
  `image-set(`, `-webkit-image-set(`, `image(`, `src(`, `cross-fade(`, `-webkit-cross-fade(`,
  and any at-rule outside the kept set (`@import`, `@font-face`, `@namespace` and the rest).
  Custom properties are included (`var()` would carry a url into another property).
- `set-theme` calls it before anything is stored; the error is the call's rejection and the lab
  shows it under the textarea. The frame calls it again on what the facet holds.
- A draft's preview is sanitized by the existing guest rule on its way to the mount, so a draft
  with a fetch path previews without it and Apply names the refusal.

Tests: `clay_surface::stylesheet::tests::a_theme_with_any_fetch_path_is_refused_not_repaired`
and `an_inert_theme_is_accepted_whole`.

## Storage: one swappable place

Storage is the `ShellTheme` facet and the two shell calls above, nothing else reads or writes
it (the frame observes the facet; the calls are the only writer). Slice 2 replaces that facet
with an Automerge Text datum in a theme cartridge's drawer (nodules' advice), which a
collaborative editor can merge: replace the facet read in `Shell::theme` / `use_theme` and the
write in `Shell::set_theme`. The `ThemeSheet` wire type, the two calls, the frame's apply path
and the lab do not change. The facet is document-backed and so also syncs across the user's
devices today.

## Default enablement

A discovered package is admitted disabled and stays so until a user enables it in Settings.
The theme lab is in `dayframe_native::ENABLED_ON_ADMISSION`, so the first boot that finds it
records it enabled. It is a stand-in until the general default-enable for installed micros
lands; delete the list then.

## Not done / open

- The web shell's CSP (`style-src-elem 'self'`) refuses inline `<style>`: the host document's
  copy of the theme is an inline `<style>`, so on the web shell only the mounts (adopted
  sheet) are themed, not the shell's own document. A constructable sheet adopted by the
  document would fix it; the web shell is superseded, so left.
- `@layer user` in the host document relies on the Tailwind sheet declaring its layers first.
- The old showcase theme lab (`dayframe_showcase`) is unchanged; it keeps being the
  specimen page and is to be retired when the micro covers its specimens.
