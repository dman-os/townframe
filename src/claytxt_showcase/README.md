# Design showcase

Native Dioxus showcase of the `clay_dioxus_kit` components: one specimen page per component of the
styling contract (`docs/design-language/styling-contract.md`), each showing every variant and
state, then the older layout components, the foundation tokens, the Material colours and the diff
view. It is built from kit components only: the one styling it carries is layout glue around the
specimens.

```sh
deno run --allow-all x/daytxt.ts showcase
```

The stylesheet bundle is a manganis asset, which only the Dioxus CLI resolves, so run the showcase
through that wrapper (a bare `cargo run` shows the unstyled DOM).

| Tab (`?tab=` / `DAYTXT_LAB_TAB`) | Page |
|---|---|
| `button`, `tabs`, `card`, `separator`, `alert`, `chip` | `specimens/<name>.rs`, the contract components |
| `scrub-strip` | `specimens/scrub_strip.rs`: A to Z sections with the strip pinned to the window edge |
| `nav-button`, `app-bar`, `list-row`, `panel-page`, `sheet`, `drawer`, `sections`, `list-details` | `specimens/<name>.rs`: the layout components, the framed ones at a compact and an expanded width (a frame is the host: it hands the component its width class) |
| `tokens`, `colors` | `foundations.rs` |
| `diffs` | `diffs.rs`, `pierre_diffs_dioxus::FileDiffView` |

`DAYTXT_LAB_TAB=<tab>` opens a tab at start-up, so a screenshot run needs no click. A page with
more than one section gets the kit's own `ScrubStrip` as an index. `DAYTXT_RENDERER=blitz` runs under Blitz instead of Wry (and tells the kit it cannot
move focus: `Renderer::Blitz`). The native window cannot be scrolled by automation, so a screenshot
run captures one page or group at a time (see `.agents/skills/daytxt-native-dev-loop`).

The theme lab itself is the `micro_theme_lab` micro (`docs/scratch/theme-lab-micro.md`); this
showcase has no theme editor, only the light and dark toggle.
