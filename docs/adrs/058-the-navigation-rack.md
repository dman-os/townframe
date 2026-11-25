# ADR 058: The navigation rack

**Status:** Accepted (2026-11-25, operator, in the daytxt visuals pairing). Proposed by the claytxt lane. Nothing here
is built.

**Amends:** [ADR 032](032-shell-micro-and-component-kit.md) §5 (the sidebar/back button).

Related: [FDR 006](../fdrs/006-shell-grammar.md) (the shell grammar), [ADR 057](057-kit-baseline-and-the-identity-sheet.md)
(the kit baseline), `src/micro_shell/view.rs` (`NavigationButton`, `Sidebar`), `src/micro_shell/navigation.rs` (back and
forward).

## 1. Context

ADR 032 §5 gives the shell one navigation control: a button that shows **back** while the current context can go back
and **open the sidebar** otherwise, which a micro mounts in its own app bar. Two problems came out of using it:

- **Back disappears.** The button is one or the other, never both, so wherever opening the sidebar wins, the way back
  is gone; on a wide window with the sidebar docked the button is absent altogether. On a phone that reads as "there is
  no back button".
- **It assumes an app bar.** The shell does not control a micro's content, and a micro need not draw a header row. The
  control only exists where a micro chose a place for it in a bar.

The operator's reference is the Claude desktop app: one cluster of controls (sidebar toggle, back, forward) that stays
at the window's top-left whether the sidebar is open or not. With the sidebar open the cluster is the sidebar's header;
closed, it sits over the start of the content. Nothing moves when the sidebar opens; the sidebar slides in under it.

## 2. Decision

### 2.1 The rack replaces the button

The shell's navigation control is a **rack**: a small group of separate buttons, never one button that changes
meaning. It is still a presentation the host supplies and a micro mounts (a custom element, per the web-components
contract), and it still acts on the shell's state, so ADR 032's ownership is unchanged: the micro decides where it
goes. The single morphing `NavigationButton` is removed.

What the rack holds depends on the width class (FDR 006) and the position it is mounted in:

| Host | Position | Rack |
|---|---|---|
| wide | main, sidebar hidden | sidebar toggle, back, forward |
| wide | details beside its list | nothing: the window's top-left is main's, and main's back and forward cover details |
| wide | any, sidebar shown | nothing: the sidebar's header holds the controls (§2.3) |
| compact | main (a list, a top-level panel) | sidebar toggle; back when there is somewhere to go back to |
| compact | details | back only |

On a compact host a details page does not offer the sidebar: leaving a details page goes back to its list first, as
it does today. Forward is not in the compact rack; it is in the sidebar's footer there (§2.3). Back and forward are
disabled, not hidden, on a wide host when there is nowhere to go, so the rack keeps its shape.

### 2.2 Floating is the default presentation

The rack's default presentation is **floating**: a compact group over the content's top-left corner, not a header
row. The kit's content pane (`PanelPage` and the list/details panes) mounts the rack floating by default and keeps that
corner clear, so a pane built with the kit gets navigation and the right spacing without drawing an app bar. A micro
that wants a header row can mount the rack inside its own app bar instead; one that mounts nothing still has the
platform back gesture.

### 2.3 With the sidebar shown

When the sidebar is shown (docked on a wide host, or open as the drawer on a compact one), the shell draws the same
controls in the sidebar's header, in the same place on screen, and every mounted rack hides itself. The header begins
with the app's mark, a 🌞 for now: a plain mark, not a control, and shown only there. There is no menu behind it yet;
nothing is added before there is something to put there.

On a compact host the sidebar's footer carries forward.

### 2.4 Later

The macOS title bar keeps its window controls at the top-left. If daytxt draws its own title bar there, the rack and
the sidebar header take a leading inset from the host; until then nothing reserves it.

## 3. Consequences

- Positive: back is always reachable where it makes sense, on every width; the sidebar toggle and back never compete.
- Positive: navigation no longer depends on a micro drawing an app bar; kit-built panes get it by default.
- Positive: the controls do not move when the sidebar opens or closes.
- Cost: a floating rack covers the content's top-left corner; panes not built with the kit must leave room for it.
- Cost: every caller of `NavigationButton` moves to the rack (unmerged code: no shim).

## 4. Order of work

1. The rack presentation in `micro_shell` with the rules of §2.1 (state from `Navigation::can_back` and a matching `can_forward`,
   the sidebar state and the width class), replacing `NavigationButton`; headless tests for each row of the table.
2. Floating by default in the kit's content panes (truly floating: pinned over the pane's top-left while content scrolls under it; the pane reads the rack from a kit context the micro provides once from the shell's presentation, and keeps its first content clear of the corner by a rack-size token); app bars that mounted the old button
   move to the rack.
3. The sidebar header (mark, toggle, back, forward) and the compact footer's forward; the "Hide sidebar" button and the
   sidebar's own app bar go.
4. The look of the rack and the sidebar header in the identity sheet (ADR 057), judged on the board.
