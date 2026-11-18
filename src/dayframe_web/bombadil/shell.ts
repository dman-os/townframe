// Invariants of the Dayframe shell, explored by Bombadil (ADR 032 section 9: the browser build is
// the broad integration surface; Playwright holds the scripted flows and Bombadil the properties
// that must hold in every state it can reach by clicking). The shell's grammar is FDR 006.
//
// Everything is read through roles and accessible names (`role`, `aria-label`, the native
// `button`/`main`), never through classes or markup structure, so a restyle cannot break a
// property. Extractors run inside the page, and a property that goes through `next` compares
// the state before an action with the state after it.
import { always, eventually, next } from "@antithesishq/bombadil";
import { actions, extract } from "@antithesishq/bombadil/browser";
import { lastAction } from "@antithesishq/bombadil/browser/defaults/actions";

// The exploration's moves: clicks on buttons, typing into the textbox, browser back (the
// platform back gesture, ADR 032), reload.
export { defaultActions } from "@antithesishq/bombadil/browser/defaults";
// Page errors, rejected promises and `console.error` (a Rust panic in the Wasm shell surfaces as
// one of these) are covered by Bombadil's own defaults; the shell must never produce any, so
// they are re-exported unchanged. The default `noHttpErrorCodes` stays too: every asset the
// shell fetches is served by the same static server.
export {
  noConsoleErrors,
  noHttpErrorCodes,
  noUncaughtExceptions,
  noUnhandledPromiseRejections,
} from "@antithesishq/bombadil/browser/defaults/properties";

type Rect = { left: number; top: number; right: number; bottom: number };

type Frame = {
  /// Some panel region has rendered. Before that (first load, a reload) the shell is still
  /// "Loading the shell…" and no structural property applies.
  booted: boolean;
  /// `main` landmarks on screen.
  mains: number;
  /// Names of the outermost panel regions that are not details (the list/main position).
  lists: string[];
  /// Names of the outermost panel regions that are details ("<list> details").
  details: string[];
  /// Bounds of every on-screen button that reads "Back".
  backs: Rect[];
  /// On-screen buttons that move between positions: "Back", "Open sidebar", "Hide sidebar".
  navigation: { name: string; enabled: boolean }[];
  /// On-screen dialogs (the compact sidebar drawer and sheets).
  dialogs: {
    name: string;
    hasDismiss: boolean;
    hasSidebarNavigation: boolean;
    isEmpty: boolean;
  }[];
  /// The viewport is wide enough that the sidebar sits beside the content (the kit's
  /// `WidthClass::fits_side_by_side`: 600 px and up).
  sideBySide: boolean;
  /// On-screen "Loading section…" placeholders.
  loadingSections: number;
};

const frame = extract((state): Frame => {
  const { document, window } = state;
  const nameOf = (element: Element) =>
    element.getAttribute("aria-label") ?? (element.textContent ?? "").trim();
  // On screen: rendered, and not hidden from assistive technology (the kit marks a list that its
  // details cover as `aria-hidden`). Opacity is deliberately not part of it: the kit's enter
  // motion fades a scrim in from opacity 0 (KIT-105 `kit-fade-in`), and a control that is still
  // fading in is present, not missing; a sampled frame in that interval used to report the
  // drawer's "Dismiss" scrim as absent.
  const onScreen = (element: Element) =>
    element.checkVisibility({ visibilityProperty: true }) &&
    element.closest('[aria-hidden="true"]') === null;
  const all = (selector: string) =>
    [...document.querySelectorAll(selector)].filter(onScreen);
  const buttonsNamed = (...names: string[]) =>
    all("button").filter((button) => names.includes(nameOf(button)));

  // A panel region is a region that is not the shell's own chrome ("Sidebar", "Content"), not
  // inside a dialog, and not inside another panel region (a guest's sections are regions too).
  const panelRegions = all('[role="region"]').filter((region) => {
    const name = nameOf(region);
    if (name === "Sidebar" || name === "Content") return false;
    if (region.closest('[role="dialog"]') !== null) return false;
    for (
      let ancestor = region.parentElement?.closest('[role="region"]') ?? null;
      ancestor !== null;
      ancestor = ancestor.parentElement?.closest('[role="region"]') ?? null
    ) {
      if (nameOf(ancestor) !== "Content") return false;
    }
    return true;
  });
  const names = panelRegions.map(nameOf);

  return {
    booted: names.length > 0,
    mains: all("main").length,
    lists: names.filter((name) => !name.endsWith(" details")),
    details: names.filter((name) => name.endsWith(" details")),
    backs: buttonsNamed("Back").map((button) => {
      const { left, top, right, bottom } = button.getBoundingClientRect();
      return { left, top, right, bottom };
    }),
    navigation: buttonsNamed("Back", "Open sidebar", "Hide sidebar").map(
      (button) => ({
        name: nameOf(button),
        enabled: !(button as HTMLButtonElement).disabled,
      }),
    ),
    dialogs: all('[role="dialog"]').map((dialog) => {
      const name = nameOf(dialog);
      return {
        name,
        hasDismiss: buttonsNamed(`Dismiss ${name}`).length > 0,
        hasSidebarNavigation:
          dialog.querySelector(
            'nav[aria-label="Sidebar"], [role="navigation"][aria-label="Sidebar"]',
          ) !== null,
        isEmpty: dialog.children.length === 0,
      };
    }),
    sideBySide: window.innerWidth >= 600,
    loadingSections: all('[role="status"]').filter(
      (status) => nameOf(status) === "Loading section…",
    ).length,
  };
});

// Why it must hold: a shell with no content region is a blank screen, and one with two lists is
// a stack that did not unwind (FDR 006, "List and details"; ADR 032, "Stack semantics": a push
// replaces the open row's entry, so the main position holds exactly one panel). On a phone the
// details come over the list, which is then out of reach, so exactly one region is on screen; in
// a wide window the list stays and at most one details region sits beside it.
export const oneMainRegion = always(() => {
  const current = frame.current;
  if (!current.booted) return true;
  const regions = current.lists.length + current.details.length;
  return (
    current.mains === 1 &&
    (current.sideBySide
      ? current.lists.length === 1 && current.details.length <= 1
      : regions === 1)
  );
});

// Why it must hold: the sidebar/back button is the only way to move between contexts and has no
// bottom bar to fall back on (FDR 006, "Phone and desktop": back inside list-and-details, open
// the sidebar otherwise). A wide window with the sidebar visible shows "Hide sidebar" in it; a
// hidden sidebar must always leave "Open sidebar" or "Back" in the panel's app bar.
export const navigationAlwaysReachable = always(() => {
  const current = frame.current;
  if (!current.booted) return true;
  return current.navigation.some((button) => button.enabled);
});

// Why it must hold: back unwinds the navigation stack one entry (ADR 032, "Stack semantics"),
// and the entry under a details panel is the list that opened it. So a click on "Back" while a
// details region is open must leave that list on screen and the details gone. The click is
// recognised by where it landed: the next state's last action is a click inside the bounds
// "Back" had before it.
export const backReturnsToTheList = always(() => {
  const before = frame.current;
  if (before.details.length !== 1 || before.backs.length === 0) return true;
  const list = before.details[0].slice(0, -" details".length);
  return next(() => {
    const action = lastAction.current;
    if (typeof action !== "object" || action === null || !("Click" in action))
      return true;
    const { x, y } = action.Click.point;
    const onBack = before.backs.some(
      (rect) =>
        x >= rect.left && x <= rect.right && y >= rect.top && y <= rect.bottom,
    );
    if (!onBack) return true;
    const after = frame.current;
    return after.lists.includes(list) && after.details.length === 0;
  });
});

// Why it must hold: a sheet is "an auxiliary, dismissable page that belongs to a panel" and the
// compact sidebar is a drawer over the content (FDR 006, "Sheet" and "Sidebar"); a sheet goes
// with its panel and entries naming a retracted panel are pruned (ADR 032). What is observable
// from outside is that a dialog on screen has its panel's content and a way out: the drawer
// holds the sidebar's navigation (and exists only where the sidebar does not sit beside the
// content), and every dialog is non-empty with its "Dismiss <name>" scrim.
export const overlaysBelongToTheirPanel = always(() =>
  frame.current.dialogs.every((dialog) =>
    dialog.name === "Sidebar"
      ? dialog.hasSidebarNavigation &&
        dialog.hasDismiss &&
        !frame.current.sideBySide
      : !dialog.isEmpty && dialog.hasDismiss,
  ),
);

// Why it must hold: a panel's presentation mounts as soon as its guest answers; "Loading
// section…" is the placeholder for that wait (ADR 032). A placeholder that never goes away is a
// panel that never mounted, e.g. the guest executor not being polled (the S5 stall in
// `clay_rt_wcmp`). Bombadil states "eventually" with a time bound; the bound is generous
// against a cold guest compile and far below the 30 s collection long poll that bug waited on.
export const loadingSectionsFinish = always(() =>
  frame.current.loadingSections === 0
    ? true
    : eventually(() => frame.current.loadingSections === 0).within(
        15,
        "seconds",
      ),
);

// Why it must hold: the shell has to come up at all. A load that never reaches a panel region
// would otherwise satisfy every other property vacuously (they only apply once booted).
export const shellBoots = eventually(() => frame.current.booted).within(
  120,
  "seconds",
);

// The shell is a Wasm bundle that takes seconds to fetch and compile after every load, and until
// a panel renders there is nothing to click, so Bombadil would stop with "no actions available".
// Waiting is the only move until then; `shellBoots` bounds how long that may take.
export const waitForTheShell = actions(() =>
  frame.current.booted ? [] : ["Wait"],
);
