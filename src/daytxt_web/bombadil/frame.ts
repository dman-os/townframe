// The daytxt shell's observable state, read from a page. Shared by the Bombadil properties
// (`shell.ts`) and the Playwright pin of the reading itself (`e2e/frame.spec.ts`).

export type Rect = { left: number; top: number; right: number; bottom: number };

export type Frame = {
  /// Some panel region has rendered. Before that (first load, a reload) the shell is still
  /// "Loading the shell…" and no structural property applies.
  booted: boolean;
  /// `main` landmarks on screen.
  mains: number;
  /// Names of the outermost panel regions that are not details (the list/main position).
  lists: string[];
  /// Names of the outermost panel regions that are details ("<list> details").
  details: string[];
  /// Bounds of every enabled on-screen button that reads "Back".
  backs: Rect[];
  /// On-screen buttons that move between positions: "Back", "Forward", "Open sidebar", "Hide sidebar" (a disabled one is listed as such).
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

/// Reads the frame from `document`. Self-contained (no module-level helpers) so that the
/// Playwright pin can serialise it into a page with `page.evaluate`, as Bombadil runs it.
///
/// Every Clay presentation (a guest's panel, the layout's app bar with the shell's
/// navigation rack) mounts inside a `data-dioxus-shadow` root, which neither
/// `document.querySelectorAll` nor `closest` crosses. A hidden sidebar's only way back is that
/// button, living in the layout's shadow root, so a reading that stopped at the document edge saw
/// "no navigation" for a screen that had it. All queries and ancestor walks therefore go
/// through the composed tree.
export function readFrame(document: Document, window: Window): Frame {
  const nameOf = (element: Element) =>
    element.getAttribute("aria-label") ?? (element.textContent ?? "").trim();
  // The parent in the composed tree: a shadow root's top element continues at its host.
  const parentOf = (element: Element): Element | null => {
    const root = element.getRootNode();
    return (
      element.parentElement ?? (root instanceof ShadowRoot ? root.host : null)
    );
  };
  // `element.closest`, across shadow roots.
  const composedClosest = (
    element: Element,
    selector: string,
  ): Element | null => {
    for (
      let current: Element | null = element;
      current !== null;
      current = parentOf(current)
    ) {
      if (current.matches(selector)) return current;
    }
    return null;
  };
  // `querySelectorAll`, descending into open shadow roots.
  const deepQueryAll = (
    root: Document | Element,
    selector: string,
  ): Element[] => {
    const found = [...root.querySelectorAll(selector)];
    for (const host of root.querySelectorAll("*")) {
      if (host.shadowRoot !== null) {
        found.push(
          ...deepQueryAll(host.shadowRoot as unknown as Element, selector),
        );
      }
    }
    return found;
  };
  // On screen: rendered, and not hidden from assistive technology (the kit marks a list that its
  // details cover as `aria-hidden`). Opacity is deliberately not part of it: the kit's enter
  // motion fades a scrim in from opacity 0 (KIT-105 `kit-fade-in`), and a control that is still
  // fading in is present, not missing; a sampled frame in that interval used to report the
  // drawer's "Dismiss" scrim as absent.
  const onScreen = (element: Element) =>
    element.checkVisibility({ visibilityProperty: true }) &&
    composedClosest(element, '[aria-hidden="true"]') === null;
  const all = (selector: string) =>
    deepQueryAll(document, selector).filter(onScreen);
  const buttonsNamed = (...names: string[]) =>
    all("button").filter((button) => names.includes(nameOf(button)));

  const enclosingRegion = (element: Element) => {
    const parent = parentOf(element);
    return parent === null ? null : composedClosest(parent, '[role="region"]');
  };
  // A panel region is a region that is not the shell's own chrome ("Sidebar", "Content"), not
  // inside a dialog, and not inside another panel region (a guest's sections are regions too).
  const panelRegions = all('[role="region"]').filter((region) => {
    const name = nameOf(region);
    if (name === "Sidebar" || name === "Content") return false;
    if (composedClosest(region, '[role="dialog"]') !== null) return false;
    for (
      let ancestor = enclosingRegion(region);
      ancestor !== null;
      ancestor = enclosingRegion(ancestor)
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
    backs: buttonsNamed("Back")
      .filter((button) => !(button as HTMLButtonElement).disabled)
      .map((button) => {
      const { left, top, right, bottom } = button.getBoundingClientRect();
      return { left, top, right, bottom };
    }),
    navigation: buttonsNamed("Back", "Forward", "Open sidebar", "Hide sidebar").map(
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
        hasSidebarNavigation: deepQueryAll(
          dialog,
          'nav[aria-label="Sidebar"], [role="navigation"][aria-label="Sidebar"]',
        ).length > 0,
        isEmpty: dialog.children.length === 0,
      };
    }),
    sideBySide: window.innerWidth >= 600,
    loadingSections: all('[role="status"]').filter(
      (status) => nameOf(status) === "Loading section…",
    ).length,
  };
}
