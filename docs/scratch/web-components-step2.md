# Web components, step 2: design note for pairing

Status: draft for the 2026-11-25 design pairing. Nothing here is decided.
Owner: claytxt lane; operator reviews.
Updated: 2026-11-24
Scope: Web components step 2: `<ct-micro>` mounts, declarative shadow DOM for SSR, `part` and slots.
Context: the operator wants us fully on web components (custom elements, shadow DOM, `part`, slots) with our own
enhancements, "sooner than later" (2026-11-19). Step 1 has landed (stack layer 07): every micro, wasm guest or
built-in, mounts through one shadow mount.

## Where we are

- A mount is `div[data-dioxus-shadow]`. Our Dioxus fork (`clay/shadow-roots`, interpreter `core.ts`) calls
  `attachShadow({mode: "open"})` on it and adopts the text of `data-dioxus-shadow-sheet` as a constructable sheet.
  The layers run `@layer host` (kit) → the micro's sheet → `@layer user` (theme). The kit's reset hides the wrapper
  with `display: contents`.
- Kit components are light DOM inside the root, and their parts are `data-part="…"` attributes.
- No custom element is registered anywhere, and nothing outside Dioxus can host a micro.
- Blitz has neither shadow roots nor custom elements. It uses `LightDomStylesheets` and stays a fallback.
- New this week: intxt's web app renders our components under dioxus-ssr, without a browser. A shadow mount needs
  a serialisable form there: declarative shadow DOM, `<template shadowrootmode="open">`.

## Proposal

1. **A mount is a registered element, `<ct-micro>`.** It has attributes for the identity (`micro`,
   `presentation`), a property for typed props (the props contract stays the wire's), and lifecycle callbacks that
   tell the host to mount and unmount. The shadow root and sheet adoption move from the fork's attribute hook into
   the element. The fork's hook then only serves the non-element hosts.
2. **Three renderings of one mount:**
   - in the browser and Wry: the custom element;
   - under SSR: the same tag with a declarative shadow root and the sheets inline as `<style>`. SSR output is
     static, so CSP doesn't come into it in the same way; the CSP check stays on the live page;
   - in Blitz: light DOM, as today.
3. **Real `part` attributes.** `data-part` becomes `part` (and `exportparts` through nested mounts), so a page can
   theme a micro with `::part()` without our `@layer user` injection. Our enhancement stays: the injected user
   layer can style inside a root, which `::part` cannot.
4. **Slots for nested surfaces.** A presentation that hosts another micro (the shell hosting panels) puts it in a
   named slot instead of a nested Dioxus subtree. Each micro then owns exactly its own root.
5. **Other frameworks.** A Solid, React or plain-HTML page hosts a micro by writing `<ct-micro micro="…">` and
   loading our element definition. That's the cross-framework test the ledger has had open since 11-06.

## Questions for the pairing

1. Element name and shape: one `<ct-micro>`, or one element per presentation (`<ct-review-pr>`)? Per-presentation
   names read better in hand-written HTML but need a registry at load.
2. Are props attributes (strings), properties (typed), or both? The wire carries typed props today.
3. `::part` versus our injected `@layer user`: keep both, or narrow the user layer to tokens only once `part` exists?
4. Do kit components become custom elements too (`<ct-button>`), or stay light DOM inside roots? Light DOM is
   cheaper and keeps Dioxus diffing; elements would let non-Dioxus pages use the kit.
5. The SSR form: is a static declarative shadow DOM enough for intxt's web viewer, or does it need hydration?
6. The order of work: (a) the DSD form for intxt's SSR, (b) `<ct-micro>` in the web shell, (c) `part`,
   (d) slots. My proposal is a, b, c, d.

## Related debt

- `use_mount_stylesheet` writes a signal during render (layer 07). It moves into the element's connected callback.
- Guest view wakes are opt-in (`notify_views`). Independent, but the same pairing could settle the waker-driven render.
