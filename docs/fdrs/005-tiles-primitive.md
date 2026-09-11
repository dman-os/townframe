# FDR 005: The Tiles Primitive

**Status:** Proposed.

## What this record is

An FDR (feature decision record): the functional decision, described from a functional
perspective. Technical/implementation choices (storage, transport, renderer) are separate
records (see ADR 010 for the pieces/faces/in-proc decisions).

## Functional decision

The dayframe shell UI is organized around **tiles** — entries in ordered lists. A tile is one
unit of content or action: a surface (daybook, daybrowser, daychat, dayshell), a widget, a
document, a chat, or a group of other tiles.

- Tiles sit in **ordered lists**. Lists are not necessarily alphabetized; ordering is a per-list
  policy. Some lists will be manually ordered by the user (e.g. the home page list).
- A tile opens a surface, holds content in place (widget), or exposes actions (create, capture,
  chat).
- A tile can contain a **nested list** (a sublist). Unlike Niagara's popup folders, sublists may
  expand/collapse in place — the interaction is to be decided by experiment.
- Tiles can be **stacked**: several tiles in one slot, cycled as a carousel.
- Lists can sit **side by side (columns)** on desktop. How columns translate to mobile is an open
  question (swipeable columns vs a single list).
- Content is **not tied to a specific model or format**. Facet-shaped tiles are one likely case and
  can be built on top of the primitive; the primitive should not require facets, or HTML/DOM/JS.

## What this primitive is not (yet)

- Not a proven solution. It is a proposal that the prototype in dayframe_dx must validate.
- Not a plugin/extension format. Plugins may eventually supply tiles; that part is deferred.
- Not a storage decision. Whether lists persist (tables datastore) or are ephemeral (chats,
  launcher app lists) is open, per list.

## Open questions (to be answered by the prototype)

1. Terminology for lists (list / shelf / column / board?) — settle before building on top.
2. Which lists persist and which are dynamic/ephemeral?
3. Ordering: how order is stored and edited; manual sort on some lists, policy elsewhere.
4. Columns on desktop → what on mobile?
5. Swipe model on mobile: swipe between columns vs swipe to open a sublist — how do they coexist?
6. Sublists: popup (Niagara) vs expand-in-place collapsible — which interaction?
7. Stacked-carousel interactions (click / hover / scroll to cycle).

## Next step

Prototype tiles in dayframe_dx (desktop first): tile lists, nesting with expand/collapse,
stacking, columns. Use the prototype to answer the open questions.
