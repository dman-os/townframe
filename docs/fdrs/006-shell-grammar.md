# FDR 006: The Shell Grammar

**Status:** Accepted (operator, 2026-11-05) for the structural layer; the semantic layer and
universal seams are directional.

## What this record is

A feature decision record: the functional vocabulary of Dayframe's shell — how surfaces are
arranged, navigated and restored — described from the user's side. How the vocabulary is
carried across micro boundaries (collections, calls, presentations) is a separate technical
record, written once this one settles.

Downstream of [FDR 005: the tiles primitive](005-tiles-primitive.md).

## Purpose

Dayframe is meant to be malleable in the way Emacs is, while feeling like a browser:
documents, chats, media, code and pages are opened, kept, arranged and returned to. It must
work on phones and desktops alike; a design that only works with a mouse and a wide window is
not acceptable.

Conventional apps hard-code their screens (a bottom bar, a home menu, fixed tabs) and need no
"glue" between features. Dayframe's features are micros written by different people, so the
space between them has to be a substrate users can arrange. Tile lists (FDR 005) are that
glue; this record names the surfaces around them.

The shell must serve these experiences without each one inventing its own frame:

- document browsing and editing (a basic editor first; extensible later);
- in-app development of micros: code editing, file trees, multiplayer agentic chat;
- one-to-one chat with a first-class camera (a Snapchat replacement);
- media management (a Google Photos replacement: galleries, "memories");
- a home space that can later become an Android launcher (apps, notifications, widgets,
  generated UI, the hero widget);
- eventually a full web browser.

## Two layers

The vocabulary splits into a **structural** layer (where things sit and how you move between
them) and a **semantic** layer (what an open thing means to the user). The structural layer is
lower: it knows nothing of tabs or history, and the semantic layer is built on it. They are
kept apart; the first shell delivers the structural layer.

### Structural grammar

- **Panel:** a surface that fits one of the chrome's sockets — the sidebar, the center, a
  side or bottom section — and owns its whole boundary: it is a self-contained area with its
  own content, not a control embedded in someone else's context. Panels are not specific to
  the shell: another micro (for example a canvas) may host panels inside itself using the same
  grammar.
- **Sections:** the sockets panels fill, resizable where the arrangement allows it (sidebar
  width, list/details split, a panel's own side section).
- **List and details:** the primary arrangement. A list panel (a tile column, a chat list, a
  drawer) opens a details panel. On a phone the details come on top of the list; in a wide
  window the two sit side by side. A list item may open another list: the new list is the
  details, and repeated list-to-list opening forms a stack (in a wide window, FDR 005's column
  strip is the same stack laid out sideways).
- **Navigation:** back and forward over that stack.
- **Sidebar:** the socket for navigation across micros. On a phone it is the left drawer; on a
  desktop a side panel, mostly visible, that can auto-hide. It may itself be tabbed, as
  Obsidian's sidebar is.
- **Sheet:** an auxiliary, dismissable page that belongs to a panel (pickers, details,
  confirmations), following the earlier "big dialog" design: a bottom sheet on a phone, a side
  sheet in a wide window.
- **OS windows:** multi-window desktops and single-window phones must both be expressible,
  but the noun for "one OS window" is deferred until it earns its keep. (The former per-device
  "window" is retired.)

### Semantic grammar

- **Tile list (column):** an ordered list of sections of tiles, per FDR 005; the glue between
  features. Columns are kept narrow, even on desktop; a wide window shows several columns side
  by side rather than one wide column. A tile column is a panel.
- **Home:** a list panel of several narrow tile columns with sections, widgets and the hero
  widget at the top. It usually has no details, though a pinned item (a contact, a chat room)
  opens its details like any other list.
- **Tab:** a panel that declares itself *content*: "I am a context; put me front and center."
  A tab may contain panels of its own (a code tab with a file tree, an editor tab with facets
  beside it). Tabs are what the user opens and expects back. A host other than the shell (a
  canvas) may also hold tabs.
- **History:** what was opened, across local and public drawers alike (browsing is not
  segmented into local versus global). Designed in its own record.
- **Table:** a separate context for a separate kind of work, with its own arrangement (for
  example a vertical-tab default, a canvas, a focused writing table). Switching tables is not a
  "mode" that turns the app into another app, and it is not reconfiguring the current context.
  A power-user tool; one default table to start.

Not vocabulary: **tab groups** behave like tags over tabs and are a shell implementation choice.

### Universal seams

Besides the sidebar, a few affordances reach across every micro. The shell owns them; micros
opt in or contribute to them.

- **Command palette:** an advanced palette in the manner of IntelliJ IDEA's: search, AI chat
  and quick commands in one place. A keyboard shortcut on desktop; reachable quickly from home.
  How it is reached on mobile is up to the micro; the initial default is a two-finger swipe
  down (as in Obsidian), to be revisited once its visuals and users' expectations are known.
- **Search convention:** micros offer search over things the shell knows nothing about, and it
  should feel the same everywhere. This is a portable convention micros implement, not a
  special search surface and not a sidebar tab; the command palette is one place it appears.
- **Arc strip:** dragged in from the screen edge, it shows a table of contents of the current
  panel's sections and scrolls quickly between them (FDR 005's arc index). Pressing and holding
  on the visible strip and moving the finger to a target activates quick-navigation features,
  the role the old Android "magic wand" overlay had. Micros opt in; one that does gains a
  universal navigation seam.

## Phone and desktop

- There is **no global bottom bar**. The bottom of the screen belongs to the content panel (a
  chat composer, a capture control).
- A micro controls its own app bar. The shell supplies the **sidebar / back button** as a chrome
  integration that the micro places where it wants, conventionally at the top left: it is
  **back** inside list-and-details navigation and **open the sidebar** otherwise, as in the
  Claude and ChatGPT mobile apps. It is not a global "leave this micro" affordance; that may
  come later.
- Wide-window arrangements: on home, **sidebar + list** (home's columns); inside a context,
  **sidebar + list + details**, or **sidebar + details**.
- A panel's own side section is a right-hand section on desktop and a right drawer or sheet on
  a phone.

## Sync and restore

General-purpose configuration (arrangements, sidebar contents, preferences) syncs across
devices. Open-context state — which tabs are open and what each shows — stays on the device;
another device's open contexts may later be listed (as Firefox's "tabs from other devices"),
but they are never applied automatically.

## How the use cases land

- **Documents:** a drawer browser in the sidebar; each opened doc is a tab with an editor
  panel and its facets in a side section.
- **Micro development:** each file or tool is a tab; the file tree is the tab's side section;
  agent chat is a tab, a sheet, or the command palette.
- **Chat:** the chat list is a tile column (tiles may carry quick-reply widgets); a
  conversation is the details, with its composer at the bottom.
- **Photos:** galleries and "memories" are tile-column sections; the viewer is the details.
- **Camera:** the hero widget's default face on home; a native camera micro.
- **Browser:** one tab per page; vertical tab trees (Sideberry, Arc, Zen) are sidebar
  designs over the same tabs.

## Recorded, not decided now

- Tile *implementations* that do not depend on micros or Clay, so they outlive both: likely a
  third plugin surface.
- The hero widget's contents (camera, pets, shader tiles) and the Android launcher.
- A global "leave this micro" affordance.

## What the first shell implements

The structural layer: panels and resizable sections, list and details (including list-to-list
stacking), back and forward, the sidebar socket, the sidebar/back chrome integration, and
sheets, on phone and desktop. Its first consumer is Settings: home opens Settings, whose
sections are a list with details. Tabs may land with it but stay a separate, semantic piece.
History, the command palette, the search convention, the arc strip, tables beyond one default
and the recorded items above are later work.

## Open questions

None at the functional level for the first shell; the technical record follows.
