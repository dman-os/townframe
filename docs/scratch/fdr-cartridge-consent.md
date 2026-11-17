# FDR (number to be assigned): What a person grants a cartridge, and how they are asked

Status: Proposal (draft, not accepted)
Owner: Townframe maintainers (nodules and claytxt lanes)
Updated: 2026-11-17
Scope: The person-facing side of the permission model: the questions a cartridge's access is asked in, where and when they are asked (install, runtime, the CLI), what is shown beside them (risks), and how a person reviews and takes access back. The mechanism is `docs/scratch/permission-model.md` (#155); this record says what people observe and is the bar that mechanism is judged against.

## Purpose

A person installs a cartridge from a store, the way they install an app on a phone. They decide
what it may reach in words they already use, once per question, and they can see and undo every
answer later. They never learn what a routine, a micro, a nodule or a permbind is. The fine-grained
declarations exist underneath so the system can tell them what a combination of access means
(risks), not so they can reason about it.

## Vocabulary a person sees

- **Cartridge:** the thing they installed. It is the only actor they relate to.
- **Drawer:** their named collections. Access is **read**, **write** or **admin**, the words the node
  already uses for every other grant.
- **Data from another cartridge:** "may use your data from Notes".
- **Internet:** "may talk to `api.example.com`".
- **Only this time / Allow / Don't allow.** "Only this time" lasts until the device restarts.

Not shown on the default path: routines, micros, nodules, permbind names, datum kinds, capability
handles. An advanced view may show the declarations for people who ask.

## Use cases

1. **Install a cartridge with what it needs.** The install screen lists each question once, with
   the drawer or cartridge proposed (most have one obvious answer) and whether the cartridge works
   without it. Saying no to an optional item installs it anyway; saying no to a required one
   installs it as "needs setup".
2. **A running cartridge asks for more.** A photo editor opens fine, then asks for the Photos drawer
   when the person first imports. The prompt names the cartridge, the drawer and the access, and
   offers Only this time / Allow / Don't allow. It appears wherever the person is: in the app, in
   the shell's notifications, or in the CLI.
3. **Nothing in front of the person.** A background job (no screen) needs access. Its request waits
   in the same pending list until the person opens it, in the shell or with `day grants pending`.
   The job reports that it is waiting rather than failing silently.
4. **One cartridge needs another's permission.** The person uses cartridge X, which calls cartridge
   Y, and Y has not been allowed to read the drawer involved. X shows "Y needs access to Photos for
   this" with the request to answer, not a generic error.
5. **See what a cartridge can do.** A per-cartridge page lists every answer in the same words, with
   the risks below, and each answer can be changed or taken back. Taking access back takes effect
   on the cartridge's next action.
6. **An update wants more.** An update that asks for more stays on the old behaviour (or "needs
   setup") until the person answers the new questions. It never gains access silently.
7. **A new device.** Answers follow the person to their other devices, as the cartridge's settings
   do; what is installed and running stays per device.

## Risks are shown, not only permissions

Beside the questions the person sees what the combination allows, from the static analysis
(proposal §9a), in plain words and only when it says something:

- "Can send your Photos to `api.example.com`", when some part of the cartridge reads Photos and some
  part talks to the internet and the two can pass data between them.
- "Can change your Notes" when write access is asked where read would have looked enough.

The store (the blessed registry) runs the same analysis before listing a cartridge. It flags risky
combinations to reviewers and may refuse to list the worst. A cartridge installed from elsewhere
shows the same risks; nothing is hidden because it skipped the store.

Rich internet services (GitHub, Google) come from trusted platform cartridges. A cartridge that
uses them asks "may use your GitHub account through the GitHub cartridge", not for raw internet
access, so its risks stay small and readable.

## The CLI

- `day grants pending` lists waiting requests: cartridge, question, when asked.
- `day grants accept <id> [--once]` and `day grants decline <id>` answer one.
- `day grants list [<cartridge>]` shows the answers, and `day grants revoke` takes one back.

Wording matches the shell's, so a person who answered on one surface recognises it on another.

## Not decided here

- The visual design of the screens: the design-language work's.
- How long a declined request stays declined before a cartridge may ask again (proposal §10).
- Drawer "types" as something a cartridge can name ("your photo drawers") rather than a specific
  drawer (proposal §10).
