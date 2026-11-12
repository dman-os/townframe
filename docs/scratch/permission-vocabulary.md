# Permission and access vocabulary

Status: Proposal for the operator (manager, 2026-11-12). Owner: Townframe
maintainers Updated: 2026-11-12 Scope: One vocabulary for who can do what. It
covers a cartridge's permissions, a client's identity, drawer sharing, and other
nodes, with a user-facing register and a developer-facing register. Built from a
full term inventory of the CLI, the wire, code and ADRs (2026-11-12). Proposals
are marked as such; nothing here is decided yet.

## The problem

Today the same idea goes by several names, and some names mean several things:

- Adding another node to a drawer is `day drawers add-member` in the CLI,
  `invitePrincipal` on the wire, and `add_member` in keyhive.
- `authority` has five meanings:
  - an NSID prefix owner;
  - the `Authorization.authority` capability;
  - the authority service;
  - "what you may do";
  - "no global authority".
- `grant` is a local entitlement, a nodule config datum, a keyhive grant on a
  doc, and consent.
- Our `write` access is keyhive's `Edit`. ADR 038 calls `relay/read/write/admin`
  "keyhive's standard verbs", but keyhive says `Edit`.
- `relay` is both an access level and a node mode. `identity` is a node seed, an
  actor, a capability used as proof, and a cartridge datum.
- The CLI leaks `capabilityId`, "handle" and `plugId`, and says "nodules" where
  users think "carts".
- The words a user would need for permissions don't exist in the CLI at all:
  allow, permissions, connect, install, enable, settings.

## Two registers (D35's split, extended)

D35 already split commands for users from XRPC names for developers. Do the same
for authority:

- **User-facing** (CLI help and output, Dayframe UI): words people already know
  from phones and Google Drive. No actors, capabilities, scopes or mints.
- **Developer-facing** (wire NSIDs, datum ids, Rust, ADRs): precise ocap and
  keyhive terms, one word per idea.

One user-facing verb pair covers every kind of permission, **allow** and
**remove**. Whether it's a cartridge's permission, a connected client, or
another node on a drawer, the user reads the same words.

## User-facing vocabulary (proposal)

| Idea                                                         | Word                                                                                  | Precedent                                                                                                        | Example                                                                                                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| A cartridge                                                  | **cart** (CLI) / **app** in UI copy                                                   | app stores                                                                                                       | `day carts ls`                                                                                                                           |
| What a cart may do                                           | **permissions**; "_intxt_ can read _Notes_"                                           | Android/iOS app permissions                                                                                      | `day carts permissions intxt`                                                                                                            |
| Giving or taking one                                         | **allow** / **remove**                                                                | Android "Allow / Don't allow"                                                                                    | `day carts allow intxt read:notes`                                                                                                       |
| A client getting its own identity (ADR 043's "pairing")      | **connect**; the list is **connections**                                              | WhatsApp/Signal "Linked devices", Google "Third-party connections", TV "sign in with a code" (OAuth device flow) | `day connect --as intxt/claude-code` shows a code; `day connections allow <code>`, `day connections ls`, `day connections remove <name>` |
| A connected client's subsystems (ADR 043 delegated approval) | the same words, scoped: "intxt allowed _Claude Code_ to connect"                      | nested "Sign in with…"                                                                                           | no operator prompt                                                                                                                       |
| Sharing a drawer with a node or group                        | **share** / **unshare**; the list is **people/devices with access**                   | Google Drive                                                                                                     | `day drawers share <drawer> <node> --role editor`                                                                                        |
| Drawer roles (keyhive `Relay/Read/Edit/Admin`)               | **backup** (holds an encrypted copy, can't read), **viewer**, **editor**, **manager** | Drive's Viewer/Editor; "backup" for relay                                                                        | `--role backup\|viewer\|editor\|manager`                                                                                                 |
| Your other nodes (sisters)                                   | **your devices**                                                                      | phone "Your devices"                                                                                             | parked with the sisters/mothers design                                                                                                   |
| Always-on nodes holding your encrypted copies (mothers)      | **backups** (or **relays** where reach matters)                                       | "backup"                                                                                                         | parked with the sisters/mothers design                                                                                                   |
| A cart's config                                              | **settings**; **reset settings** (= `clearConfig`)                                    | VS Code / phone app settings                                                                                     | `day carts settings intxt`                                                                                                               |
| The person with the keyring credential                       | **you** (copy) / **owner** (CLI, where a noun is needed)                              | "device owner"                                                                                                   | `you must be the node's owner (keyring)`                                                                                                 |

Users never see: actor, capability, handle, scope, mint, introduce, delegate,
privileged, principal, subject, NSID authority.

## Developer-facing vocabulary (proposal)

One word per idea. Keyhive's word wins wherever keyhive has one (operator, D42).

| Idea                                              | Term                                                                  | Replaces                                                                                                                                                       |
| ------------------------------------------------- | --------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A local identity with a key                       | **actor**                                                             | principal (b), subject (b), "identity" (actor sense)                                                                                                           |
| Someone in a keyhive drawer group                 | **member** (keyhive Agent)                                            | `Principal`, `invitePrincipal`/`revokePrincipal` → `addMember`/`revokeMember`; `UnknownPrincipal` → `UnknownMember`                                            |
| Drawer access levels                              | keyhive's **`relay`, `read`, `edit`, `admin`**                        | our `write` → `edit` (wire, CLI arg, `DrawerAccess::Write`)                                                                                                    |
| A live, holder-bound handle                       | **capability** (`CapId`)                                              | "handle" in user output                                                                                                                                        |
| The actor a capability is bound to                | **holder**                                                            | `subject` (holder sense); grantee `subject_id` → `actor`                                                                                                       |
| A durable local entitlement (yields capabilities) | **grant** only                                                        | grant used for keyhive membership or consent                                                                                                                   |
| Shrinking a capability                            | **narrow**                                                            | attenuate, "from a source"                                                                                                                                     |
| Killing a capability / removing a grant or member | **retract** (capability) / **revoke** (grant, member)                 | `remove`, `forget` stay for objects only                                                                                                                       |
| Minting for a descendant actor                    | **introduce**, with right `INTRODUCE`                                 | `DELEGATE` (collides with keyhive's `Delegation`)                                                                                                              |
| Method may confer authority                       | **privileged**                                                        | unchanged                                                                                                                                                      |
| NSID prefix owner                                 | **authority** (atproto's sense, only this one)                        | the `Authorization.authority` field → `capability`; the "authority service" → **actors service** (it opens and resolves actors and holds the capability table) |
| A client requesting an identity (ADR 043)         | **connection**: `connections.request`, `.approve`, `.list`, `.remove` | "pairing", "enrollment"                                                                                                                                        |
| A cartridge, its parts                            | **cartridge**; **nodule** (backend), **micro** (frontend)             | `plug`/`plugId` leftovers                                                                                                                                      |
| What a cartridge needs                            | **request** (manifest), turned into **grants** on approval            | "binding" (keep only for the cartridge-local name)                                                                                                             |

The **capability** collision with keyhive (theirs is a signed delegation chain,
ours a live table entry) stays. Both are capabilities in the ocap sense. We say
"keyhive capability" when we mean theirs.

## What this changes, if accepted

- **CLI:**
  - `drawers add-member/revoke-member` → `drawers share/unshare --role`;
  - `day connect` and `day connections …` replace ADR 043's `day pairing`;
  - `day carts permissions|allow|remove|settings|ls` sit beside `dev-takeover`;
  - `day node status` says carts, not nodules, and `cartridgeId`, not `plugId`.
- **Wire:**
  - `invitePrincipal`/`revokePrincipal` → `addMember`/`revokeMember`;
  - `write` → `edit`;
  - `Authorization.authority` → `capability`;
  - `Rights::DELEGATE` → `INTRODUCE`;
  - the authority service → the actors service (its NSIDs move from
    `…authority.*` to `…actors.*`);
  - ADR 043's methods → `connections.*`.

  Pre-release, so no aliases.
- **ADRs:** 038's "keyhive's standard verbs" fixed to `edit`; 043 renamed to
  "client connections"; 022, 037 and 044 use the developer terms above.

## Open questions for the operator

1. "connect / connections" for client identities: does it read right to you?
   Alternatives: "link / linked apps", "sign in / signed-in apps".
2. Drawer role names: backup / viewer / editor / manager. Is "manager" right for
   keyhive Admin (Drive says "Manager" for shared drives), or "owner"?
3. Renaming the authority service to the actors service: worth the churn, or
   keep "authority" as the service and give the NSID-prefix sense another word?
4. Cart permissions phrasing: should the CLI show them as human sentences ("can
   read Notes") generated from the requests' Lexicon descriptions?
