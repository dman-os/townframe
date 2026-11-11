# ADR 044: Cartridge configuration documents

**Status:** Proposed (manager, 2026-11-11). It implements the operator's ledger decision D42 ("Config datums"). The shape of the datums and documents is the manager's proposal, to review with the operator.

**Related:**
- [ADR 027: cartridges, nodules and operation contracts](027-cartridges-nodules-and-operation-contracts.md) §4, §6 (today's config datums)
- [ADR 028: cartridge identity, naming and dependencies](028-cartridge-identity-naming-and-dependencies.md)
- [ADR 033: what a node is](033-what-a-node-is.md) (local drawers)
- [ADR 035: references and URLs](035-references-and-urls.md) (`#datumPin`)
- [ADR 038: node orientations](038-node-orientations-and-the-relay-alpha.md) (sisters share configuration)

## Context

Today one document in the node's config drawer holds every installed cartridge's configuration (`big_nodule_core::config`). Per cartridge, it holds:
- `nodule.install/<cartridgeId>`: the whole published cartridge envelope, the selected revision, and `enabled`;
- `nodule.settings/<cartridgeId>`: an untyped `values` map;
- `nodule.grants/<cartridgeId>`: one datum holding every drawer and method grant;
- `nodule.substitution/<cartridgeId>`;
- `blob/nodule.artifact/<cartridgeId>/<key>`: pins for the handler artifacts.

The operator rejected four things about this (D42):
1. **The envelope is copied into `install`.** Enabling a cartridge already pins its document in the registry drawer, so `install` should reference it: a full URL with heads.
2. **The settings are an untyped map.** That is a step back from daybook's plugs, which declared their own typed config facets (`daybook_types` manifest). A cartridge should declare its settings datums in its Lexicons, and configuration then *is* those datums.
3. **Grants are one datum per cartridge.** Every datum is signed as a whole. One datum per grant records who granted each thing, and grants made on different devices don't collide.
4. **Everything is per node.** Settings and grants should follow the user to sister nodes (an email cartridge's mailbox drawer and its settings are usable on a new sister right away). Only whether a cartridge is enabled, and at which version, is per node.

A fifth problem follows once settings become datums the nodule reads. If every cartridge's settings sit in one document, granting a nodule read on it shows it every other cartridge's settings too.

## Decision

### 1. Two kinds of document

| Document | Where | Holds | Synced to sisters |
| --- | --- | --- | --- |
| **Node config** (one per node) | the node's config drawer, as today | `nodule.install/<cartridgeId>`, artifact pins | no |
| **Cartridge config** (one per installed cartridge) | a new **shared config drawer** | the cartridge's settings datums and its grant datums | yes, once sisters exist |

Until sisters exist (ADR 038), the shared config drawer is a local drawer created at boot like the registry drawer. Its sync policy is the sister work's to decide. Nothing in this ADR depends on it.

The node config document records, per cartridge, the URL of its cartridge config document. A sister that installs the same cartridge finds that document through the shared drawer, rather than creating a new one.

### 2. `install` references the cartridge

`nodule.install/<cartridgeId>` becomes `{ cartridge: #datumPin, enabled, configDoc: docUrl }`.
- `cartridge` is a pin (ADR 035 `#datumPin`, URL plus heads) to the cartridge's document in the registry drawer. Its heads select the revision. Holding the pin is what keeps that version.
- `revision` and `envelope` go. The admission cache keeps what the reconciler needs from the pinned version, keyed by the pin's heads.
- The artifact `Blob` pins stay in the node config document: whether the bytes are held is per node.

### 3. Settings are the cartridge's own datums

A cartridge's Lexicons declare which of its records are settings: a manifest list of settings NSIDs, next to `implements` and `requires`. Each settings record is an ordinary datum in the cartridge config document, under its own NSID and slot (for example `org.example.photos.settings/main`). It is admitted against the cartridge's Lexicon like any datum, with no `values` map. A cartridge with no settings declares none.

The nodule reads its settings with ordinary datum reads. At admission it gets **read** on its own cartridge config document (a doc capability, not drawer access), so it sees its own settings and grants and nothing else.

### 4. One datum per grant

Grants are datums in the cartridge config document, one per grant, with the grant as the slot:
- `nodule.grant/drawer:<drawerId>`: `{ access }`, where access is `relay|read|write|admin`;
- `nodule.grant/method:<nsid or authority>`: `{ privileged? }`. The privilege bit is refused at mint for nodules, as today.

The slot is the grant's identity. So the same grant made on two devices converges to one datum, and two different grants never touch the same datum. Every grant datum carries its own signature, so its author is recorded. Revoking a grant deletes its datum.

Grant keys use only portable ids (drawer ids, NSIDs), never per-node actors, so they mean the same thing on a sister. The nodule's actor and the capabilities minted from its grants are rebuilt on each node, as today.

### 5. The reconciler reads both

`NoduleConfig` hydrates from the node config document plus every cartridge config document that an `install` names. The state actor watches both kinds through the wake-up watch (`DatumStoreHandle::watch`). A change to a cartridge config document reconciles that cartridge only.

### 6. Writes

- Writing `install` and grant datums is privileged (the existing nodule-managing methods: `admitNodule`, `setNoduleEnabled`, and the grant setters, which become per grant).
- Settings writes are ordinary datum writes. Who may write a cartridge's settings is decided by the capability on its cartridge config document. Whether the nodule itself may write its own settings is open question 1.

### 7. Substitution is out of scope

`nodule.substitution` is replaced by the routing/dev-hook split decided in D42, in its own change. Until then it stays in the node config document unchanged.

## Consequences

- `big_nodule_core::config` breaks: `NoduleSettings.values`, `NoduleGrants` as one datum, and `NoduleInstall.envelope`/`revision` go. There's no migration: pre-release, and no node with nodules has shipped.
- `setNoduleDrawerGrants`/`setNoduleMethodGrants` become per-grant methods (`grantNodule`/`revokeNodule`, or datum writes through a privileged handle; to be decided in implementation by what the cabinet API makes simplest).
- A cartridge manifest gains its settings NSIDs. `big_ndk`'s `CartridgeBuilder` emits them.
- intxt's install flow changes with it (admit, grant, enable).

## Open questions

1. May a nodule write its own settings (an app saving its preferences), or only read them?
2. Default values: are settings with no datum the Lexicon's defaults, as with atproto's `default`, or does admission write an initial datum?
3. Uninstall: does deleting the node's `install` leave the cartridge config document (so reinstalling or a sister keeps the settings), with deletion as a separate, explicit act? The proposal is yes.
4. The shared config drawer's name and its relation to ADR 038's "synced configuration drawer" for orientations and preferences. Probably the same drawer.
