# ADR 006: Recovery Principals

**Status:** Proposed.

## Context

A daybook repo's authority is a Keyhive graph. Normal **repo agents** are
Keyhive principals bound to checkouts (de facto to a device, but multiple
checkouts of one repo can live on one device): they sync, author content,
and are cheap to create. But a repo that loses every checkout must be
recoverable. For that, the repo needs a principal that:

* can admit a **new repo agent** into the repo's authority graph after total
  checkout loss;
* has **Admin** authority on the repo's authority surfaces — including the
  account document and sponsorship/recovery-relevant groups — but is *not*
  used as a normal content author or sync actor;
* is created **separately from the daybook app** so its secret never lives
  where the app's secrets live;
* is stable enough to anchor the repo's identity at a relay (the relay needs
  a durable principal it can verify Admin on, rather than a checkout-bound
  repo agent).

The relay ADR (005) depends on this: the relay verifies a recovery principal
has Admin on the account document before accepting an account.

### The current state

* `daybook_core/repo.rs` keeps the repo's identity in a per-checkout secret
  store (`RepoCtx` is the daybook layer). `BigRepo::boot(Config)` is
  separate: it takes `node_identity_seed` as a plain parameter over a
  `scope_key`-isolated sqlite path, so booting a BigRepo with a different
  identity seed is structurally possible at that layer.
* The recovery flow (steps sketched in earlier design notes) leaves the
  principal's creation, storage, and authority scope open.

## Decision

### 1. The recovery principal is a separate Keyhive individual

The recovery principal is a distinct Keyhive individual, physically created
**outside the daybook app** — e.g. in a separate tool/device/wallet — whose
contact card is imported into daybook. Daybook then turns it into a recovery
principal by granting it **Admin** on the repo's authority surfaces:

```text
repo root group            → recovery principal: Admin
repo agents group          → recovery principal: Admin (may add new agents)
recovery group             → recovery principal: Admin
account document           → recovery principal: Admin
sponsorship group          → recovery principal: Admin (may adjust retention)
```

The recovery principal is granted Admin on the repo's authority groups
**only**; it is never granted content-document Edit directly. Note that
access is transitive: if a recovery group holds Admin/Edit on a content
document, the recovery principal reaches it through the group. It is
created **per relay by default** — one recovery principal per relay the
repo is uploaded to — so that no single recovery secret spans every relay.
### 2. The relay's anchor identity

The relay's account anchor is the recovery principal, not a per-device repo
agent. When the relay verifies an account, it checks the recovery principal's
Admin on the account document (ADR 005 §1). Optionally the relay can flag a
recovery principal that is being misused as a repo agent (suspicious sync
activity from the recovery key).


### 3. Recovery flow

Recovery is **costly**: it requires rotating keys (the new repo agent)
across the authority surfaces and may require checkpoint commits for
causal coverage. It also **covers only documents in the sponsored set**
— a document the relay never retained is not recovered from the relay.
The flow:
```
1. User unlocks recovery secret/method (separate place).
2. Client reconstructs the recovery principal.
3. Client opens the repo with the recovery identity — a throwaway big_repo
   booted from the recovery secret keys.
4. Recovery principal grants a newly-minted repo agent Admin on the repo's
   authority surfaces (repo agents group, content groups, account doc,
   sponsorship groups).
5. Client ensures causal coverage on every document (materialize/reconcile
   each sedimentree) so the new agent sees the full frontier.
6. New repo agent becomes the normal local principal; recovery principal
   returns to dormant state.
```

### 4. Throwaway identity: a separate recovery boot

Recovery runs as **separate code**, not a layer over `RepoCtx::open` — a
dedicated recovery path that boots a throwaway BigRepo with the recovery
identity over the same sqlite layout, confined to the recovery window.
The exact API shape is out of scope for this ADR.
### 5. Recovery principal storage

The recovery secret does not live in the daybook app's secret store. Daybook
holds only the **contact card** (public identity) for Admin grants. The secret
lives in the separate creation place (wallet/device/paper), so total app
compromise does not grant repo recovery.

---

## Consequences

### Positive

* Total-device-loss recovery is possible without the recovery secret being in
  the app.
* The relay has a durable, verifiable anchor identity that is not a
  checkout-bound repo agent.
* Admin-grant-only keeps the recovery principal out of the authoring path
  (no content provenance from the recovery key); per-relay principals keep
  one relay's compromise from spanning others.

### Costs & Trade-offs

* Recovery requires dedicated recovery-boot code (separate from
  `RepoCtx::open`) and N key rotations plus possibly checkpoint commits —
  recovery is costly and should be rare.
* The recovery principal must be created by the user in a separate place
  before first repo loss; daybook must import the contact card and grant
  Admin as part of repo init.
* Relay verification of the recovery principal's Admin requires the relay to
  read the account document's membership (it already does).


---

## Open Questions

1. **Where is the recovery key created** — a separate tool, an existing
   device, a hardware wallet? The creation surface is out of daybook_core,
   but the import contract (contact-card exchange + Admin grant) must be
   specified.
2. **Recovery boot surface**: where does the recovery boot live, and how
   does it get the recovery secret (wallet/device/paper import) without
   touching the normal secret store?
3. **Causal coverage during recovery**: does "ensure causal coverage on every
   doc" mean full materialization of all sedimentrees, or only the authority
   surfaces needed to admit the new agent?
4. **Should the relay itself verify the recovery principal's Admin at
   account-acceptance time** (as 005 §3 states), or only on recovery events?
