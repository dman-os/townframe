# Micro composition: desktop-first working sketch

Exploratory, not a second normative architecture. ADR 014 carries the contract.
The runnable consumption sites are `dayframe_app/application.rs`, `settings/`,
and `sample/`.

```text
Desktop App
  ThemeRoot
    Application
      startup roots: Settings, Sample
      native root mounts
      mock-node development controls (not sample authority)

Host {
  instances: runtime incarnations + owned effects
  dependencies: committed typed bindings
  component references + mounts
  hosted collections: typed read / insert / own-entry handles
  document service: sparse projection + optimistic transactions
}

SettingsContext {
  instance
  sections: host-provided storage<SettingsSection>
  roots: granted insertion<RootMount>
}
SettingsMicro {
  sections: read<SettingsSection>
  published API: SettingsContribution
  root component: list/detail layout + navigation
}
SettingsSection { title, detail: ComponentRef }
SettingsContribution { insert(section) -> OwnedEntry<SettingsSection> }

SampleContext {
  instance
  settings: SettingsContribution
  config_document: authorized DocumentRef
}
SampleMicro {
  config: config_document.facet<SampleConfig>(tag, keyId)
  detail: published native component consuming config
  section: settings.insert({ title, detail })
}
```

Settings mounts content but never receives Sample's config-document authority.
The host owns section storage; Settings owns section vocabulary. A contributed
component is a mediated reference, not an enum of all known features. Native
closures stay in `native.rs`.

## Document waist

```text
DocumentAccess { opaque, context-bound authority }
  document(id, branch) -> DocumentRef
  watch({ documents: Set<Id>, branch, facets: Tags | Keys }) -> DocumentWatch
  mutate(route, valueIntent) -> mutationId

DocumentWatch {
  current authorized snapshot
  atomic subscription: initial snapshot + coalesced snapshot updates
  select(newSelection)
  refresh()
  close()
}

Host document service {
  mock FacetSet membership lookup: doc + branch + ordinary tag -> keys/tombstones
  targeted value hydration
  shared cached base + existing Collection optimistic overlays
  in-flight refresh waiters: route -> watch + selection generation
  bounded unretained LRU; active watches and pending writes pin values
  per-watch filtered mirrors and delivery revisions
}
```

A side channel may supply IDs or authorized document bindings. Knowing an ID
does not mint a grant. Exact-key watches represent absent keys; tag watches do
not become document discovery. Permission filtering precedes hydration and
delivery, including when the shared cache is warm. Matching manifest ACL entries
union independent rights. Missing configuration owner means the invoking plug;
explicit owners resolve to their own config documents before grant issuance.

Selections preserve N documents and N tags/keys. No well-known-tag distinction.
No global document collection, search service, or loading all document values.
The mock keeps source storage separate from retained projection values; it is
not a production drawer adapter.

## Ownership

- Startup owns roots; downstream creation needs explicit authority and an owner.
- Multiple instances are allowed. Stable configured names, fresh runtime
  incarnations, and mounts are distinct; launching once is not enforced
  singleton policy.
- Consumer recovery precedes provider teardown. Failed/cancelled activation
  withdraws partial effects.
- Closing a supplier invalidates its mounts, removes entries, revokes watches,
  and rejects stale handles.
- Submitted writes survive watch/instance closure; rejection rolls back and
  acceptance commits locally.
- Overrides, when introduced, are independently owned records—not snapshot
  restoration.
- Editor physics and themes are independent surfaces; neither is a host
  primitive in this slice.

## Future WIT adapter

Map opaque native authority handles to guest resource-table entries. Lower route
sets and snapshots to ordinary records/lists. A guest resource owns its
subscription and exposes asynchronous next/close; Tokio receivers and native
renderer factories do not cross the component boundary. The protocol can support
routine wrappers later without changing the existing routine world in this work.

The future node adapter consumes `index/facet_set.rs`, preserving bounded reads,
replay completion, removals, source cursors, and committed progress, and
hydrates through drawer. Document selection currently filters a full frontier; a
stronger index can come later. Source ordering is not mirror revision ordering.
Actual WIT bindings, core wiring, public discovery, and generalized query
predicates are deliberately not implemented by this mock-backed slice.
