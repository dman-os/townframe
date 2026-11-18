# Micro surface section (revision 3 of the cartridge manifest)

Replaces the "Micro section" of `cartridge-surfaces.md` and sections 3-4 of `micro-vocabulary.md`. Nothing built. The shared part (identity, `lexicons`, `components`, `permbinds`, `settings`) and `Reach { uses }` are unchanged; the cartridge carries `surfaces: Vec<Surface>`, a `$type` open union (`#nodule`, `#micro`, unknown carried unread).

## 1. VS Code, from primary sources

- `contributes` is "a set of JSON declarations that you make in the `contributes` field of the `package.json`" ([contribution points](https://code.visualstudio.com/api/references/contribution-points)). It is an object **keyed by contribution point**; each value is that point's own shape: `commands` (array of `{command, title}`), `menus` (keyed by location such as `editor/title`, items `{command, when, group}`; `group` "defines sorting and grouping"), `views` (keyed by container id, items `{id, name, when}`), `viewsContainers` (keyed `activitybar`/`panel`, items `{id, title, icon}`), `configuration` (`{title, properties}`, a settings schema).
- **Defining vs contributing.** The docs describe contributing to host-defined points. The one visible defining case is `viewsContainers`: an extension declares a container `id`, and `views` (its own or another extension's) are keyed by that id. Defining arbitrary new points is not documented there.
- `activationEvents` is "an array of the activation events for this extension" ([manifest](https://code.visualstudio.com/api/references/extension-manifest)); names `onCommand:x`, `onView:id`, `onStartupFinished`, `*` ([activation events](https://code.visualstudio.com/api/references/activation-events)). Since 1.74 "commands contributed by your extension do not require a corresponding `onCommand` activation event": activation is derived from the contribution. That is our lazy link.
- `main` is "the entry point to your extension", `browser` "the entry point to your Web extension"; `extensionDependencies` is an array of extension ids.

**Taken from VS Code:** `contributes` (the field), keyed by point; contribution/contribution point; `views` and `view`; `activation`; "defining a point" via an id others key by (our `collections`). **Not taken:** `commands` (collides with cabinet commands and the command ledger), `extensionDependencies` (our `dependencies` are delivery hints only, and suppliers are routes).

## 2. The three concerns of `requires`, carried by one declaration

Today's `requires` entry says authority ("may call Y"), composition ("Y is supplied by X") and lifecycle ("start Y first, keep it"). The author writes **one** thing: a permbind with a necessity, named in `uses`.

| Concern | Where it comes from | Author writes |
|---|---|---|
| Authority | the permbind exists (consent text, grant) and the endpoint's `uses` names it | the permbind plus the `uses` key (already needed) |
| Composition (which supplier) | routes (ADR 045) and the lockfile choose a supplier of the permbind's target NSID. Default: the sole provider, e.g. the same cartridge's nodule. Assumption: the "sole provider wins" default is the nodule rule today | nothing normally; an override is a route/lockfile entry, never in the micro |
| Lifecycle | derived from the permbind: `required` `#method`/`#mount` means the supplier is started first and held while this micro lives (a strong edge); `optional` means start without it. Unresolved required means `Unconfigured`, not a fault | nothing |

`uses` is per endpoint but an incarnation is per micro, so lifecycle takes the union of `required` permbinds over all the micro's endpoints, since one incarnation serves all. A micro with one cheap endpoint and one dependency-heavy endpoint should be two micros (which is the reason several are allowed).

## 3. Vocabulary: old to new

| Old | New | Why / precedent |
|---|---|---|
| `publications` | `endpoints` (map by name) | Last proposal, kept: ADR 031 already says "lazy endpoint"; no authority sense (unlike `capabilities`); VS Code has no equivalent. Rejected: `offers` (Fuchsia `offer` means routing to a child), `services` (node services), `commands` (cabinet). |
| `kind: call \| presentation` | `$type: call \| view` (open union) | Members differ in data (`call` has `implements`), so a union, `empty` member for `view`. `view` is VS Code's `views`; the WIT interface name `presentation-events` is unchanged. |
| `requires`, `Request` | permbind (`#method`/`#mount`) plus `uses` key | Section 2. `Request` disappears; `binding` is the permbind key, no second local name. |
| `resolution` | removed | ADR 049 item 4; runtime requests are a host service. |
| `facet`, `FacetAccess`, `configuration_requests` | `#datum` permbind on a `settings` Lexicon; per-verb `uses`; `key` narrowing (was `key_id`) on the permbind | Datum is the noun in ADR 049. |
| `collection_requests` | `#datum` permbind whose `target` is `LexRef` of a collection token | Same permbind list. |
| `collection_contracts` | `collections` map keyed by the collection's `token` def (`nsid#def`) | Manager review item 1; VS Code's `viewsContainers` pattern: an id others key by. |
| "contract" (three meanings) | gone as a word. Call: the **method Lexicon** (`implements`). Collection: the **collection definition**. Ref: `LexRef`. | One word was three things. |
| `offered`, `CollectionCapability` | `verbs` (open string, `knownValues`: `read`, `register`, `causalWrite`, `domainCommands`) | Frees "capability" for ocap `CapId`. |
| `capability-ref` (WIT), `published-capability` | `endpoint-ref`, `published-endpoint` | Same reason. |
| `contributions[]` with `binding` | `contributes`, keyed by **`#contribute` permbind key**, then row key | VS Code shape: keyed by point. The permbind names the target either way. |
| `payload` | `value` | It is validated against the collection's `value` def. |
| `attachments{index, publication}` | `links{index, endpoint}` | One slot-to-endpoint link; avoids the three "attachment" shapes. |
| `activation` | `activation`, open string (`lazy`, `eager`) | VS Code `activationEvents`; live `eager` stays configuration. |
| `entrypoints.installer` (reserved) | `initialization` (below), driving the installer interface | Reuses the nodule noun. |
| Sidecar `id`/`package_version`/`authority` | cartridge identity | Unchanged from revision 2. |

Identifiers: only Lexicons are NSIDs. A micro's id is `<cartridge id>/micro/<name>`, its endpoint `<micro id>/endpoint/<name>`, its rows `(<micro id>, key)`; none appear in the manifest as strings beyond the local `name`.

## 4. Initialization for micros (option A) versus static rows (option B)

Nodule `initialization` is `[{key, target, mode}]`, run at install/upgrade against an own operation. A micro analogue fills ADR 030 section 10's reserved `installer`: at install or upgrade the host runs a short-lived instance (not an incarnation, no mounted UI, nothing published) of the micro's component through the `installer` WIT interface, once per `initialization` entry (`run(key)`). Its only reach is the entry's `uses`, in practice a `#contribute` handle scoped to its own rows. Rows go to an installation-scope registration set, written atomically and replaced as a set on each run (so upgrades cannot leave stale rows), retracted on uninstall.

| | A: initializer writes rows | B: static `contributes` in the manifest |
|---|---|---|
| Visible at consent before code runs | permbind text only ("adds to Weather's toolbar items") | the rows themselves, previewable |
| Manifest size | small | grows with every button/menu |
| Dynamic or many rows (one per saved place, locale) | natural | impossible |
| Admission | rows validated against the target's `value` def at write; a bad row fails the install, rolled back | validated at admission, statically |
| Needs runtime that exists | an installer host (not built) | nothing new |

**Recommendation: A.** The payload vocabulary belongs to the target and is opaque to the host, so static visibility buys a preview and not an audit; the audit is the `#contribute` permbind either way (own rows only, shown at consent), and the host validates identically. A keeps manifests small, matches the operator's direction that buttons stay out of the manifest, and handles computed rows. B stays specified as the declarative form (`contributes` below) if a pre-run preview is later wanted; the two write the same rows, so authors can mix them.

## 5. Rust types

```rust
pub enum Surface {                       // `$type` open union, unknown carried
    Nodule(Nodule),
    Micro(Micro),
    #[datum(other)] Unknown(UnknownMember),
}

pub struct Micro {
    pub name: String,                    // micro id = <cartridge id>/micro/<name>; unique per cartridge
    pub component: String,               // one component, one instance (singleton)
    pub entrypoints: Entrypoints,        // initialize, presentation, installer: WIT interface per role
    pub activation: Activation,          // open string: Lazy | Eager | Unknown(String)
    pub endpoints: BTreeMap<String, Endpoint>,        // renamed from publications
    pub collections: BTreeMap<LexRef, CollectionDef>, // key = the `token` def naming the collection
    pub initialization: Vec<MicroInitializer>,        // option A (recommended)
    pub contributes: BTreeMap<String, BTreeMap<String, Contribution>>, // option B: permbind key -> row key -> row
    #[serde(flatten)] pub extra: Extra,
}

pub enum Endpoint {                      // open union; each member flattens Reach
    Call { implements: Nsid, reach: Reach },   // a method Lexicon other micros may call
    View { reach: Reach },                     // a mountable UI (empty member otherwise)
    #[datum(other)] Unknown(UnknownMember),    // carried, never served
}

pub struct CollectionDef {
    pub key: LexRef, pub value: LexRef,        // object defs beside the token
    pub selector: Option<LexRef>, pub write_context: Option<LexRef>,
    pub verbs: Vec<Verb>,                      // open string vocabulary
}

pub struct MicroInitializer {            // same Initializer noun as Nodule.initialization
    pub key: String,                     // passed to the installer's run(key)
    pub mode: RunMode,                   // reuses RunMode: Once | EveryStart | Unknown;
                                         //   adds knownValue `everyUpgrade` (install and each version change)
    #[serde(flatten)] pub reach: Reach,  // normally { "<#contribute permbind>": { create, delete } }
}

pub struct Contribution {                // option B
    pub value: serde_json::Value,        // checked against the target collection's value def
    pub links: Vec<Link>,                // row slot -> endpoint of THIS micro, as a lazy link
}
pub struct Link { pub index: u32, pub endpoint: String }
```

`Reach`, `RoutineUse`, `LexRef`, `Permbind` as in revision 2. `Permbind::Mount` and `Contribute` are cartridge-level, so a nodule can use `Contribute` too.

## 6. The Weather micro entry, annotated

Permbinds referenced are the cartridge-level ones from revision 2 (`forecasts`, `prefs` (a `#datum` with `"key": "units"` narrowing), `refresh` (`#method`, required, supplied by the cartridge's nodule), `toolbar` (`#contribute`, target `org.example.shell.toolbar#items`)).

```jsonc
{
  "$type": "org.example.daybook.cartridge#micro",
  "name": "panel",                         // id <cartridge>/micro/panel; a second micro (say "badge") would be another entry
  "component": "panel",                    // key into the shared `components`
  "entrypoints": {                         // WIT interface per host-driven role
    "initialize": "townframe:dayframe/micro-lifecycle@0.4.0",
    "presentation": "townframe:dayframe/presentation-events@0.4.0",
    "installer": "townframe:dayframe/micro-installer@0.4.0"   // role for `initialization`; no longer reserved
  },
  "activation": "lazy",                    // open string: lazy | eager; the live flag is configuration

  "endpoints": {
    "view": {                              // a mountable UI panel
      "$type": "#view",                    // open union member; carries only `uses`
      "uses": {
        "forecasts": { "read": true },     // authority: permbinds named here
        "prefs":     { "read": true, "write": true },     // settings access (was a facet ACL)
        "refresh":   {}                    // AUTHORITY + COMPOSITION + LIFECYCLE in this one line:
                                           //   `refresh` is a required #method, so (1) the micro may call it,
                                           //   (2) the supplier is chosen by routes/lockfile (default: our nodule),
                                           //   (3) that supplier is started first and held while the micro runs
      }
    },
    "current": {                           // a call other micros use ("weather now")
      "$type": "#call",
      "implements": "org.example.weather.current",   // a method Lexicon; callers target it, not this micro
      "uses": { "forecasts": { "read": true } }
    }
  },

  "collections": {                         // a collection other cartridges may fill (we define it)
    "org.example.weather.alerts#items": {  // a `token` def in OUR Lexicon, not a new def type
      "key":   "org.example.weather.alerts#itemKey",    // `object` defs beside the token
      "value": "org.example.weather.alerts#item",
      "verbs": ["read", "register"]        // open vocabulary, unknown carried
    }
  },

  "initialization": [                      // option A: runs at install and every upgrade, no incarnation
    { "key": "toolbar", "mode": "everyUpgrade",
      "uses": { "toolbar": { "create": true, "delete": true } } }
                                           // may add and remove ITS OWN rows in the toolbar collection and nothing else
  ]
  // The row itself is written by the installer code (section 7). Option B would instead say:
  // "contributes": { "toolbar": { "weather": {
  //     "value": { "slot": "toolbar", "order": 2, "label": { "en": "Weather" }, "action": { "$link": 0 } },
  //     "links": [ { "index": 0, "endpoint": "view" } ] } } }
}
```

What the host does with it. At install: admit (every `uses` key is a permbind, the component and the installer export exist, `view` and `current` match the WIT), consent shows "adds items to the toolbar" from the `toolbar` permbind, run `installer.run("toolbar")`, validate the row against `org.example.shell.toolbar#itemKey/#item`, write it to the installation scope. The shell draws the button from `value` with Weather stopped. Pressing it redeems the row's link: the lazy link starts the micro, whose required `refresh` supplier (the nodule, already ready) is held; closing the last strong edge closes the micro and leaves the row.

## 7. Adding a toolbar button, step by step

1. **Know the target.** The shell's Lexicon `org.example.shell.toolbar` defines the token `#items`, with `#itemKey` and `#item` (label, slot, order, a link slot). Read `#item` to know what to write.
2. **Declare the permission** in the cartridge's `permbinds`: `"toolbar": { "$type": "…#contribute", "target": "org.example.shell.toolbar#items", "description": "Adds a Weather button", "necessity": "optional" }`. Optional means the cartridge still installs if the shell has no toolbar.
3. **Give the micro something to open:** an endpoint `view` (`#view`) with the `uses` it needs. Nothing else to write for suppliers or start order.
4. **Declare the initializer:** one `initialization` entry, `{ "key": "toolbar", "mode": "everyUpgrade", "uses": { "toolbar": { "create": true, "delete": true } } }`.
5. **Write the installer function** (a few lines in the MDK): given `"toolbar"`, `cx.contribute("toolbar").put("weather", Item { label, slot, order, action: link_to("view") })`. The MDK handle's `put` validates and scopes the key to this micro.
6. **Install.** The row appears; the micro is not started. Uninstall retracts it.

Open points: (1) `MicroInitializer` omits `target` because the installer interface is one export called with the key; if initializers should name separate exports, give them `ExportRef` like routines; (2) the `everyUpgrade` run mode is a new known value on `RunMode`; (3) `collections` could live cartridge-level (a nodule might own one) and is micro-level only because the manager review put it there; (4) `{"$link": 0}` replaces `{"$attachment": n}` in values and needs the same CBOR tag.

## Operator sign-off (2026-11-18)

Signed off as the direction ("keep it as it is and improve it as we go"), with these alignments folded in:
- **One `Initializer { key, mode }` and one `RunMode` for both surfaces** (`once`, `everyStart`, and new `everyUpgrade`), and one typed input record for both, `{ key, trigger: install | upgrade | start, previous, current }`, closing the nodule to-do that initializers receive `{}`. A nodule initializer targets one of its routines; a micro initializer calls the micro's installer export with the record.
- **Supply goes through dispatch for both** (ADR 045): a call to an NSID goes to its only claimant, several claimants are `Ambiguous` until a node route picks one. A micro's `#call` endpoint registers as a dispatch provider like a routine, so micros and nodules supply each other under one rule. No separate "default supplier".
- **Collections stay in the micro section** until a nodule needs one.
- **Naming:** what other cartridges refer to without knowing the provider is an NSID or `nsid#def` (methods, collection definitions); what only this manifest refers to is a local key (permbinds, endpoints, initializer keys), which the host turns into ids under the cartridge; runtime ids are the host's and never authored.
