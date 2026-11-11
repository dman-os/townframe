# ADR 035: References and URLs

**Status:** Proposed.

**Supersedes:** the `db+datum:` grammar of ADR 007 ("Datum addresses have no URI authority") and ADR 021 §2, the sibling `refHeads` pin (ADR 007 plug manifests, ADR 021) and the `x-automerge: "reference"` marker (ADR 021; references are Lexicon defs, §2.6), the `db+actor://` form of ADR 020/022, and the `db+blob:` form of ADR 001 ("Blob Identity and URIs"). **Keeps:** the semantics those records already gave references (§2 gathers them in one place). **Related:** ADR 020 (datum identity, signed claims), ADR 022 (authorization), ADR 025 (reference predicates), ADR 026 (blobs), ADRs 036, 040 and 041 (the first heavy users). Discussion and evidence: `docs/scratch/intxt-ledger.md` N3–N6, A1, D24, D25.

## 1. Context

The repository uses six `db+…` schemes, inconsistently:

| Scheme | Uses | Form |
|---|---|---|
| `db+facet:` | 124 | `db+facet:///<doc>/…`, the older name for datum references (dpaths FDRs, dayledger `FacetRef`, `day plugs` arguments) |
| `db+datum:` | 62 | `db+datum:/<doc>/<tag>/<slot>` (`big_cabinet_core/reference.rs`) |
| `db+blob:` | 40 | `db+blob:///<multihash>[?via=…&size=…]` |
| `db+actor:` | 13 | `db+actor://<node key>/<path>` (`big_cabinet/authority/actor.rs`) |
| `db+command:` | 3 | `db+command:///@scope/plug/command` |
| `db+iroh-clone:` | 2 | `db+iroh-clone:<ticket>` |

Problems:

- `a+b:` conventionally means protocol *a* over transport *b* (`git+ssh`, `svn+ssh`, `coap+tcp`). Ours means namespace plus resource kind.
- The forms disagree on authority: empty (`:///`), a node key (`://`), none (`:/`), opaque (`:<ticket>`).
- `db+datum:/…` has a hierarchical path, so WHATWG parsers normalize dot segments (`db+datum:/zDoc/tag/../x` parses to `/zDoc/x`); the code must forbid `.`/`..` slots for that reason.
- A slot may contain `/`, so it runs to the end of the path and nothing can follow it; there is no way to point inside a datum.
- Versions are a separate `refHeads` sibling in hex, and the text form (CLI, links) has no way to carry a pin at all.
- The rules that make a reference mean something (it is weak, it has no authority, `self` is relative, only declared positions are references) are spread over four ADRs and stated per scheme.

Facts this decision relies on:

- Custom schemes, including ones with `+`, parse per the WHATWG URL Standard in Chromium 156, Firefox 158, Node 24 and Deno (tested; Chrome adopted standard non-special URL parsing around M130). Safari not tested. A scheme followed by `:` and no `/` gives an opaque path, which no parser normalizes.
- automerge-repo addresses inside documents with `automerge:<docId>[/<path>][#<heads>]`, where a path segment is `@n` (list index), `{json}` (list element match), `[start-end]`/`[cursor]` (text cursor range) or a URL-encoded map key, and heads are bs58check, `|`-joined and sorted (`AutomergeUrl.ts`, `subdoc-handles/`; to be re-verified against source before implementation).
- A datum is stored at the Automerge path `datums/<tag>/<slot>`; the slot is bytes, spelled as a Go string-literal body so that it can be an Automerge map key (`docs/wiki/docs.md`).
- Actor URLs are signed: an actor's attribution certificate binds its exact actor URL (ADR 020 §5).

## 2. What a reference means

These rules hold for every kind in §3. Most were already decided for `db+datum:`; they are restated here once so new kinds inherit them.

1. **A reference is an address, not a capability.** It grants no access, implies no retention, and does not assert that the target exists or is reachable (ADR 007). Being allowed to read a datum that holds a reference does not authorize dereferencing it (ADR 022). An unresolved reference is a normal state that readers show, not an error: the target may be private, not yet synced, or gone.
2. **Identity carries no locator.** No reference has an authority component. Documents live in one global space: a document id names the same document on every node, and any node holding it can serve it. Drawers are access and collection contexts, nodes and relays are providers, a git remote is a fetch hint; none of them identifies the target, and none of them goes into the identifying part of a URL. Putting the node in the authority was rejected for that reason (ADR 007): it would make one document's references differ by where they were found. If locator hints are ever needed they are query keys, which leave the path (the identity) unchanged; none are designed until a feature needs one.
3. **The parts of a URL have fixed roles.** The path names the primary resource. The query selects a version of it. The fragment selects a position inside it, is interpreted by the target's type, and is not part of the target's identity (RFC 3986 §3.5; JSON Pointer fragments, RFC 6901 §6; W3C Media Fragments). An API that takes a whole resource rejects a fragment rather than ignoring it.
4. **No version means "current at read".** A reference without a version follows its target; a reference with one is reproducible. A reader that cannot reach the pinned version reports it as pending; it does not fall back to current, because that silently shows different content under the same reference.
5. **`self` is relative to the containing document.** It lets a datum copied into a branch keep its internal references (ADR 007). `self` is never a concrete id and is resolved by the reader against the document the datum was read from.
6. **References are declared, never sniffed.** Inside a datum, a reference is a field whose Lexicon type refers to one of the shared reference defs (`org.example.daybook.defs`, registered at boot with the core datums and owned by the nodules lane), in the way an AT Protocol record refers to `com.atproto.repo.strongRef`:
   - `#datumRef`: a string, `format: uri`, holding a `dtxt:datum:` address with no query and no fragment. It names a whole datum or document.
   - `#datumLocation`: the same, with a fragment allowed (§2.3). It names a position inside a datum, and the index keys it by its datum part, so "what targets this datum" finds comments that point inside it.
   - `#datumPin`: an object `{ "ref": <datumLocation>, "heads": [<head>, …] }`, a location at a version (§2.8). `heads` is optional; without it the object follows its target (§2.4), and when present it is non-empty (an empty list is refused, not read as current), so a field that may or may not be pinned has one type, not a union of a string and an object.

   A field holding one is `{"type": "ref", "ref": "org.example.daybook.defs#datumRef"}` (or an array of them). Because the declaration is part of the published Lexicon, peers, cartridge records and tools that never see a node's storage annotations still know which fields are references. Writes refuse a value there that does not parse, and indexes find every reference from the Lexicon alone (the `datum_ref` index; ADR 025 reference predicates), which is what makes backlinks ("what points at this?") a query instead of a scan. `x-automerge` describes Automerge storage forms only (text, list, counter); it no longer marks references. A string that spells a URL in a field typed as a plain string is data.
7. **One canonical spelling.** Each reference has exactly one canonical string: the encoding rules below, heads sorted by their hash bytes (the order Automerge's `get_heads` returns, whatever their text encoding; a base58 string sort differs when encodings differ in length), percent-escapes uppercase, and nothing escaped that the URL grammar does not require (`:` and `@` stay raw in a path, as RFC 3986 `pchar` allows). Writers emit the canonical form, so equal references compare equal as strings and indexes can key on them. Parsers accept non-canonical input and canonicalize.
8. **Stored references split off the version.** The URL with `?heads=` is the text form: what a CLI prints, a link carries and a person pastes. Inside a datum a pinned reference is a `#datumPin` object: `ref` holds the URL without its query and `heads` the heads, canonical (§2.7) and multibase base58btc. Re-pinning to newer heads is the common write, and keeping the heads in their own field means it rewrites only that field, not the string naming the target. The fragment stays in `ref`: it changes when the position changes, not when the pin moves (an Automerge cursor survives revisions, so re-pinning an annotation leaves its fragment alone). The pin is inside the declared object rather than a sibling field (the earlier `refHeads`) so that the Lexicon itself says which address a set of heads pins; a sibling would tie them only by a naming convention, which is sniffing (§2.6). A reference with no version is a plain `#datumRef` or `#datumLocation` string.

## 3. Decision: one scheme, kinds as namespaces

```
dtxt:<kind>:<opaque path>[?<query>][#<fragment>]
```

The scheme is `dtxt` (no `+`, no `db`; D24). The kind follows a colon, as DID methods (`did:key:…`) and URN namespaces (`urn:isbn:…`) do: a kind is a namespace, not the root of a hierarchy. Kinds are full words. Nothing follows the scheme's colon with `/`, so every path is opaque and no parser normalizes it. A new kind is an amendment to this ADR and inherits §2.

Parsers reject unknown kinds wherever a position declares the kinds it accepts. A position declared to accept any `dtxt:` reference carries an unknown kind unchanged, as Lexicon open unions carry unknown members.

### `datum`

```
dtxt:datum:<doc>/<tag>/<slot>[?heads=<head>|<head>…][#<path inside the datum>]
dtxt:datum:zDoc/org.example.dpath//photos/beach.jpg?heads=zAb…|zCd…#meta/@0
```

- **Path** names a datum: `<doc>` is a document id in its text form (multibase base58btc) or `self`; `<tag>` contains no `/`; `<slot>` is the slot's stored string (below) percent-encoded, with `%`, `?`, `#`, controls and non-ASCII always encoded, `/` left as is, and nothing else escaped that the grammar allows raw. The slot runs to the end of the path; nothing in the path follows it.
- **Query** pins a version: `heads=` lists the document heads the reference was made at, each in multibase base58btc (`z` prefix, as our other ids; D25), sorted, `|`-joined. No other query key is defined. Keys are added when a feature needs one (a UUID alias for consumers that care about concurrent creation, ADR 020 §4; a locator hint, §2.2), never to the path. An empty `heads=` is invalid: an empty frontier is the document before any change, never a useful pin. A reference names a concrete document, so a branch is referenced by its own id (ADR 007); there is no "branch of" selector.
- **Fragment** selects a position inside the datum's value using automerge-repo's path grammar: `@n`, `{json}`, `[cursor-cursor]` or `[cursor]`, and percent-encoded map keys, `/`-separated. This holds only because the datum codecs keep the JSON value's structure one-to-one with its Automerge storage (ADR 021): a fragment means the same position in both. A cursor is spelled as Automerge spells it, `[-]<counter>@<actor hex>`; see "Automerge actors" below. In the URL, the grammar's delimiters that RFC 3986 does not allow in a fragment are percent-encoded like any other character: `[` `]` `{` `}` `"` and space become `%5B` `%5D` `%7B` `%7D` `%22` `%20`, so a cursor range is `#content/%5B12@a3f-40@a3f%5D` and a list match is `#toolCalls/%7B%22id%22:%22toolu_01%22%7D`. The reference defs validate `format: uri`, which refuses the raw form, and readers decode it for display. Hand-written positions use `lines` (ADR 041) instead, so nobody types these.

**Slots.** A slot is bytes, but it is stored as an Automerge map key (`datums/<tag>/<slot>`), and map keys must be Unicode strings. The stored string is the slot as a Go string-literal body: valid printable UTF-8 is kept as is, `"` and `\` are backslash-escaped, and invalid bytes and non-printable characters are written as `\xNN`/`\uNNNN` escapes (`docs/wiki/docs.md`). Most slots are therefore their plain UTF-8 text, and only odd bytes look escaped. The URL then percent-encodes that stored string, as automerge-repo encodes any map key, so the URL is ASCII. The two encodings do different jobs: Go escaping makes bytes a valid key, percent-encoding makes a key a valid URL. `/` stays raw in both because slots are expected to use it as a separator (dpaths, `drawer.metaDoc/blobInventory/<name>`), and readable slots are worth more than a bounded segment; this is why the slot must be the last thing in the path.

**Automerge actors.** An Automerge actor id is the random identifier of one writer's change stream inside a document; Automerge prints it in hex. It is unrelated to our `actor` kind below (an authority principal, ADR 022) and is never written as a `dtxt:actor:`. Automerge 0.12 added *authors* on top of actors (opaque bytes, one author to many actor ids, a new actor id whenever the author changes); actors and the cursor format are unchanged. Cursors are an Automerge text-object detail, not part of a generic path grammar, so the fragment keeps Automerge's spelling, hex included, rather than re-encoding it; partial compatibility with automerge-repo is worth little, and this part costs nothing to keep.

We diverge from automerge-repo in one place: heads go in the query, not the fragment, because a datum is a resource with its own versions and the fragment is needed for positions inside it.

Code has two types. A **datum reference** has no fragment and is what every API taking a datum accepts (branch origins, write targets, reference positions in datum values). A **location** may carry a fragment and is what annotation targets and other selectors accept. Both carry the optional heads. A location without a fragment is the whole datum.

### `actor`

```
dtxt:actor:<node key>[/<path>]
```

The node's public key in its multibase text form, then the actor path (ADR 022): the operator is the bare node key; every other actor's path names its ancestry (`dtxt:actor:zNode/agents/<session>`). Replaces `db+actor://`. It has no query or fragment.

### `blob`

```
dtxt:blob:<multihash>
```

Content identity only, with no query hints. In ADR 026 a blob is reached through its `Blob` datum (digest, MIME type, length; the cipherBlob is found by naming convention and bytes through drawer inventories), so a datum that refers to a blob holds a `dtxt:datum:` reference to the `Blob` datum. A bare blob URL names bytes without the context to fetch or decrypt them. `?size` duplicated `lengthOctets`, `?via` is superseded by the naming convention, and `?kem`/`?keyring` were already withdrawn.

### Dropped

`db+facet` becomes `dtxt:datum`. `db+command`, `db+iroh-clone` and the `future+…` placeholders are removed and get designs of their own when needed; a command is a plug contribution resolved through manifests, not a document address, and a clone ticket is a locator (§2.2).

## 4. Consequences

- `big_cabinet_core/reference.rs` is rewritten for the new grammar and both types. The dot-segment rule for slots goes; slots must still be non-empty.
- The sibling `refHeads` field is replaced by the `#datumPin` object (§2.8), and heads change from hex to multibase base58btc. Text forms (CLI output, links) carry the pin as `?heads=`; converting between the two is the reference type's job.
- The `x-automerge: "reference"` and `"location"` markers go; ADR 021's table and the reference index read the Lexicon's ref targets instead (nodules lane, 2026-11-11).
- **Actor URLs are inside signed attribution certificates** (ADR 020 §5). Changing the scheme changes the signed bytes, so existing certificates are reissued under the new URL (the node holds the keys that sign them) or the verifier accepts the old spelling for certificates issued before the cut-over. There is no deployed data outside development nodes today, so reissue is the default.
- `Blob.urls` is legacy (the encryptor no longer writes it; `big_blobs/documents.rs` still falls back to it and accepts `db+blob` and `daybook-blob`). It is retired with daybook_core's migration.
- Every `db+…` occurrence (about 240, including the Kotlin app; half are `db+facet` in FDRs and the dayledger plug) is migrated or deleted with its feature.

## 5. Target constraints

A reference def can only say "a `dtxt:datum:`". A schema does not constrain the target's tag, and Cabinet does not enforce one. Cabinet could only check at write time, and a reference is weak (§2.1): its target may not exist yet, may be unreadable to the writer, or may change tag meaning later, so a write-time check would reject legitimate references and still not keep them valid. Readers check instead: a reference whose target has an unexpected tag is rendered as unresolved, like any other reference that doesn't resolve (operator, 2026-11-10). A schema may document the expected tags for tools and renderers; that note is not enforced.

Settled by the operator on 2026-11-10: cursors keep Automerge's hex spelling; slots keep raw `/`; UUID aliases and locators are query keys if ever needed, not path parts; query keys are not formalized beyond `heads=`; stored references keep the pin in its own field, apart from the address (since 2026-11-11 inside a `#datumPin` object, §2.8); target tags are reader-checked, not enforced.
