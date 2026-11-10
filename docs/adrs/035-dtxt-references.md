# ADR 035: `dtxt:` references

**Status:** Proposed.

**Supersedes:** the `db+datum:` grammar of ADR 007 ("Datum addresses have no URI authority") and ADR 021 §2, and the `db+blob:` form of ADR 001 ("Blob Identity and URIs"). **Related:** ADR 020 (datum identity), ADR 026 (blobs), ADR 036 (the first heavy user). Discussion and evidence: `docs/scratch/intxt-ledger.md` N3–N6, D24.

## 1. Context

The repository uses six `db+…` schemes, inconsistently:

| Scheme | Uses | Form |
|---|---|---|
| `db+facet:` | 124 | `db+facet:///<doc>/…`, the older name for datum references (dpaths FDRs, dayledger `FacetRef`, `day plugs` arguments) |
| `db+datum:` | 62 | `db+datum:/<doc>/<tag>/<slot>` (`big_cabinet_core/reference.rs`) |
| `db+blob:` | 40 | `db+blob:///<multihash>[?via=…&size=…]` |
| `db+actor:` | 13 | `db+actor://<node key>/<path>` |
| `db+command:` | 3 | `db+command:///@scope/plug/command` |
| `db+iroh-clone:` | 2 | `db+iroh-clone:<ticket>` |

Problems:

- `a+b:` conventionally means protocol *a* over transport *b* (`git+ssh`, `svn+ssh`, `coap+tcp`). Ours means namespace plus resource kind.
- The forms disagree on authority: empty (`:///`), a node key (`://`), none (`:/`), opaque (`:<ticket>`).
- `db+datum:/…` has a hierarchical path, so WHATWG parsers normalize dot segments (`db+datum:/zDoc/tag/../x` parses to `/zDoc/x`); the code must forbid `.`/`..` slots for that reason.
- A slot may contain `/`, so it runs to the end of the path and nothing can follow it; there is no way to point inside a datum.
- Versions are a separate `refHeads` sibling in hex, not part of the reference.

Facts this decision relies on:

- `+` in a scheme and custom schemes generally parse per the WHATWG URL Standard in Chromium 156, Firefox 158, Node 24 and Deno (tested; Chrome adopted standard non-special URL parsing around M130). Safari not tested.
- automerge-repo addresses inside documents with `automerge:<docId>[/<path>][#<heads>]`, where a path segment is `@n` (list index), `{json}` (list element match), `[start-end]`/`[cursor]` (text cursor range) or a URL-encoded map key, and heads are bs58check, `|`-joined and sorted (`AutomergeUrl.ts`, `subdoc-handles/`; to be re-verified against source before implementation).
- A datum is stored at the Automerge path `datums/<tag>/<slot>`; the slot is bytes, spelled as a Go string-literal body so that it can be an Automerge map key (`docs/wiki/docs.md`).

## 2. Decision

### One scheme, kinds as namespaces

```
dtxt:<kind>:<opaque path>[?<query>][#<fragment>]
```

The scheme is `dtxt`. The kind follows a colon, as DID methods (`did:key:…`) and URN namespaces (`urn:isbn:…`) do: a kind is a namespace, not the root of a hierarchy. Kinds are full words. Nothing follows the scheme's colon with `/`, so every path is opaque and no parser normalizes it.

### `datum`

```
dtxt:datum:<doc>/<tag>/<slot>[?heads=<h>|<h>…][#<path inside the datum>]
dtxt:datum:zDoc/org.example.dpath//photos/beach.jpg?heads=…#meta/@0
```

- **Path** names a datum: `<doc>` is a document id in its text form (multibase base58btc) or `self`; `<tag>` contains no `/`; `<slot>` is the stored slot string percent-encoded (`%`, `?`, `#` and non-ASCII always encoded; `/` left as is, so dpath slots stay readable). The slot runs to the end of the path.
- **Query** pins a version: `heads=` lists the document heads the reference was made at, sorted, `|`-joined. Absent means "current", resolved by the reader. Other query keys are reserved.
- **Fragment** selects a position inside the datum using automerge-repo's path grammar unchanged, relative to the datum's value. A fragment is a secondary resource (RFC 3986 §3.5), not part of the datum's identity.

Code has two types. A **datum reference** has no fragment and is what every API that takes a datum accepts (branch origins, write targets, references inside datum values marked `x-automerge: reference`); such APIs reject a fragment. A **location** may carry a fragment and is what annotation targets and similar selectors accept. Both carry the optional heads.

### `actor`

```
dtxt:actor:<node key>[/<path>]
```

The node's public key in its multibase text form, then the actor path (ADR 022): the operator is the bare node key; every other actor's path names its ancestry. Replaces `db+actor://`.

### `blob`

```
dtxt:blob:<multihash>
```

Content identity only, with no query hints. In ADR 026 a blob is reached through its `Blob` datum (digest, MIME type, length; the cipherBlob is found by naming convention and bytes through drawer inventories), so a datum that refers to a blob refers to the `Blob` datum with a `dtxt:datum:` reference. A bare blob URL names bytes without the context to fetch or decrypt them. `?size` duplicated `lengthOctets`, `?via` is superseded by the naming convention, and `?kem`/`?keyring` were already withdrawn.

### Dropped

`db+facet` (becomes `dtxt:datum`), `db+command`, `db+iroh-clone` and the `future+…` placeholders are removed and get designs of their own when needed.

## 3. Consequences

- `big_cabinet_core/reference.rs` is rewritten for the new grammar and both types. The dot-segment rule for slots can go; slots must still be non-empty.
- `refHeads` siblings move into the URL's `heads=`. Existing datums carrying `refHeads` are migrated or read through a compatibility path during the cut-over; new writes use the URL form only.
- `Blob.urls` is legacy (the encryptor no longer writes it; `big_blobs/documents.rs` still falls back to it and accepts `db+blob` and `daybook-blob`). It is retired with daybook_core's migration.
- Every `db+…` occurrence (about 240, including the Kotlin app) is migrated or deleted with its feature.

## 4. Open questions

1. **Heads encoding.** No hex in URLs (operator). Our ids use multibase base58btc (`z` prefix, Bitcoin alphabet, no checksum); automerge-repo uses bs58check (the same alphabet, no prefix, a 4-byte double-SHA-256 checksum appended). Pick one for `heads=`.
2. **Cursors inside fragments.** Automerge's cursor string is `[-]<counter>@<actor hex>`. Accept it as Automerge's own spelling inside the fragment, or re-encode the actor and diverge.
3. **Slot `/` and `%`.** Slots keep raw `/`. Confirm no consumer splits the path on `/` past the tag.
