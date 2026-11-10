Status: Research
Updated: 2026-11-10
Scope: Who in the atproto ecosystem works on shared or generic lexicons; existing records relevant to our generic datums (document title/description, note body, path, ordered refs, annotations/comments, thread state, structure, seen state, identity/attribution); where discussion happens. Read-only research, no repo changes.

Reading convention: "verbatim" = I fetched the raw JSON/text of the lexicon or post and parsed it myself (curl + parse). "summary" = the page went through a summarizing fetch tool or a search snippet, so field lists are not guaranteed complete.

## 1. Community lexicon efforts

### Lexicon Community (`community.lexicon.*`)
- Sources: https://github.com/lexicon-community/lexicon (README, summary; archived mirror), https://tangled.org/lexicon.community/lexicons (summary), https://lexicon.community (fetch FAILED, not read), forum topic "Lexicon Community restructuring" https://discourse.atmosphere.community/t/1011 (verbatim via Discourse JSON).
- Who: volunteer-run working group, governed by a Technical Steering Committee (TSC). Posters seen: ngerakines.me (meeting/restructuring announcements), bmann.ca, bnewbold.net (mentioned as holding domains to donate).
- Process (verbatim from t/1011): the website is the source of truth for governance docs (governance model, contributor covenant, contributing guide, versioned). Canonical schema repo moved to Tangled (https://tangled.org/lexicon.community/lexicons); GitHub is an archived mirror. `community.lexicon.*` NSIDs resolve through the AT repo `@lexicons.lexicon.community`. "Anyone can start a working group without asking us first. The TSC gets involved at the end, when a pull request needs approval, and the bar for acceptance is real-world use." Forum is "the front door". Monthly TSC meeting, first Thursday, 1:00 PM ET, published as a `community.lexicon.calendar.event` record; guests welcome (t/1306). Blog: https://blog.lexicon.community/ (has "how to design a lexicon" and "publishing and resolution" guides; not read).
- Lexicons that exist (GitHub mirror directory listing via API; parsed raw JSON for the ones marked verbatim):
  - `community.lexicon.bookmarks.bookmark` (verbatim): required `subject` (uri), `createdAt`; optional `tags` (array of string).
  - `community.lexicon.interaction.like` (verbatim): `subject` (strongRef), `createdAt`.
  - `community.lexicon.app.profile`, `.app.entry`, `.app.profileLocalization`, `.app.defs` (verbatim for profile/entry): fields `name`, `description`, `images`, `tags`, `status`, `links`, `platforms`, `lexicons`, `accountIndicators`, `createdAt`, `updatedAt`; entry adds `profileDid`, `langs`. This is app (software) profiles, not person profiles. README: "A `profileDid` is a pointer to where a canonical profile may exist, not proof of ownership or verification by itself."
  - `community.lexicon.calendar.event`, `.calendar.rsvp`, `community.lexicon.location.*` (address/geo/hthree/fsq), `community.lexicon.payments.*`, `community.lexicon.preference.ai` (names from directory listings only; fields not read).
- Not present on the GitHub mirror: any note, comment, annotation, thread, or person-profile lexicon. In-flight forum topics (titles only, from category JSON): "Drafting a Threads lexicon" (t/1418), "A general purpose ratings lexicon" (t/1265), "[Proposal] A lexicon for user-defined collections" (t/1388), "Proposal: a community lexicon for standalone images" (t/1091), "Handling Reviews for the Book Lexicon" (t/1085), "Purposal: General purpose File sharing / Download links lexicon?" (t/1187), "Extending Lexicons: sidecar records patterns and typed extensions" (t/1353), "Proposal: a shared, informal protocol for identity confidence" (t/985), "Exploring CRDT Lexicons" (t/919), "Community App Lexicon WG" (t/761).

### standard.site (`site.standard.*`)
- Sources: https://standard.site (summary), https://tangled.org/standard.site (repos: `standard.site/standard.site`, `standard.site/lexicons`; summary), https://lexicon.garden/nsid/site.standard.document (summary).
- Who: "Maintained by the developers building on it", no single owner stated. Implementers named: Leaflet, pckt, Offprint. Questions via https://bsky.app/profile/standard.site. Forum threads: "Proposal: standard.site.declaration" (t/974), "Updates to Standard Site lexicon, adding recommend and permission sets" (t/865).
- Lexicons named: `site.standard.publication`, `site.standard.document`, `site.standard.theme.basic`, `site.standard.graph.subscription`, `site.standard.graph.recommend`.
- Scope statement from forum (t/1418, erlend.sh, verbatim): standard.site should stay focused on long-form blog-like content; forum threads should be a separate lexicon.
- Raw JSON in the standard.site repo not fetched (404 on guessed URLs); field lists below are from the Lexicon Garden summary.

### Lexicon Garden / registry / resolution
- Lexicon Garden https://lexicon.garden: search and indexing of published lexicons by NSID (fetched search pages, summary). Indexes lexicons from many authors (e.g. `fyi.frontpage.feed.comment`, `pub.leaflet.*`, `app.offprint.*`, `blog.pckt.*`).
- atproto resolution spec https://atproto.com/specs/lexicon#lexicon-publication-and-resolution (read, verbatim excerpt): schemas are published as records of type `com.atproto.lexicon.schema` in a repo; DNS TXT record `_lexicon` links the NSID authority (group level, i.e. all NSIDs differing only in the final name segment) to a repo DID; record key is the NSID; fields `lexicon` (=1), `id`, `defs`, `description`.
- Bluesky guidance on canonical sources (verbatim, https://raw.githubusercontent.com/bluesky-social/atproto/main/lexicons/README.md): atproto repo holds only `com.atproto` and `tools.ozone`; `app.bsky` and `chat.bsky` canonical source is https://github.com/bluesky-social/bsky.
- I did not find a Bluesky-authored guide on "how to share lexicons across apps"; the Lexicon Community blog guides (not read) are the nearest.

## 2. Comments and annotations

### Margin (`at.margin.*`), the closest match to our Web Annotation model
- Sources: https://github.com/paddinglabs/margin (formerly margin-at/margin; also on https://tangled.org/margin.at/margin), file `lexicons/at/margin/note.json` and `reply.json` (verbatim raw). Repo files: apikey, authFull, collection, collectionItem, like, note, preferences, profile, readingRoom, reply. `annotation.json` is no longer on main; a letta-ai issue (https://github.com/letta-ai/social-cli/issues/35, search snippet only) says `at.margin.note` is the unified replacement and `at.margin.annotation` stays supported.
- `at.margin.note` ("W3C Web Annotation Data Model compliant unified note record"), required `motivation`, `target`, `createdAt`:
  - `motivation` string knownValues: commenting, highlighting, bookmarking, tagging, describing, linking, replying, editing, questioning, assessing.
  - `body` ref `#body`: `value` (max 10000 / 3000 graphemes), `format` (default `text/plain`), `uri`.
  - `target` ref `#target` (required `source` uri): `source`, `sourceHash`, `title`, `selector`, `state` (`#timeState`: `sourceDate`, `cached`).
  - `selector` (required `type`): `type` knownValues TextQuoteSelector, TextPositionSelector, CssSelector, XPathSelector, FragmentSelector; `exact`, `prefix`, `suffix`, `start`, `end`, `value`, `conformsTo`, `refinedBy` (recursive).
  - Others: `color`, `tags`, `facets` (app.bsky.richtext.facet), `generator` (`id`, `name`, `homepage`), `rights`, `labels`, `modifiedAt`.
  - Note: the target is a web URL `source`, not an at-uri/record; this is annotations of web pages.
- `at.margin.reply`: required `parent`, `root` (both `#replyRef` = `uri` at-uri + `cid`), `text`, `createdAt`; optional `format`.
- Who: Padding Labs / margin.at project (maintainer handle not confirmed by me). Interop with Semble cards noted (Cosmik "Lab Notes #005", search snippet only).

### Leaflet comments (`pub.leaflet.comment`, verbatim raw from hyperlink-academy/leaflet)
- Required `subject` (at-uri), `plaintext`, `createdAt`. Optional `reply` (`#replyRef`: required `parent` at-uri, no root), `facets` (`pub.leaflet.richtext.facet[]`), `onPage` (string), `attachment` (union, open to `#linearDocumentQuote`).
- Anchoring: `#linearDocumentQuote` = `document` (at-uri) + `quote` ref `pub.leaflet.pages.linearDocument#quote` = `start` and `end`, each a `#position` with `block` (array of integer, path into block tree) and `offset` (integer). Position-based, no text-quote context (no exact/prefix/suffix).

### Tangled (`org.tangled.*`, verbatim raw via api.tangled.org getBlob, repo tangled.org/core, master)
- `org.tangled.feed.comment`: required `subject` (union, refs `com.atproto.repo.strongRef`), `body` (union, `org.tangled.markup.markdown`), `createdAt`; optional `replyTo` (strongRef), `embed` (union, `org.tangled.embed.commit`). No line anchor on this record.
- `org.tangled.markup.markdown`: `text` (max 262144 bytes) and `blobs`. `org.tangled.markup.plaintext`: `text`.
- `org.tangled.review.patch`: required `ticket` (record-key), `base`, `head` (`org.tangled.git.oid`), `createdAt`; `source` (`repo` did, `branch`). `org.tangled.track.ticket`: required `title` (markdown#inline), `state`, `createdAt`; `body`, `labels`, `tangledLabels`, `assignees` (did[]), `targetBranch`.
- Older snapshot (search snippet only): `sh.tangled.repo.pull.comment` had `pull`, `body`, `createdAt` (+ later-removed `repo`, `commentId`, `owner`). I did not read a line-anchored review-comment lexicon; the directory `lexicons/org/tangled/review/unspecced` exists but I did not read it.
- Identity: `sh.tangled.identity.resolveCommitters` (verbatim): procedure, input `actor` (did), output `committers` (array of string), described as "Resolve an actor DID to its verified Git committer identities."

### Frontpage, Bluesky posts, others
- Frontpage: NSIDs `fyi.frontpage.feed.post`, `.vote`, `.comment`, `fyi.frontpage.richtext.block` (names only, from Lexicon Garden search; fields NOT read).
- `app.bsky.feed.post` (verbatim from bluesky-social/bsky): `reply` = `#replyRef` with required `root` and `parent`, both `com.atproto.repo.strongRef`; `text` (max 300 graphemes), `facets`, `embed`, `langs`, `labels`, `tags`, `createdAt`.
- Not read: Smoke Signal, WhiteWind comments, Semble annotation records (Semble uses `network.cosmik.card`, `.collection`, `.collectionLink`, `.connection` per a third-party skills page; summary only).

## 3. Documents, notes, titles

- `site.standard.document` (Lexicon Garden, summary): required `title` (max 5000 / 500 graphemes), `site` (uri: at:// publication or https URL), `publishedAt`; optional `path` (leading slash, joined with site URL), `description` (30000 / 3000), `textContent` (plaintext only), `content` (open union), `tags`, `coverImage`, `bskyPostRef` (strongRef), `contributors` (`did`, `displayName`, `role`), `labels`, `links`, `updatedAt`.
- `site.standard.publication` (standard.site summary): `url` (required), `name` (required), `icon`, `description`, `basicTheme`, `labels`, `preferences`.
- `pub.leaflet.document` (verbatim, summarized by tool): required `pages`, `author`, `title`; optional `postRef`, `description`, `publishedAt`, `publication`, `theme`, `preferences`, `tags`, `coverImage`; `pages` = array union of `pub.leaflet.pages.linearDocument` and `pub.leaflet.pages.canvas`.
- `com.whtwnd.blog.entry` (tool summary of atcute copy): required `content` (max 100000); `title`, `subtitle`, `createdAt`, `visibility` (author/public/url), `theme`, `ogp`, `blobs`, `isDraft` (deprecated).
- `app.bsky.actor.profile`-style generic "note": none found in Lexicon Community. Closest generic notes: `at.margin.note` (above); `network.cosmik.card` (Semble, URL or note content, summary only). Forum t/1345 "shared lexicon for video notes" is a proposal, nothing ratified.
- Ordered lists / collections: no ratified lexicon. Forum t/1388 "[Proposal] A lexicon for user-defined collections" (chrisshank.com): tyler.fun replied that list ordering "needs standardization on its own" and that a second ordering list drifts. Semble has `collectionLink`. Leaflet orders via `pages`/blocks arrays. Our "ordered list of references to parts of a document" has no direct counterpart.

## 4. Identity and profiles

- `app.bsky.actor.profile` (verbatim, bluesky-social/bsky): `displayName` (64 graphemes), `description` (256), `pronouns`, `website`, `avatar`, `banner`, `labels`, `joinedViaStarterPack`, `pinnedPost`, `links` (strongRef[], max 10), `createdAt`. Record key is `self`.
- Lexicon Community: no person-profile lexicon found; `community.lexicon.app.profile` is for apps. A forum thread "Profile background image lexicon" (t/747, title only) exists.
- Identity confidence proposal (t/985, read verbatim first 5 posts): jtayler.bsky.social proposes a record shape with `rank`, `score`, `confidenceType`, issuer signature (example NSID `app.truanon.label`). gui.do (Barazo) objects that scores cause context collapse and argues for per-community derivation from raw attestations. Relevant to "attributed vs verified" but still an open proposal; 21 posts, active 2026-11-10.
- Attribution to non-atproto identities: Bridgy Fed creates DIDs and repos on the fly for bridged accounts (snarfed.org posts, search snippet only; not read in full). Tangled's `resolveCommitters` maps a DID to verified git committer identities (verbatim). No generic "attributed identity" lexicon found.
- Related: standard.site `contributors` (`did`, `displayName`, `role`) lets a document name contributors by DID only.

## 5. Other venues and findings

- Sidecar records and extension patterns (t/1353, verbatim first 3 posts): essentialrandom.bsky.social surveys patterns (keys derived from subject, explicit reference, metadata object keyed by NSID, namespaced extension fields); snarfed.org favours the embedded-extension and subject-keyed patterns; chris.pardy.family suggests a `$types` mixin array. Directly relevant to "extend other people's lexicons".
- Threads (t/1418, verbatim first posts): erlend.sh proposes `community.lexicon.thread` as a baseline-compatible fork of standard.site; prior art listed: Colibri (`social.colibri.beta.thread`), atmoBB, Barazo (github.com/singi-labs/barazo-lexicons), UserInput, Roomy. Also from `cloud`: SIOC ontology comparison. Our thread resolved-state would sit here.
- CRDT lexicons (t/919, verbatim): hyl.st, chris.pardy.family (Leaflet post "CRDT's on ATProto"), zicklag.dev on op compression; mentions Automerge/Loro/Sedimentree. Relevant to a local-first CRDT store.
- Video notes (t/1345): maboa.bsky.social drafts transcripts "TAML" with time-range anchors (ms from media start); bmann.ca (Boris Mann) says first-class comment for timeline remarks is good and points at Leaflet.
- IETF: working group "atp" (Authenticated Transfer), charter-ietf-atp-01 approved, chairs Mallory Knodel and Shuping Peng, AD Andy Newton (datatracker group/atp/about, summary). List atp@ietf.org, Zulip stream "atp", GitHub org linked from the page. Out of scope in charter: "application-specific schemas, and social application semantics", so lexicon design is NOT an IETF topic; do not engage there for record types. Drafts named: draft-newbold-atp-aturi, draft-holmgren-at-repository. A different datatracker page (wg/atproto) shows the older group as abandoned.

## Could not read
- https://lexicon.community (fetch failed), lexicon.community blog guides, standard.site raw lexicon JSON (guessed URLs 404; used Lexicon Garden summary), Margin's removed `annotation.json`, Tangled line-anchored review/comment lexicon, Smoke Signal lexicons, Frontpage/WhiteWind comment field lists, Semble lexicon JSON, Bridgy Fed docs, any Discord/Matrix invite (none found; no claim made), most Discourse threads beyond the ones cited.

## Overlap with our generic datums

| Our datum | Closest atproto lexicon | Notes |
|---|---|---|
| Document title/description | `site.standard.document` `title`/`description`; `pub.leaflet.document` `title`/`description`; `app.bsky.actor.profile` `description` | standard.site requires `site` and `publishedAt`, which are publishing-oriented; scope statement says long-form only |
| Note: text body + media type | `at.margin.note` `body` {`value`,`format`,`uri`}; `org.tangled.markup.markdown`/`plaintext` | `format` default `text/plain` is the closest to a media type; no community-ratified note |
| File path | `site.standard.document` `path` | URL-path semantics, not filesystem path; no generic file-path lexicon (t/1187 file sharing is a proposal) |
| Ordered refs to document parts | none ratified; Leaflet `pages`/`linearDocument` blocks; t/1388 collections proposal | ordering identified as an open standardization gap |
| Annotation/comment target + selector | `at.margin.note` `target`+`selector` (TextQuoteSelector `exact`/`prefix`/`suffix`, motivation); Leaflet `#linearDocumentQuote` (block path + offset) | Margin is nearly our model but targets web URLs; Leaflet anchors by position only |
| Reply with explicit parent | `at.margin.reply` (`parent`+`root`, strongRef-like with cid); `app.bsky.feed.post` `reply.root/parent`; `org.tangled.feed.comment` `replyTo`; `pub.leaflet.comment` `reply.parent` | convention is strongRef (uri+cid); Leaflet and Tangled omit root |
| Thread resolved-state | none found; t/1418 threads draft may host it | |
| Tasks/decisions/sections | `org.tangled.track.ticket` (`title`,`state`,`assignees`) only for code issues | no generic task lexicon found |
| Per-reader seen state | none found | |
| Profiles | `app.bsky.actor.profile` | `links` strongRef array; person profiles absent from `community.lexicon.*` |
| Attributed (non-verified) identity | t/985 identity-confidence proposal; `site.standard.document` `contributors`; Tangled `resolveCommitters`; Bridgy Fed bridged DIDs | open; our "attributed vs verified" split is not covered by any ratified lexicon |
| Extending foreign records | sidecar/extension discussion t/1353 | decide embedded extension vs sidecar before adopting |

## Engagement

- Lexicon Community forum, category lexicon-community: https://discourse.atmosphere.community/c/lexicon-community/31 (the stated front door; bring a schema plus a real app).
- Lexicon Community TSC monthly meeting, first Thursday 1:00 PM ET; ngerakines.me announces it; bnewbold.net and bmann.ca (Boris Mann) participate.
- Canonical schemas: https://tangled.org/lexicon.community/lexicons; updates https://bsky.app/profile/lexicon.community.
- Threads draft (t/1418): erlend.sh (Roomy), gui.do (Barazo), Colibri, atmoBB authors.
- Sidecar/extension discussion (t/1353): essentialrandom.bsky.social, snarfed.org (Bridgy Fed), chris.pardy.family.
- Identity confidence (t/985): jtayler.bsky.social, gui.do.
- Collections/ordering (t/1388): chrisshank.com, tyler.fun.
- CRDT lexicons (t/919): hyl.st, zicklag.dev, chris.pardy.family.
- Annotations: Margin project https://github.com/paddinglabs/margin and https://tangled.org/margin.at/margin; Leaflet https://github.com/hyperlink-academy/leaflet.
- standard.site: https://tangled.org/standard.site, https://bsky.app/profile/standard.site.
- Tangled: https://tangled.org/tangled.org/core.
- IETF atp (protocol only, not record types): atp@ietf.org, Zulip stream "atp".
- Handles above come from forum usernames; I did not verify Discord or Matrix venues.
