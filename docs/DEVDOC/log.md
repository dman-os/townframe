# duck-log

## 2026-02-22 | bed time thoughts

- Non-server stored documents/blobs.
  - You tag them with a special dpath that is excluded from server/host storage.
    - Or more accurately, they're kept on devices that you specifically opt them into.
  - Usecase: replacing syncthing.
- Trapwires.
  - Repo state that triggers actions will set a trap if it requires user confirmation.
    - An event wait in wflows?
- Andy's swiping browsing

## 2026-02-22 | rotating headers

I need a good way to show avail headers in a document without occpuying horizontal space.
Portrait screens are the main target afterall.

here's an idea to try out, at the bottom/top of the screen, you get small palettes that lay out in a row. as soon as you reach the next header, the left/right most item pops from the row into the screen. the more distant you are from the screen, the less we can show for a header. essentially, it's like the headers are travelling across edges of the screen. think of it like offscreen target indicators in games that hug the edge.

## 2026-02-22 | wasm compilation

So I decided to spend some time mending some broken windows today.
I've had a bunch of flakey tests plaguing me for months now.
A bunch of bugs were fixed to improve this but one of the main culprits turned out to be wasm JIT for our daybook_wflows.wasm payload leading to timeout issues.
I increased the timeouts substantially which helps ofc.
I should bring wasm-opt and release builds for wasm artifacts into scope too.

---

Wow, debug build wasm artifacts are 101MiB.
Release mode? 900KiB.
Are they not doing any dead-code elimination? 
That has to be lack of dead-code elimination, right?
It surely can't fucking be unoptimzed code-gen.

## 2026-02-17 | the missing research

I should sit down and study more prior art.
I should sit down and design the app in full in PenPot, also.
But to do that, I should probably study prior art well.
I'm just going off Obsidian and Notion and Patchwork which are well and good.
But that's just things I'm familiar with, difficult to see the foundations when you're holding things you're familar with.
Familarity blindness? Is that a thing?

Having accessible index of prior art for each feature is going to help speed all of this up.
Start from the earliest systems to the latest, there's a looot of prior art here.
Xandu, OpenDoc, Org-mode...all the way upto the millions of Notion clones and todo apps I've used in the past few years.

I should make a rubric for evaluating them:
- UI usability
  - Is it layed out well? Accessible?
- UI looks
  - Does it look good? Inviting?
- Flexability
  - Will I need to use another app for a usecase that should be covored?
- Text editor
  - How functional is the text editor? This used to be make or break for me and here I am completely throwing that down the drain.
- Programmability
- Userbase satisfaction
  - I mean, the historical record of the actual people they served. Why and how and when?
- Automation
- A11n

Shortlist of things to study:
- Notion
- Obsidian
- Anytype
- Org-mode
- Patchwork
- Craft.co
- Roam
- OpenDoc
- Xanadu
- LogSeq
- Joplin
- Keep Notes
- Telegram Saved Messages
- HackMD

---

Wait a fuck? Xanadu was never released??

## 2026-02-17 | one fucking year

I just noticed it's been 1 year since I created the first commit in here.
I haven't really been working on it that much, employment and all but...yes, let's try to clean it up and make the repo public before the month is out.
Freeze feature work and get the CI working for Android, Linux, Windows, MacOS and iOS.
Who am I forgetting there?

Need to setup proper contact vectors, setup a Discord (yuck)?

## 2026-02-17 | saving prompts?

Should I keep a forever log of the prompts used to make this app?
Would that be useful?
Cursor doesn't provide these data for some reason, I'd love to go back and see all the chat threads I've had with them but those are hardly useful.
I do like the prompts/plans I'd workshopped working with the latest models and with Codex CLI though.
Especially the fist plan/prompt for a feature, those seem well written. 

Looking in ~/.codex, the full history with all the details is stored in JSON files. One could easily extract and make a viewer for these.
I'd like to have some kind of index that takes me from a feature/file to the relevant chat thread for it. Maybe summuraize it first?
Actually no, the artifacts in the code should be enough for whatever task.
Documentation should be documentation and that should be where rigor is appled, no need for hacks.
Good to know that ~/.codex exists at least, in case I ever need to remember undocumented details. 

---

Ah fuck, I know I'll never write the right docs.
And LLM written prose has the ick factor.
But still, these prompts, as lengthy and detailed as they are, are not written to be read again.
Wouldn't writing prompts to be read again be too laborious?

## 2026-02-17 | I <3 paths!

I keep finding paths as a good solution for my tagging or identification problems.
So far we have:
- UserPaths for identifying actors
- BranchPaths for naming doc branches
- Dpath system
- Progress tags path

When in doubt, use paths!

But there's nuance here:
- We're a CRDT Jose, there's no global arbiter to ensure exclusivity.
  - Some paths like dpath and progress tags allow multiple items to have the same path
  - Mapping these to posix require tricks, lotsa tricks.

## 2026-02-07 | ppf genesis prompt

What's pauperfuse? It's the poor man's fuse.

Or in other words. It's going to be like the git worktree.
In a sense, git worktree is the fuse materialization of the git contents, right?

We're going to implement that.

Our main usecase is remote collaborative document editing except its on the FS layer rather than in the editor.
Basically, the pauperfuse daemon will update the files as changes come in.
It'll need to detect changes to files and send it down the pipe.

Ofc, this is tricky to do in POSIX.
If that file is open in VIM for eg, it'll need to actively watch the file to avoid drift BUT!
We're not interested in solving that.
We assume that all editors working on our tree are aware of the semantics and will avoid toctou bugs themselves.

Details:
- Pauperfuse will be a library that'll be used by daybook_cli mainly
- It'll accept a bunch of objects that describe files
  - Their path relative to root
  - Their stat info
  - A method to read offsets into the actual contents
      - This detail is important since we will add an actual FUSE based backend
      - The actual impl of that object could choose to do passthrough to another file or hold it all in memory
  - A method to ask for processing changes to the byte contents
- It shall have an event loop task where it'll process change events
  - Either from the actual fs through the OS 
  - Or from the object provider (which will correspond to remote events for our main usecase)
- No mentions of automerge or collaborative text editing, pauperfuse will be agnostic and general purpose
- We'll have different backends
  - A livetree backend (find a better name)
      - This will just write out the whole files onto the path
      - It'll watch the paths using https://lib.rs/crates/notify to detect changes
      - It'll even support this usecase without having to have a daemon online in a lazy and discrete manner like `git` or `jj` worktrees
      - We'll need to mantain metadata in some location that will be provided to us
  - An fuse(8) backend on linux (for later)

Tricky details:
- Most of pauperfuse will use byte offsets and byte arrays
  - But for example, some of our usecases might have structured datatypes
    - We'll have to reject bad inputs?
    - I guess what we want to do is support multiple types of objects and this will be defined by the object given
      - First case, we reject invalid changes restoring to the valid state
      - Second case, we keep the ondisk file as is but mark it somehow?
        - You know how in git, we have staged and unstaged file?
        - We'll mark invalid files, marks that will be queryable
- For daybook_cli and any future users, we'll want to make implementing their objects easier
  - What's the shape of these objects? trait impls?
    - Dynamic dispatch can be a pain and not cheap. can we do better?
      - A single pauperfuse context will deal with a heterogenus set of object types
  - We'll want to provide composable primitives for implementing the different behaviors
    - Edit rejection to be used by structured files
    - Passthrough support to other on disk files

## 2026-01-29 | a case for ids in keys

I keep going back and forth on this.

Idea:
  - Instead of `{"org.eg.item": { "id1": {} }}`, do `{"org.eg.item/id1": {} }`
  - I.e. roll ids into top-level keys allowing N items for a key without needing a nested array/map

Cons:
  - Iteration over keys required to find key.
    - You can't just do a basic hash map lookup
    - Acceleration structures using prefix matching are out of scope?
      - I'd say so. We'd like this to be easily programmable through WASI.
Pros:
  - The biggest one right now is that this works well with per key metadata
    - We track createdAt/updatedAt for keys
    - Additionally, we use the last commit hashes of when a key changed to dedup work
    - Having ids in keys allows us to have multiple instances of a key while tracking each separately
    - We'll either have to introduce key level nested metadata or pay additional costs of change checks

Thoughts:
- How often do we look up keys in a document?
  - For general routines that must check for existence of multiple keys 
- Should we do an automerge document per key?
  - I don't think so, that would increase our document count by an order of magnitiude.
    - Too much pressure on the sync backend?
    - We'd still pay the metadata cost per key if we do ids in keys
- Specialized support for nested ids?
  - Specify in the definition of a key that it's of the array and map instance type?
    - Hmm
  - Arbitrary nested tracking field change tracking support?

I suppose let's flesh out the change tracking idea.
- We have a key Foo that is derived from key Bar.
  - A plugin must have registered this relationship somehow.
  - I.e. types of key Bar are derived from other keys.
  - I suppose it should indicate which fields of Bar hold the reference key.
- We'll need to mantain an index of these relationships
  - In SQLite?
    - Best fit, querying and indices are efficent
    - Rebuild on each device?
      - Painful, will need to read each doc and each key
      - Consider cross device syncing for sqlite tables in future as optimization
  - In Automerge?
    - No, churn would produce too much garbage.
    - Denormalization should only be considered for cases that don't churn.
- But wait, how do we minimize work?
  - Derivation of Bar happens in a plugin defined routine presumably?
    - Schedule the routine for every change?
      - Scheduling is not exactly cheap, we're using wflow + wasm
      - It'd be great if we didn't have to schedule work for simple diff checks that routine would perform
  - How such a processor routine defined anyways?
    - Defined to watch Foo instances?
      - How would we minimize work in this case?
        - Specialized field diff predicates?
    - Two processors, one defined to watch new Foos and another to watch dep changes?
      - Needing to register two routines to just do a basic derivation seems excessive
- We'll want to have unbounded number of derivied keys
  - User programmability shouldn't look out for footguns here
- Let's KISS and add optimzations later
  - Simple derivation usecase and solutions for it
    - 1 DocA-Foo -> 1 DocA-Bar
      - Schedule routine when Foo changes
    - 1 DocA-Foo -> 1 DocB-Bar
      - Schedule routine when Foo changes
    - 1 DocA-Foo, 1 DocA-Baz -> 1 DocA-Bar
      - Schedule routine when Foo or Bar change
    - 1 DocA-Foo, 1 DocB-Baz -> 1 DocC-Bar
      - Schedule routine when Foo or Bar change
    - 0-N DocA-DocX-Foo -> 1 DocZ-Bar
      - Schedule routine when any Foo changes

Going back to our main quandry, this doesn't seem relevant to the optimization usecase.
Our processor system is already granular on keys and not docs.
I.e. processors are defined per key and changes are processed per key.

We'll still want to mantain key specific change timestamps which gives us key specific commits refs for free.
The question is, for nested keys, do we want that granularity?
I think so, since URLs are specific to keys?

---

Alternative solutions:

Top array:

```js
Doc([
  {
    ty: "org.eg.blob",
    id: "uuid",
    /**/
  },
  {
    ty: "org.eg.ocr",
    id: "uuid",
    /**/
  }
])
```

- Insertions and deletions make prop paths unstable
- Semanticallly the same but worse than the next proposal

---

Uuids for keys:

```js
Doc({
  "uuid": {
    "$ty": "org.eg.blob",
    "$id": "uuid",
    /**/
  },
  "uuid": {
    "$ty": "org.eg.ocr",
    /**/
  }
})
```

- Type queries require scans
- Payload has extra ids

---

Hmm, what about: 
```js
Doc({
  "org.eg.dmeta": {
    "$facetId": "main",
    /**/
  },
  "org.eg.blob": {
    "main": {
      /**/
      ]
    },
    "other": {
      /**/
    }
  },
  "org.eg.ocr": {
    "$facetId": "main",
    /**/
  }
})
```

- Payload polluted by extra fields

---

Upon further thought, I think deferring the id scheme to plugins is not a good idea.
We'll want to have stable and simple global id space that addresses all facets across all docs.
Usecases like global indices demand this.
I was hoping to use hierarchical URLs for identity but that's not exactly simple.

Additionally, storage format need not optimize for routine usage. 
How common is the directly editing the JSON usecase?
On one hand LLMs should have an easy time with it so supporting that isn't exactly ungrounded.
Human editable text formats are always a good thing.
On the other hand, there are a lot of steps involves in editing the the values directly already.
For example, you'll have to update metadata elsewhere in the indices and the drawer.
So, if there's ever direct JSON editing involved, it will have to be done on a facet specific projection.
This is how the prop capability routines do it today, they don't get to see the automerge json directly but an extracted prop.


With this in mind, the winner ought to be:
```js
Doc({
  "uuid": {
    "$daybook.facet.ty": "org.eg.blob",
    /* */
  }
})
```

Alternatively:

```js
Doc({
  "$daybook.doc.meta": {
    facets: {
      "uuid": {
        "ty": "org.eg.blob",
        createdAt: "2026xxx",
        updatedAt: ["2026xxx"],
      }
    }
  },
  "uuid": {
    /* */
  }
})
```

...BUT!

Hmm.
One thing I forgot about the original formulation is that not relying on random ids allows adding new keys concurrently with the same intent and merging should work.
In this design, if I add a new key that's supposed to be a singleton, I can't use a globally unique prop path.
Think duplicate derivations happening on two devices that are to create new facets but they write to two different keys.
We'd have to add a separate merging logic to resolve singletons.
Yes, we'll need to separate the global id space from the prop struct path.

FUCK.
It's good to write down but it's surprising how initial intiutions can be so efficent.

So, to solve this, plugins need access to a keyspace that they control. 
But a key space that the drawer aware of since we'd still want to identify and track facets as a unit of information.
That's something like:

```js
Doc({
  "org.eg.dmeta": {
    facetsByUuid: {
      "uuid": "org.eg.blob/main"
    }
    facetMeta: {
      "org.eg.blob/main": {
        guid: ["uuid", "uuid"],
        createdAt: "2026xxx",
        updatedAt: ["2026xxx"],
      }
    }
  },
  "org.eg.blob": {
    "main": {
      /* */
    }
  }
})
```

This brings us back, full-circle, to our main effing quandry.

We do have more information though now.
We don't expect direct writes to the automerge JSONs bu ont projections.
I.e. id in keys or nested ids is an impl detail for plugins.
In fact, if we ever want to migrate to a separate automerge doc per facet in the future, plugins ought to be none the wiser.

At this point, the question becomes, is id in keys efficent for drawers?
It's not!
We'll want O(1) lookup.

A small issue, should plugins have ids for each facet?
- URL ambiguity is a thing.
  - `db:///docid/org.eg.blob` refers to a facet instance but so does `db:///docid/org.eg.blob/main` which is not great since what if we want to refer to props in facets? How do we differentiate if `main` is a prop or the singleton?
    - Special case the name `singleton`? Not elegant.
- We could assign a const `MAIN` key for singleton facet schemas if they don't upfront do so.
  - Hardly an improvement, let's always require ids but conventionalize main for singletons.
    - Is there a better name?

- On the concern of what is allowed in facet ids
  - `main` is simple enough but what if our singleton is to be derived
    - We'll want to have a id that's based on the inputs somehow
      - Easiest is to use UUID of the other facet in the id but I'd like to keep most of the uuid usage in the drawer
        - Also, what if the inputs are multiple?
      - Hashed input of all input facet URLs?
        - Good enough I say.
        - Probably should be the general approach.
          - This will allow us to reject paths as facet ids

## 2026-01-28 | procrastidesign for expense tracking

```js
Doc({
  "org.eg.blob": [
    {/*..*/}
  ],
  "org.eg.ocr": [
    {
      url: "db:///self/org.eg.blob/0"
      atCommit: ["hash1", "hash2"],
      content: "text",
      contentWithLocation: [
        {
          bboxPx: [0, 0, 100, 100],
          content: "text",
        },
      ]
    },
  ],
  "org.eg.embedding": [
    {
      url: "db:///self/org.eg.blobs/0"
      atCommit: ["hash1", "hash2"],
      model: "jina-embedding-4",
      vector: Blob([1.2, 1.3, 1.4])
    }
  ],
  "org.eg.invoice": [
    {
      src: [
        {
          url: "db:///self/org.eg.embedding/0/#hash1|hash2",
        },
        {
          url: "db:///self/org.eg.ocr/0/#hash1|hash2",
        },
      ],
      items: [
        {
          name: "Onions",
          price: "XXXX",
          amount: "XXX"
        },
      ],
      subtotal: "XXX",
      vat: "XXX"
    }
  ]
  "org.eg.txn-ref": [
    "db:///ledger-file-id/org.eg.ledger/txns/0",
  ],
})
```

```js
Doc({
  "org.eg.ledger": {
    accounts: {},
    txns: {
      "txnId-xxx": {
        src: [
          {
            url: "db:///self/org.eg.embedding/0",
            at: ["hash1", "hash2"],
          },
          {
            url: "db:///self/org.eg.invoice/0",
            atCommit: ["hash1", "hash2"],
          },
        ],
        ts: "2026xxx",
        title: "Groceries",
        subtitle: "Weekly",
        subitems:  [
          {
            acccount: "liabilities:credit",
            amount: "2000",
            currency: "ETB",
          },
          {
            account: "expenses:food",
            amount: "-2000",
            currency: "ETB",
          },
        ]
      }
    }
  },
})
```

## 2026-01-27 | procrastidesign

```js
{
  "org.eg.dmeta": {
    primary: "image",
    createdAt: "2026xxx",
    updatedAt: "2026xxx",
    facetMeta: {
      "org.eg.blob": {
        createdAt: "2026xxx",
        // the change hash of the change of this prop
        // indicates the last commit of the facet
        // array to allow merge resolution from multiple updates
        updatedAt: ["2026xxx"],
      }
    }
  },
  "org.eg.note": {
    "main": {
      mime: "text/md",
      // automerge text
      content: Text("User written text #hi #hello"),
    }
  },
  "org.eg.labels.hashtags": {
    // having source groupings avoids redundant
    // urls in each tag
    // alternate design would be
    // tags: [{ content: "hi", url: "db:///xxxx", atCommit: ["xxx", "xxx"]} ]
    // but that would duplicate the url and commit would churn too much
    // FIXME: nope, this is a bad convention sicne it'd make referring
    // to this instance difficult
    "db:///self/org.eg.note": {
      // commit in nested key instead of url allows efficent
      // automerge churn
      atCommit: ["hash1", "hash2"],
      tags: [
        {
          content: "hi",
          // this fields would churn too much, not useful enough
          // byteStart: 15,
          // byteEnd: 20,
        },
        {
          content: "hello",
          // byteStart: 15,
          // byteEnd: 20,
        },
      ]
    }
  },
  "org.eg.blob": {
    // multiple blobs seems like overkill in one sense
    // why not string together multiple docs?
    // but on the other hand, since each doc is a separate
    // automerge document, it makes it a more atomic and 
    // and portable to have related blobs in one doc
    // think embedded images in markdown
    "main": {
      mime: "image/png",
      digest: "z38d837e018f9820b9a098f99e0989a98d98bdc980a",
      lengthOctets: 1024,
      // TODO: need a convention around oneOf on keys. inline optional fields
      // or always a nested field like this?
      inline?: Blob("38d837e018f9820b9a098f99e0989a98d98bdc980a"),
      urls?: [
        // no authority need in most urls
        // FIXME: I hate typing  triple slashes in nvim
        "db+blob:///dazsdj9sj98ad98cajs9d8cjasd",
        "https://example.com/pic.png",
      ]
    }
  },
  "org.eg.dpath": {
    // hmm, on the other hand, multiple blobs
    // is a bitch for dpaths
    paths: [
      "/docs/hi.d",
      "/hi/hello.md?org.eg.note/main",
      "/hi/hello.png?org.eg.blobs/main",
    ]
  },
  "org.eg.image-metas": {
    "db:///self/org.eg.blobs/main": {
      atCommit: ["hash1", "hash2"]
      mime: "image/png",
      widthPx: 1024,
      heightPx: 1024,
    }
  },
}
```

## 2026-01-24 | content schema

Requirements:
- Raw hand written text
- Hand written text with markup
- Media
  - Images
  - Videos
  - Audio
- Structured documents
  - OpenCanvas
  - Ledgers
- Metadata
  - Embeddings
  - Extracted metadata
  - Tags

Concerns:
- Should image docs have the blob meta inline or reference other dedicated doc for blob?
  - Inline is just efficent.
- Mininimze the number of `match` cases on the consuming side while incrasing the fidelity of the data.
  - Optional fields?
  - How do we do fields unique to certain formats?
- Relying on cross-key and cross-doc reference makes it tricky to mantain consistency
  - Need a convention/policy around reference liveness
    - We'll need a database that indices all references across repo
  - Would key ids help here?
    - Move related items into a single doc

```js
{
  "org.eg.facets": {
    primary: "image"
  },
  "org.eg.blob": {
    mime: "image/png",
    digest: "z38d837e018f9820b9a098f99e0989a98d98bdc980a",
    lengthOctets: 1024,
    // TODO: need a convention around oneOf on keys. inline optional fields
    // or always a nested field like this?
    inline?: Blob("38d837e018f9820b9a098f99e0989a98d98bdc980a"),
    urls?: [
      "db+blob://dazsdj9sj98ad98cajs9d8cjasd",
      "https://example.com/pic.png",
    ]
  },
  "org.eg.image-metadata": {
    srcRef: "db://self/org.eg.blob?at=hash1,hash2",
    // case when current key and src commit match
    srcRef: "db://self/org.eg.blob?at=self",
    mime: "image/png",
    widthPx: 1024,
    heightPx: 1024,
  },
  "org.eg.audio-metadata": {
    srcRef: "db://self/org.eg.blob?at=self",
    mime: "audio/ogg",
    lengthMs: 100000,
  },
  "org.eg.freehand": {
    mime: "text/md",
    // automerge text
    content: Text("User written text #hi #hello"),
  },
  "org.eg.labels.hashtags": {
    srcRef: "db://self/org.eg.freehand?at=self",
    tags: [
      "hi",
      "hello"
    ]
  },
}
```

## 2026-01-24 | pseudo labeler

Let's make this happen.
I'm thinking:
- Embed content
- Cluster embedding with known labels
- If not found in cluster
  - Label with LLM
  - Cluster new label
  - If new label not found in cluster
    - Add new label to known labels
  - If label found in cluster
    - Find label for cluter
    - Use cluster label
- If found in cluster 
  - Find label for cluter
  - Use cluster for label

## 2026-01-21 | existing format interop

I've been thinking, related to our recent thoughts on innovation...
mighth be best to avoid creating new data formats but instead, try to be a container for existing formats.
This fits in nicely with the FUSE idea since you can easily map it to an on disk virtual file that can easily be edited by other editors.

Concrete examples:
- Text only formats:
  - hledger.ledger
    - Do we parse/write out the whole ledger on each mutation?
      - Maybe store JSON repr of the ledger?
        - Not good, I'd like the file to keep it's own formatting/comments
  - Markdown
    - I don't like Markdown, it feels like something better is possible.
      - BUT...one can always innovate once they have the basics down.
- Structured
  - Opencanvas

## 2026-01-21 | key schema

The current key schema has a tag and id approach.
```js
{
  "org.eg.tag/1": "first",
  "org.eg.second/any-string": "first",
  "org.eg.second/a/path/even?": "first",
  "org.eg.second/org.eg.third": "ref to another key",
  "org.eg.second/8jc88Sad8hLKASdualsdlji/org.eg.fourth/123": "ref to another key in another doc?",
}
```

This is too powerful?
The id section that comes after the slash is only to be parsed by those interested and is to be treated as a string by everyone else.

Additionally, querying for an existence of a tag sucks. 
You'll need to see it any tag+id keys also exist.
I.e. you'll have to scan over all keys.
Though one could argue that if you don't know the id, you shouldn't able to access it.
Capabilities system that will mediate access will have other blockers anyways.

The reason the current design is like this is:
- Misunderstanding of automerge array semantics
- Evolution with array tag list of Nostr
- Simplicity of adding a new repeated item
  - You just add a new key as opposted to upserting an old one 

But still, the design is not great. I think I'm going to remove the id approach.

---

Our design is sort of a flipped inside out PDS from ATmosphere.
In their scheme:
```yaml
- user123:
    org.eg.one:
    - myDocId.json:
        myKey1: myValue1
```

Wheras in our scheme:

```yaml
- user123:
  - doc456.json:
      org.eg.one:
      - myKey1: myValue1
```

AT seems to prefer hard types on single documents.
There are a lot of clients that need to read all X types of a single document.
In other words, it's less opinionated and more standard/UNIXy.
They're also using the schema id as a key for top level collections.

Daybook allows documents to contain multitudes.
A document is more like a directory.
Think OpenDoc.
There are usecases for querying all X types of keys across the repo but, in most of them, users are operating on a single document at a time.

I think they're mostly equivalent but it does raise questions.
- I like that they put the createdAt and so on in the leaves.
  - In our case, the document has created at but prop metadata is external to the actual content.
  - Making documents self contained is great.
  - On the other hand, I've instinctively tried to optimize for easy of writing code against daybook docs (daydocs??).
    - Ease of scripting, plugins and JIT software.
  - Should I store the metadata in the document itself?
    - This would mean having to fetch and decode each doc in a repo to get all meta.
      - I expect other usecases would have this need anyways.
    - This would mean two automerge txns for every mutation.
      - One for the real write and one for the metadata which contains commit heads.
        - Yikes, yeah, The commit head of the branch metadata would immediately stale out.
          - We can still move prop metadata within though.

## 2026-01-20 | innovation?

we don't exactly need to innovate here.
Providing a good free and private alternative to available proprietary products is good enough.

## 2026-01-20 | agents, how?

Claude Cowork is a good idea. 
How does it fit in here? 
Don't you dare expand the scope now.
A thought exercise?

Essentially, unix based computer use seems fantastic.
Why even build on top of automerge?
Just build an agent to use git directly and provide a mobile app to talk to it?
It can find or write the documents on demand?
A purely human language based interface.

But, chatbots are not perfect yet. For repeated/common tasks, I don't want to use my words.
Additionally, additions to the vault by agents should be kept to a minimum.
Or a minimum visibility anyways, "no one likes reading code".

## 2026-01-20 | expense tracking how?

- A document comes in
- Said document is identified to be related to expenses
  - A transaction artifact
    - Reciept photo/screenshot
    - Account activity email
    - Account activity sms
    - Actual receipt
  - A transaction document is created
    - Existing accounts are matched hierarchically
    - New accouts can be created if not recognized
- How do we veiw account status?
  - Mantain a hledger file?
    - A collator doc
- How do we identify files?
  - Embedding => proximity check?
    - Storing embeddings is expensive
      - Embedding => proximity check => tag?
        - Nonsense and untaggable files?
          - Blurry photographs?
  - Image => Desc => Tag?
    - Image tokens are not cheap, we should only do VLM if need is identified.
- How do we group multiple documents of a single transaction?
  - Collator on the ledger document?

- ACL capabilities
  - glob `docTag/*` capablities
  - predicate access capablities
    - All XXX keys across docs
      - Do we need an index?

## 2026-01-17 | dynamic plugs

In the current system, code execution on documents and definining new schemas and so on is determined by plugs.
I'd love to be able to quickly make these things in app.
Instead of a rigid query based system, that'd go far.
And vibe coding does make it accessible.

My decision to go with a non web tech are a hinderance here but embedding a browser is always an option. 
Projects like https://json-render.dev/ are encouraging, I'm not the only one thinking of generated UI after all.

Blockers:
- I'd like every user code to run in WASM
  - Boundary costs
- UI tech
  - WASM boundary cost again

Usecase?
We could focus all collators on this I guess.
A system like obsidian datacore where, ime, a lot of the value was when I wrote JS and not with the QL in place.
Admittedly, I had the rich html and markdown canvas beneath it.

## 2026-01-17 | software as art

I've been muling over what's unfolding in this repo, the ambitious irrationalities, the perfectionism bullshit, the political undertones...a lot of elements that makes me pause before I really commit to the bit.
And commit to the bit undersells it since this, after all, is intended to be open source software project.
Open source, a big stupid thing that many of my peers have cast our hearts to in a stupor of collective meaningfinding.
A fucking mirage. 
Endless tablespoons of horsedung.
Your bog standard political movement really.

Anyways, this is not the place for critiquing opensource culture but it's definitely a question one should ask before commiting.
And this is what I've been pondering when I recently went to find out why it's been a minute since I've seen my favorite internet philosopher.
CJ The X has a lot of things to charm the audience but a highlight is how well he's concentrated and collected his love of art.
A love that's stark in how well it's examined, how well the limitaitons and the faults are understood.
He's a philosopher, I suppose, and of the video essayist variant so examination is a big requirement.
But it does make one envious.
For if I'm to dedicate my most to something, I'd love to love it to such a degree, warts and all, no misgivings.

Now, there's a lot in here that's wanton of love but a big part of it is the fact that it's to be opensource software.
I haven't done opensource and some part of really doesn't want to.
It seems like a shit deal.
A shit deal only a mother could love.
A thing you do entirely out of love.
There doesn't seem to be a lot of money in opensource and I'm not some engineer on an Alphabet payroll. 
I'm fucking unemployed.

Besides those opensource projects bankrolled by trillion dollar companies that are entirely defined by how much they guardedly control the computers of our lives -- yeah, besides those -- most seem to be either abandoned or are dead lamp away from being abandoned.
Underpaid and always on call for an ungrateful audience for some bullshit they started on a whim decades ago, god bless those old white dudes but what a shit fucking deal.

This doesn't cover all of open source ofcourse.
There are many that were written recreationally.
Someone having fun trying to solve some problem that one July.
I'd love to daily drive the realized vision herein but oh, I couldn't just have a fun weekend project the idiot that I am.
No, the scope is has already eclipsed the horizon for recreational.

Can I sell it?
Make it proprietary?
No.
Why would anyone buy this?
It's literally designed to be unbuyable.

So what then? Just abandon it like they all do? It makes so much sense. So many other opporutunities calling. Reasonability an arm stretch away. Just forget it dude.

That's when I came back to CJ The X's thoughts and positions.
He has a project he's been working on for years now.
Not a swan song exactly but something he's teased about and stated to have taken over his life (citations bitch).
I think it's been 3 years at ago at this point and who knows, 3 more for all I know.

Why is it taking him long? 
Can't say. 
Doubt it's perfectionism.
He's released a lot of work already and his close examinations of the sin of perfectionism...it can't be.
This is an affliction of many artists that have already released estabilished works, a big, pre-announced project taking forever.
It taking over their career, drowning them in expectation, head of line blocking and all.

Now, there's little in that situation to compare with opensource work and this project in particular.
In fact, it's a simple difference that struck me to write this down.
Take over their career until their death or not, art objects, unlike fucking software, will have an actual done date.
Not a release date, a done date!
They can just put it down one day, with its flaws and all, and that's it! 
They never look at it again for the rest of their lives and it'll still be there. hardly losing any value.
An art object all the same.
Others can appreciate it even.

Software?
Big pile of software especially?
It fucking rots.
It falls apart.
Imperfections? You'll have to withstand every day of your use until you don't.
Or, even more tragic, someone else has to.

Is there a way to practice software as an art form?
A method of closing chapters that doesn't signal "here be dragons"?
Howw?

Maybe I was quick to cast rocks at the abandoned and barely abandoned projects.
Maybe it wasn't abandoned, but done?
Light mantainance viable dude?
Maybe LLMs can be told to light mantain it for eternity, no developer required.

Nothing wrong with recinding your commitment later on anyways. 
That one guy in hacker news screeding about how "open source developer don't owe you shit".

Ofcourse, finding replacement mantainers is the best solution.
Building it around a community.
The open-source dream.
Someone will fork it if they need it or something.
Jumping the gun.

I'm going around in circles.

## 2026-01-17 | mltools

ML stack. This is going to be difficult. I have no idea what I'm doing.

What do we want? Let's lay that out:

- Machine learning methods to analyze, summarize, process and use documents.
- It should run wherever it can.
  - Some ML tasks can be done on a mobile.
    - Embedding
    - OCR
    - STT
    - Segmenting
  - Some can be done on a desktop.
    - Each device ought to support higher quality impls of tasks of lower devices.
    - Opt-in for server tasks.
      - Emphasis on opt-in.
    - Small LLM.
  - Some must done on at a server.
    - Large LLM.
    - Image editing.
  - Some we must pay for to token providers
    - Agent LLM
    - SOTA LLM.
    - LLM voice chat?
- We need to have some measure of difficulty or quality on every task to allow or require uptiering the machine for a better model.
  - This implies delayed analysis as per our offline constraint.

Hard realities:
- Local LLMs are not ready.
- ML OPs is not self-hosting friendly.
  - Rent-a-GPU is not cheap.
  - Tuning requires a lot of expertise of which I lack.
- High degree of batching is a must on the cost analysis.

Decisions:
- First class support for API providers.
  - Bring your own keys support.
- Starting set:
  - Small LLM.
    - Gemma-series
    - CPU gemma3 is very slow on image
      - Research cost-effective GPU providers
  - OCR
    - Phone: PaddleOCRv5 on ONNX
    - Server: PaddleOCR-VL
  - Embedding
    - sentence-transformers on Phone??
    - text-embedding-3 on Server.
    - jina-embedding-4
  - STT:
    - paraqueet on server.
    - kyrutai for SSTT on server.
  - Segmenting:
    - What's that model from Meta? SegmentAnything?
- Single rust crate that has pluggable backends for each location
  - How to ship python + onnxruntime?
    - PyO3?
    - Just use candle?
      - Model wrapper python libraries are too useful
        - Encode expertise that would take time to translate

## 2026-01-12 | one dollar

here's an ambitious grounding goal/question, can you offer a service tier that starts at 1$?

## 2026-01-10 | dispatch system

This is taking way too long but I do have a working prototype for the plugin runtime very similar to what was described a few entries back.
It allows me to invoke routines on documents manually or when the documents themselves change in a reactive manner.
The latter case does have murky semantics we need to figure out.

First issue is that execution state lives in a separate dispatch repo in addition to the wflow state.
This state is partially backed by automerge but still, I'm wondering how well this will clear cut semantics to avoid duplicate/clashing work playing out on different devices.

Some more decisions:
- Put wflow processors in a different branch and merge when they succeed.
  - I expect most processors will use their own keys for state so should be merge conflict free...mostly.
  - What happens if we dispatch the same processor twice in a row after observing changes?
    - Let's cancel the job on the rt for the old one.
    - Clean up the branch if any state was written there.
    - Where to put the state for (doc_id, processor)? Triage worker state methinks.
      - Listen on dispatch repo to clean this up.
- We need to reliably clean up after routines are done/discarded.
  - Tail the wflow log to find rejections.
    - We need the dispatcher to reliably depend on wflow.
      - Let's attach wflow partition ids to dispatches?
  - What hapends to outputs?
    - All routines are doc based so the output is in the buffer/doc?
    - What about errors?
    - How do routines call other routines and get results? Should they?

The biggest question is what is the execution location of the processors and routines. 
We want to execute them globally once but should we be storing their state in automerge and synchronizing?
That seems like it'll be a lot of garbage state for automerge.
And this state will need to share the automerge doc with the the drawer which shouldn't have to deal with that much garbage.
We need a solid scheme to avoid clobbering and make sure any conflicting state resolves in a determinstic manner.


## 2025-12-27 | plugin-system

The doc scheme I have...

```
Doc {
  "org.example.my.foo":  Foo {}
  "org.example.my.bar/1":  Bar {}
  "org.example.my.bar/2":  Bar {}
}
```


..is essentially RDF. 
Not sure what to think about that.

## 2025-12-25 | plugin system

Wait a minute, if I make `content` itself a `Doc` `prop`...essentially make all details of a `Doc` a `prop`, I can use prop predicates to properly ACL doc access by user code.
I.e. any routine must define what props it can read and write up front.
Since prop keys are namespaced by reverse domain notation, this shouldn't lead to issues with clashes across plugins.

- Reverse domain names tie this system to DNS names? Maybe we should use the plugin registry as our name system?
  - Or maybe I can use `dns.com.example.key1` and `github.townframe.daybook.key2` where the first segment corresponds to the namespace itself?
- Okay but this is basically a type ontology in disguise. Do key names mandate what shape the value takes?
  - Or make it convention based??
  - YES! Each key must register the schema of the prop type.
    - It's easier to reason about.
    - Removes the need to create separate schema zoo.
- A big aspect of the design is to foster a positive, low-effort ecosystem. Think the glorious nixpkgs. Any other ecosystem would have a hard time mantaining large package trees if the system wasn't reliable.
  - Assume short context window by plugin authors. Most of the details should be handled by the system.
  - Bad plugins shouldn't run and destroy/ruin data.
    - Automerge should make this recoverable but still.
  - Plugins that want to depend on keys not defined by the manifest must declare the originating plugin as a dependency.
    - Through semver.
    - Systematically assure breaking key schema changes lead to major version increments?
    - Is there a non-convoluted way to assure that besides a semver, a package depends on a specific shape of a key?
      - Instead of depending on a package, it depends on a specific key+schema combo.
      - A package either declares/owns key+schema or depends on specific key+schema.
        - Reject there's a mismatch between the key+schema depended and the key+schema defined by owning package??
        - This would lead to issues if semantics change but shape stays the same.
        - On the other hand, this would allow us to do the type checking at boundaries instead of plugin code.
      - What happens when the owning package breaking changes a key?
        - This should lead to a new key.
        - Plugins ought to support old keys.
        - Key+schema migration can be done mechanically in some cases.
          - But mandating all documents being updated is silly.
          - Automechanical upgrade system would incentivze breaking changes.
          - Docs in cold-storage would go stale fast.
          - Plugin can always provide a command I guess.
            - Provide specialized case/ui for this but registry should de-weight packages that do this per instance.
        - Who's the arbiter of keys+schema validity?
          - The code in daybook_core, not just the online registry.
      - Generic schemas seem like a good ida, hope JSON schema supports this.
        - Or I suppose let's make do with `any` holes.
  - Just looked it up and the large number of packages in NixOS is bullshit apparently? Well, the explanation number is less impressive than I thought. Still, there's magic there.

      
Start from least generic impl and expanded as needed
  - Invoke doc routines
    - Execute on a user specified doc
  - Prop routines.
    - Execute when props are present on a doc and can only modify said prop
      - Must the prop be defined/namespaced to plugin?
  - Predicate routines
    - Execute on all documents that satisfy a predicate
    - By default will go into a branch that must be merged by a user
    - Can either be user invocation based or add a prop when a predicate is satisfied
  - Collators
    - Can read documents that satisfy a predicate but can modify pre-set document.
      - Are they run when collation document is modified or when predciate docments are modified?
        - For now, the former.

## 2025-12-23 | plugin system

Requirements:
- Commands for daybook_cli
- Commands for daybook_compose command palette
- Processors for props
- Event handlers for events

Early decisions:
- Capability based security
- Wasm worlds for each variant of plugin code
- Modifications from a routine get their own ActorId and go in a branch by default.

---

Concrete worlds:
- plug-wflow-bundle
- plug-routine

## 2025-12-09 | branch first

Patchwork is a big inspiration for this system and I've been thinking a lot about their branch based workflow.
- Fantastic place to stuff in unverified modifications.
  - From LLMs 
  - Other users
  - Processors

## 2025-12-03 | pglite

My Cursor subscription finally reset and I just blew 20$ of the allowance porting [pglite-oxide](https://github.com/f0rr0/pglite-oxide) to a wash plugin.
Not even sure if I'll stick with it honestly, but it does trump out SQLite on "shiny" which is a metric for some reason :p.
Comparision with SQLite:

- Sqlite pros
  - Starts faster ++
  - Is generally faster +++
  - Single file database ++
  - Not experimental ++++
  - Litestream exists: a generally avail solution for PoP edge +++
    - Probably can write something simlar for postgres -
  - Can use sqlx ++
- Pglite pros
  - More extensions ++
  - Can use postgres on server side for large deployments +++++
    - Litestream and Turso have an answer here but they'ren not ready -
  - More features/prorammable ++
  - I love postgres? ++

SQLite should win by all means here but I really do love postgres.
I'm going to PGLite a bit more chance.
The biggest win of SQLite is that it's "not-experimental".
I.e. PGLite will probably have bugs and unsupported edge cases.
I'll continue with PGLite but with the expectation of quick retreat to SQLite when I encounter these.
I do want to see how far I can take it.

PGLite does have a lot of promise, right?
Can't wait until *someone* (NOT ME!) gets it to build to wasm32-wasip3 using wasi-sdk over wasi:sql interface using wit-bindgen for C.
I can't afford any such detours.

---

Make that $40 ;;

## 2025-11-29 | "routing"

So there are a bunch of overlapping concerns here. 
My brain is fried for some reason but I'll try my best to lay them and a general plan of action out.

- Distributing work across remote and local machines
  - Specficially in the local-first sense
- Discovering available workers
- Choosing workers according to who can do what and other preferences.
  - Some work can only be done on the server and vice versa
  - Server might not be available
- Configuring workers with workloads.
- Where does work come from and when/where is the routing done
- Building doc processing pipelines
- Allowing user programmability for pipelines
- Events that originate from server
- Retries/re-routing
- Metering!
- Cancellation

Early decisions:
- Put config in automerge document
  - Routing decisions can be tagged with head of config commit
- Generic event handler system
  - Stamps with head of config for each event

Let's just trace it out.

- Document gets created by user
- documentCreated event handler runs
- A bunch of listeners run
- Some listeners schedule wflow jobs
- Some jobs need to be scheduled on the server
  - We need to make sure this happens by re-trying
- Jobs emit a bunch more events
- How do server jobs emit events to local?
  - They will have to modify document
- How do we re-run events for documents?
  - Tags on documents to identify processing status
  - User created pipelines can use tags to make sure all docs are gotten

Wait, generic doc tag based event system??
- Schedule documentCreated job when there's no docProcessed tag on doc
- On the other hand, pipelines can run when a tag is detected.
- After a pipeline is complete, it'll replace the initiating that with a result tag.
  - Result tag can link to separate document.
- Use idempotency keys to make sure only one such job is running for certain doc
- If we want to force re-run a pipeline, just remove the tag.
- Jobs can themselves decide to move to a server:
  - They either
- More brainstorming with a chatbot at [chat.com](https://chatgpt.com/share/692afef9-275c-8007-82de-8143ff7cc19c)

---

Okay so I have something okay

- Predicates
  - Some basic targetting for processors
- Reconciler
  - Goes over (doc + predicate) pairs and makes sure jobs are scheduled
  - Uses idempotency keys to avoid duplicates
  - Predicates can contain versioning to allow re-runs
  - It will run on doc or predicate changes
  - It will cancel jobs if predicate is no longer fulfilled
- Processor
  - Will run on wflow engine
  - Have cancellation policies?
- Tags
  - Attachment to docs
  - Samples
    - Embedding
    - PathGeneric

- Hmm, here's a question? Is this indirect system too difficult to program?

---

The following is just an copy of some thoughts I'd scribbled elsewhere in the past

- A document is created
  - Photo taken
  - Note written
  - Speech recorded
- DocumentEvents are put in an API somewhere
- Parametrized event handlers then get to work
  - I.e. if event == xyz, run handler abc
- Is this message based actors basically??
  - This system doens't allow one handler to suspend and wait on another

```
- Doc
  - Composite
  - Structured
  - Text
  - Image
  - Video
  - Audio
  - PDF
  - Docx

- DocMeta
- DocMarker
  - Embedding
  - Type

- Event
- EventListener

- Services
  - DocPipeline
    - documentCreated()
  - DocEmbeddings
    - embedDoc()
    - nearestKDocs()
  - DocMetadata
    - extractMetadata()
  - DocChatAgent
```

---

More old sketches.

- React native
    - OTA updates
- Livestore
- Local-first frontend
- Postgres (DB)
- Kanidm (auth)

-- 

New version

- Compose multiplatform
  - We need this for the native integration on Android
  - I like the desktop support
  - We do miss out OTA updates but those were limited anwyays
    - We ought to rely on a system similar to Telegram
- Automerge
  - Use our own stupid sync server
    - Connect through websocket
  - Backed by object store storage
- Kanidm is a pita for some usecases
  - Consider Authelia
- Compute
  - Wasmcloud
    - Should be used for 99% of mutations
  - Durable store
    - Not
- Database
  - Postgres for relational
  - Redis for kv
- Processing queue

## 2025-11-29 | serverless wflow

Right now, with the path I'm treading, wflow will be of the "scale up partition" when needed and scale down to zero when not.
That's good enough for now but I'll have to keep it mind on how we can make it more efficent.
What's more, it'd be great if it can all be wasm native, all parts of the engine.

## 2025-11-22 | wflow in memory

Well, we have the in memory version with zero features working.

## 2025-11-08 | wflow details

This will be a rough reimpl of restate.
I don't want to loose time inventing from scratch.
Nor coming up with new names.

- Wflow: the impl.
- Service: the live implementation.
- Job: the instance.
- Partition: the engine.
- Log: the ground truth.
- Cache: accelerator over log.
- Ingress: inngest/API for system.
- Metadata: the wflow and partition information registry.

```rust
metadata.registerWflow(key, wflowMeta)
// job added by client
ingress.addJob(key, args)
  // event persited in log
  log.addEvt(newJobEvt)
  // client gets 200
// worker gets new event
worker.eventAdded(newJobEvt)
  // starts wflow on service
  service.startWflow(wflowArgs)
worker.addEvt(wflowEvt)
  partition.eventAdded(newJobEvt)
```

## 2025-11-07 | wflow

I am guilty of yak-shaving here but I think it's critical workflows are able to run on local devices.
Still, the only way to asuage my guilt is to power through the impl asap.
I feel that I'm just setting myself up for failure.

## 2025-10-26 | architecture

As I start to build out more features, I'd love to have in hand something that will take me far.

Concerns:
- Portable execution (client or server)
  - Wash is building out soemething nice here
- Possible plugins through wasm
  - Again, very positive about wash here
- Possible multiple UI impls
  - UI constrains around uniffi
    - FFI boundary is expensive to cros ruling out Elm
    - Crux provide a nice abstraction here but maybe too much abstraction?
      - I think I'll wait on them to make some progress and see how that shakes out

### 2025-09-18 | Wasmcloud

After long delays due to some tooling bugs, I was able to complete the wasmcloud based API system.
Overengineering crap but I do like the result.
Even though I feel like wasm is an important part of the vision, I can't help but feel this is loosing focus.

## 2025-07-26

Spent the day trying to get it to start on desktop.
That's like 3 hours of trying to debug the JDK issues and 3 hours of writing a ghjk port for it.
What a waste but at least I did get it started on desktop.

### 2025-07-20 | daybook

Spent the weekend vibe coding the magic puck stuff. 
I feel productive somewhat productive.
I have to downscope fast.

## 2025-05-31 | daybook

I need to make this happen ASAP. Everything depends on it.

### 2025-03-05 | Proc macros

Spent today porting glue code from the aggy codebase.
Cleaned up some of the boilerplates due to new proc macros.
I had an LLM write it for me.
Well, it certainly helped anyways.
Next time, I'll have to setup the db stuff.
Can't wait to start on actual feature work lol.

### 2025-03-04 | Ramping up

Trying to get the show on the road.
This is a full stack project in the traditional sense.
I'll need to write a web app, backend API and possibly a mobile app.
I'll be starting out with Granary which seems doable.
