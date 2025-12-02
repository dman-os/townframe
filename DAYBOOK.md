# daybook

## Notes

- Avoid adding dependencies if possible

## TODO

### Stack

- [ ] Get pglite building to oci
  - [ ] For pglite-build
  - [ ] Maybe a wash provider?
  - [ ] Maybe wasi component
- [ ] Finish autosurgeon::Patch
  - [ ] Fork with context generic programming
- [x] Test to assert snapshot recovery works
- [x] Sqlite backed KvStore impl
- [x] Get uniffi working on android
- [x] Save a photo
- [ ] wflow
  - [x] in-memory smoke
  - [ ] Ingress workload
  - [ ] Durablity
  - [x] Snapshots
  - [ ] Use flatbuffers instead of JSON
  - [ ] web UI
  - [ ] Non-wasm impl
    - [ ] Catch panics
- [ ] Convert DHashMap to be wrapper around RwLock<HashMap>
- [ ] DRY up all the wit bindgen
- [ ] Move http tests into api crates
- [ ] Code generator for http wrapper
- [ ] PgLite based testing
- [ ] Policy against tokio mutexes (cancel safety)
- [ ] Move to wasmcloud v2
  - [ ] Use async on wit_bindgen
- [ ] Replace time with jiff??
- [ ] Wasi 0.3
  - [ ] wRPC everything
- [ ] autosurgeon doens't respect serde attribs like untagged and camelCase
- [ ] Pipeline editor web app
- [ ] wrpc + iroh

---

### Features

- Data model
  - [x] Document repository with tests
    - [ ] Tests
  - [x] Automerge
    - [ ] Tests
- Ingest
  - [ ] Screenshots
  - [ ] Photographs
  - [ ] Text input
  - [ ] Audio recordings
  - [ ] Browser snips
  - [ ] Social bots
- Magic wand
  - [ ] Follow bubble behavior from android
  - [ ] Status bar/Gesture bar insets for puck and widgets
  - [ ] Puck drop required to be on center bug

### Upstream Issues

## design-doc

### Architecture

- Compose Multiplatform
- Custom wash runtime
  - Locally
  - And also through wasmcloud
- SQlite or PGLite

### Features

- Daybook
  - Core
    - E2ee
    - Multiplayer editing
    - Muti-device
    - Auth
  - Application
    - Inbox screen
    - Capture screen
      - Photo
      - Video
        - Live transcript
      - Audio
        - Live transcript
      - Text
    - Config screen
    - Collection screen
      - Group by time
      - Path based tree
      - Table
      - Kanban
    - Markdown editor
      - Subtext better?
    - Screenshots
    - Magic wand
    - Share reciever
    - Print doc
    - Import/export
      - Markdown
    - Multiple window support
    - Document types
      - Images
        - Thumbnails
      - Videos
        - Thumbnails
      - Audio
      - Markdown/subtext
  - Server
    - Object store
  - Processors
    - Pseudo labeler
    - OCR
    - ContentToTag
    - Transcript
    - Thumbnails
  - Ingest
    - Telegram bot
    - Browser extension
    - Discord bot
    - Mastodon bot
  - CLI
    - FUSE tree on Linux
    - Directly processor on given file
  - Quality
    - CI/CD
    - GUI tests
- Granary
  - Metering for LLM requests
  - Metering for object store

#### Stretch goals

- Self hosted auto-updates for android app (think Telegram)

## dev-log

### 2025-11-29 | "routing"

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

### 2025-11-29 | serverless wflow

Right now, with the path I'm treading, wflow will be of the "scale up partition" when needed and scale down to zero when not.
That's good enough for now but I'll have to keep it mind on how we can make it more efficent.
What's more, it'd be great if it can all be wasm native, all parts of the engine.

### 2025-11-22 | wflow in memory

Well, we have the in memory version with zero features working.

### 2025-11-08 | wflow details

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

### 2025-11-07 | wflow

I am guilty of yak-shaving here but I think it's critical workflows are able to run on local devices.
Still, the only way to asuage my guilt is to power through the impl asap.
I feel that I'm just setting myself up for failure.

### 2025-10-26 | architecture

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

I 

### 2025-07-26

Spent the day trying to get it to start on desktop.
That's like 3 hours of trying to debug the JDK issues and 3 hours of writing a ghjk port for it.
What a waste but at least I did get it started on desktop.

### 2025-07-20 | daybook

Spent the weekend vibe coding the magic puck stuff. 
I feel productive somewhat productive.
I have to downscope fast.

### 2025-05-31 | daybook

I need to make this happen ASAP. Everything depends on it.
