# daybook

## Notes

- Avoid adding dependencies if possible

## TODO

### Stack

- [x] Get uniffi working on android
- [ ] wflow
  - [ ] in-memory smoke
  - [ ] ingress workload
  - [ ] durablity
- [ ] Save a photo
- [ ] DRY up all the wit bindgen
- [ ] Move http tests into api crates
- [ ] Code generator for http wrapper
- [ ] PgLite based testing
- [ ] Policy against tokio mutexes (cancel safety)
- [ ] Move to wasmcloud v2
  - [ ] Use async on wit_bindgen

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
- Magic wand
  - [ ] Follow bubble behavior from android
  - [ ] Status bar/Gesture bar insets for puck and widgets
  - [ ] Puck drop required to be on center bug

### Upstream Issues

## design-doc

### Architecture

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
-

### Features

#### Stretch goals

- [ ] Android auto-updates

### Endpoints

### Schema

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

## dev-log

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
