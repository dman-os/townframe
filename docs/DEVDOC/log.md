# duck-log

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

Wait a minute, if I make `content` itself a `Doc`...essentially make all details of a `Doc` a `prop`, I can use prop predicates to properly ACL doc access by user code.
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
