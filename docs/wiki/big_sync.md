# `big_sync`

Big Sync is an efficent sync coordinator for large lists of objects.

# Why

Our primary user of this system is [./big_repo.md], the automerge CRDT storage and sync system.
While big_repo uses the [./subduction.md] protocol to sync each CRDT across peers, big_sync handles the mass/collection sync usecases including:
- efficiently diff presence *and versions* of objects across peers
  - Set reconcilliation in other words
- efficient live change notifications of objects to connected peers
- only give out object information to authorized peers

# How

Each obj, identified by a byte string ObjKey, holds a payload.
Payloads are JSON objects.
Objs are members of Parts, identified by byte string PartKeys, then contain these objects.
A single object can be member of multiple parts.
All of this data is mantained in the PartStore.

One critical need is that object part membership changes are independent from object payload changes.
It should be possible to create objects without knowing the parts yet and, more importantly, it should also be able to assign an object to parts without having the payload yet.

Every change operation is then tagged by a monotonically increasing txn id that each node mantains locally.
I.e. operations are ordered on every node.
The part store mantains roughly the following events per data object *but* only the latest one:
- object payoad was changed
- object added to part
- object was removed from a part (tombstones)
These events are all ordered by the txn id.
Note that the txn id is global for the node and not per object or part.

Now then, on this data model, big sync offers two "syncing" strategies, Cursor and Bucket, that are intertwined with space for more in the future.
The primary and headline one is the Cursor strategy.
Everything about big_sync, including the core data model, was designed and built around the cursor strategy.

Before we describe the strategies though, one thing must be highlighted.
Big sync itself can't be considered a set reconcilliation implementation or a CRDT.
A better description would be it's a building block of one.

These strategies are not doing set reconcilliation themselves but are more accurately described as "replay state machines for events on other nodes that take into account local state".
Strategies don't ever write to the part store directly. Instead, they send these replayed events to SyncBackend abstraction.
The SyncBackend can do as it wishes according to the event (which *usually* contains the object payloads) and it doesn't need to write to the local part store.
In big_repo for example, the objects are automerge CRDTs, their payloads are the heads digests and the SyncBackend starts a subduction sync session if it detects that the payload heads are different from what it has locally.
More importantly, the SyncBackend must ack the event before the strategy considers that event succesfully replayed.

## Cursor Strat

As the name suggests, cursor strategy uses the remote node's event txids as cursor.
It keeps track of the last succesfully replayed event of every node it meets and uses it as a lower bound when getting the next page of events.

Now, the part store doesn't really store a full event log but latest event per object.
I.e. the cursor strat thus won't observe intermiedate object states/payloads.
Concretely, only two events kinds are handled:
  - ObjChanged
  - ObjRemovedFromPart

ObjChanged contains the full object payload and all the keys of parts the object is a member of.
<!-- this is actually very wasteful on live replay scenarios -->
It's emitted on any object payload or part membership change.
ObjRemovedFromPart is what it says on the tin, one event per part.

Concretely, there's no ObjectWasRemoved event.
The networked view of a node is entirely framed by parts.
When the cursor strat pages events from a peer, it specifes a list of parts and objects it's interested in.
The peer then returns one collapsed event across parts per object.
What's more, the part store uses parts as authorization boundaries.
A peer is authorized access not per object but per part.
The cursor machine doesn't see multiple per part events on object paylaod changes and it only sees objects that are members of parts it asked for and parts it's authorized for.

The cursor machine only progress it's cursor under the following cases:
- SyncBackend has acked the event as succesfuly processed
- A new event comes for the same ObjKey which will make the old event obsolete

On restart/reconenct, the cursor machine continues from the last cursor that has been acked.
This means that sync backends should support seeing the same event again.
The backends must implement its own correctness or recovery seams to avoid cursor stalls.

Once the machine catches up to the latest cursor, it long polls for the next event. This serves as the primary live replay seam of the system.

The cursor strat work's well for new peers that must fully enumerate and sync full objeect streams and it works well for live catchup but it has issues for peers that have mostly the same data and haven't met before or it's been a long time since they met.
The bucket machine was written to address that gap.

## Bucket Machine

The bucket machine is a more traditional set reconcilliation implementation that uses fingerprint hashes on ranges of objects to achieve more efficent diffing/replay.
Instead of arbitrary ranges, it breaks up the object space into ranges called buckets by hasing the object keys and using hash prefix to bin items.
Hashes are used used instead of the raw ObjKeys because random distirubtion ob objects is desired across keys improving bucket residence/distirubtion.
It'd be useless if all the objects were in one bucket since the fingerprint will always have that large bucket as dirt even though only a single object has changed.

Buckets are identified by the prefix they denote and there are multiple levels of buckets which correspond to the prefix length.
The root bucket contains all the objects and has a length zero id.
If the root bucket fingerprints are equals for a peer, that's it, no need to replay and events, they can directly move on to the CursorStrat for live replay.
If the fingerprints of a root or any other bucket differ, either of two things is done:

If the number of objects in bucket is small, we walk or *leaf* the bucket. We get the keys and payload fingerprints for every object in the bucket and if the fingerprint doesn't match the local object fingerprint or if the key is new, we send this to the sync backend.
Leafing also returns a `dead` flag for tombstoned objects which is how removal is communicated.

If the number of objects in the bucket is large, we can instead move on to the next level. The bucket count per level is determined by the prefix length we must increase to go one level deep.
Right now, the bucekt strat is using 2 bytes per level which means that every bucket contains 16 child buckets.
That's 16 buckets on level 1, 256 on level 2, 4096 on level 3, 65536 on level 4 and so on.

Nodes can configure the maximum depth they're willing to mantain.
The default today is 4 levels which gives an expected 15 objects per bucket with good hash function to keep the distribution random.

The machine keeps leafs or dives buckets until it has sent every different object to the sync backend.
Once all the events have been acked by the sync backend, it can then hand off to the cursor strat which is enabled by two facts.
First, we get the latest cursor of the peer before starting the bucket machien to get the lower bound rom which the cursor strat can cont.
What's more, we actually start a cursor strat concurrently on the same peer to handle any live events while the bucket machine is still active.
It can be said that the bucket machine is just efficent replay of events upto the latest cursor of a node.

Now, all well and good but I've been holding back a sour unhappiness that ruins the fun.
We have to do the bucket strat per part and not per peer.
This is because authorization happens at the part granularity.
We can't build a gloal bucket fingerprints lest those fingerprints contain obejcts that the remote is not authorized to see forever breaking equality.
This means that the bucket fingerprints must be mantained per part and the bucket strat will pay dupe costs for any object that straddles multiple parts.

Thankfully, our bucket fingerprints can be incrementally mantained on every object write and we can share them across peers.
There are also wire inefficencies around the bucket dive/walk algorithm.
It's possible for a single dirty objet to require diving all three levels deep.
We do dive back up if the next bucket is detected to be equivalent to avoid this cost but inefficencies still stand.

Having per part fingerptints does open up opportunities for other set reconcilliation algorithims like RBSR and RIBLT which ought to be implemented if costs are terrible.

## Object parts

For every object, we also support a virtual, non-physical part that exists at `o:<objkey>`. Peers can long poll events on these parts which will act as traditional parts across the wire.
In order to have access to an object part, a peer must have access to at least one part that contains that object.
Object parts don't get ObjRemovedFromPart events for obvious reasons.
If object is removed from all parts that the peer has access to, the long poll will reject with "Unauthorized" on the object part.

## Removals

Object removals are rather poorly modeled today and strictly in the backend's court.
While big_sync itself mantains part membership tombstones, replay is not always good enough to decide to remove something.
A separate backend specific authority is usually needed for removals.

Since there's no global ordering of events, an object that was removed in one network partition might have seen updates in another.
Even if there were no updates in the second partition, node exchange doesn't have a global event ordering so nodes that have removed an object might end up seeing it again.
Sync backend could look at the tombstone and decline and in this case, the removal event should then propagate through the other partition ensuring the object is removed on all.

But if the object was genuinely resurrected, the big_sync system itself doesn't have enough information to decide. 
Backends shold put extra information like timestamps, epochs and other causal mechanisms in object payloads to determine the semantics of their system.

If an obejct is removed from all parts, the payload is not removed. 
While this object becoms ineaccessible to peers, auto-removal would break re-addition of objects to parts.
The sync backends must garbage collect such objects if part removal semantic is desired to be object removal.

# Factlist

- There is no network global event ordering or cursor
- If both peers are symmetricly replaying each other, a sync backend deciding to write to the local part store means that the origin peer of the event will then get a ObjChanged event
  - big_sync based system converge by the backend implementing a system that at some point will produce no new events
- The whole system is asymmetric and pull based
  - While this forgoes some optimizations, it makes the implementation as simple as presented
  - It also allows the nodes to cont at their own pace without need of backpressure
- If an object was added and removed after the page's cursor, we don't need to transmit ObjRemovedFromPart events since the asking peer presumably hasn't seen the object.
- Bucket strat doesn't fetch the remote payload if fingerprint's mismatch which means that sync backends that really care about the payload to use other RPC mechanisms to get it. 
- The removal gap is just the natural inverse of a backend's right to decide if it should pull the object before acking the event. 
  - This flexebility is what really enables some of the users of big_sync today
- Cursor replay uses "stateful long polling"
  - Essentially, we want to read a page of events for N parts
  - But we want to only transfer on the wire a single event if it concerns multiple parts
  - Unforutnately, our target part set is always changing
  - Object parts are mainly used to get fastpath live object replay for currently used objects without fully waiting for full hisotry replay
  - I.e. our object part interest set if quickly changing
  - Long polling is used over event streams to make the system pull based which matches the strats
  - A problem is thus now apparent: we have possibly large number of targets that are quickly changing and long poll means sending these sets for every poll
  - By having the serving peer incrementally mantain the target set for a session, we eliminate uncessary wire transfer of PartKeys for every poll
