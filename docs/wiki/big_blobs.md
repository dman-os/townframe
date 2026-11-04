# big_blobs

Big blobs is the system that manages the complementary blob data for [docs](./docs.md).

## Why

While the Automerge CRDT works well for strucured data that can be represented in JSON, it's a bit worse fit for blobs.
It does have a byte string typse by hydrating automerge documents requires loading the full document in memory.
Additionally, we have the following needs from our blobs system driven by the following usecases:
- They're complementary to docs and thus must layer over them
- It should work in a mobile network scenario
  - Resumable downloads are a must
- It should be possible to adopt existing files in place as blobs
  - There should be no need make a copy into a separate store forcing users to pay the storage cost twice
- It should be possible to have relays serve blobs
  - Relays should only see encrypted blobs
- It should be possible to stream video or audio blobs from other nodes
  - Even from nodes that only have the ciphertext

## How

We'll be relying on iroh-blobs as the primary.

## factlist
