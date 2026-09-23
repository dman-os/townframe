!> is the drawer analogous to an "origin" on the web? webpages have origns and are defined by it. drawers will define the documents with regards on how they're resolved and stuff.
!> so maybe drawers will become part of document identities. in the blob ADRs, we decided that a hash is the blob identity but a URL might contain resolution information like the cyphertext versions of the blob and maybye which nodes might contain it?
!> similaryl, maybe drawers and documents and nodes have that kind of relationship
!> let's discuss these usecases with regards to how discovery, delgation and so forth plays.
!> keyhive provides a good direction we can respect. essentially, every node will have an idioesyncratic hive and just because it know of a document/user/group in it's hive, doesn't mean it has the contents. It's the metadata graph.
!> now, while it's not implemented today, we might want to support rejctiong keyhive events or pruning our graph? we don't want the relay to learn about every damn document it's users have even tho today's keyhivie sync will make sure all vertices/edges of tthe graph propagate
!> simlarly, i dont want to have to ingest the wikipedia graph to be able to use it. 
!> that's what XRPC can help us solve maybe
!> additionally, whlie most indices will be built locally, we might want to have indices built by other nodes
!> now, ofc, I want the p2p to be first and I'd love to have some kind of index that is synced p2p wise. we do have the tools to build such indices
!> why synced? well, p2p doesn't mean that all nodes will be avail so ofc, if an index is only found on an unaddressasble node, it breaks the app
!> but on the other hand, there are some usecases that make sense in a communal/public way like the web. public indices.
!> now, I don't want to burden keyhive with this requirement and also, I don't want to recreate big search engines and aggregators politically speakin
!> this is where ATProto will come in in the future. Essentially, what I want to enable is that we can have relays offer ATProto services on the side. You can through drawers and other primtives setup atproto PDSes and other parts of the stack to take over for such usecases
!> ofc, having XRPC at our node layer also allows us to have it play with the authenticated index uscase using our own principals.
!> but yeah, having dedicated XRPCs helps us in some index cases
!> btw, XRPC for reading/editing documents, that's going to be a minrotiy uscecase.
!> today, subduction rejects writes for docs not known by the keyhive. in the future, we can have the subduction and keyhive protocols allow partial interoperation. i.e. pushing in a single doc into some other node withoug doing full blown sync and so forth
!> btw, today, in big_re
