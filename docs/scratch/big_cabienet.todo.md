# Todo

Everything the big_cabient PR must deliver

- [ ] Meta 
  - [ ] Design and sign off on testing approach
    - [ ] Example based testing
    - [ ] Property based testing
    - [ ] Some kind of snapshot tool to detect public interface drift. 
  - [ ] Design asynchrony model
- [ ] big_cabinet
  - [ ] AuthorityRepo
    - [ ] Actors
      - [ ] Capability tokens for actors
  - [ ] DocumentsRepo
    - [ ] Ensure writes respect schema
      - [ ] Ensure schema violating writes are rejected
    - [ ] Have writes take actors 
    - [ ] Design and sign off public surface
  - [ ] DrawerRepo
    - [ ] Create drawers
      - [ ] Create single documemnt drawers
      - [ ] Create single group drawers
    - [ ] Modify drawers
    - [ ] Design and signoff on drawer facets
    - [ ] Public agent support for drawers
    - [ ] Drawer relationships
    - [ ] Design and sign off public surface
  - [ ] SchemaRepo
  - [ ] FacetSet/FacetRef
- [ ] big_cabinet_core
  - [ ] Full dmeta implementation
  - [ ] Redo branch facets
  - [ ] Facets codecs
    - [ ] Rust facet authoring tools 

Future PRs:
- [ ] Subset drawers
- [ ] Managed participants drawers


that's the thing, we don't give them the durable journal right away. it's interperted by the service. the service then ensures read-after-event violation can't happen befor brodacast

Coalescing must depend on runtime timing lest we do intermiedate reads. Every other revisioned stream works like this and in most cases, anyone is only interested in the latest verison. If services want to offer intermediate states, that's their buisness. Sure this means that live observers get different events but that's okay.


yes, dmeta updated at can lie but we're only using them to order generated events. we have to make them determinstic by using tie-breakers but I don't think assuring globally identical order here is necessary since we've already violated that with hte previosu point. We can do better by mapping the automerge change that produced that update and then ordering those but that requires querying the automerge metadata and I'm not sure about the cost of that. THat's definitionally a graph walk or can automerge do it better?

yes, so version tags will bloat our atuomerge history forever so small values are better. snowflake ids or soemthing 16byte or lower is better.
