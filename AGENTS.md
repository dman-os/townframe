./README.md

- Don't be lazy with solutions.
- Don't catch or log errors every edgecase.
  - Most errors are programming errors and should crash the program.
  - Errors that should be handled include
    - Network errors
    - User input validation
  - Don't swallow errors with empty catch statements or catch-and-log unless explcitly told to do so.
- To type check and lint the ./src/daybook_compose multiplatform app, use `./x/check-dayb.ts`.
- Don't be lazy with solutions.
- Prefer `cargo clippy --all-targets --all-features -p myCrate` over `cargo check`.
- When working with rust, in addition to `cargo clippy`, small tests can be used to validate ideas.
- Do not use the cargo integration tests features.
  - I.e. avoid making tests in crate_root::tests.
- DHashMaps shouldn't not be used for sync across tasks/threads. 
  - They easily deadlock if modified across multiple tasks.
  - They're only a good fit for single modifier situation where a normal HashMap won't due to do async problems.
- Don't be lazy with solutions.
- Do not adress TODOs or FIXMEs unless told to do so.
- Prefer to preserve comments unless they are progress comments written by an agent. 
- Don't be lazy with solutions.
- Use RUST_LOG_TEST env var for controlling log levels during testing.
- `printf` and experimental debugging is always quicker than coming up with premature hypothesis.
- CRITICAL: Never ever use git commands. Never!
- CRITICAL: Tackling somethign tricky?
  - Do small experiments using temporary tests to prove ideas.
  - If refactoring across many items, instead of iterating on all items at once, resolve one item and then apply pattern to the rest.
- Don't be lazy with solutions.
- CRITICAL: if the users request seems like it comes from a place of misunderstanding, push back!
- Don't use single char variable names.


Nice, so we're going to do a very deep feature impl that is going to touch multiple things. 

- First up is src/mltools/lib.rs. 
  - We want to impl ocr_image there similar to what you just did. It should return an error if no ocr backend is found. 
  - This will be wired with src/wash_plugin_mltools/lib.rs which must add mtlools proper as a dependency. 
    - It's literally working against the wit definition of mltools.
    - Actually, on second thought, let's compltetely ignore that plugin or the mltools wit.
    - Instead, let's add to ./src/daybook_core/wit/main.wit a mltools-ocr interface that will be much simpler. 
    - It doesn't require a context, it just takes a blob ref and returns the results
    - What we can do is we can define the types in mltools/wit/main.rs in a ocr interface that we'll then use in daybook/mltools-ocr. 
      - Mainly, it'll be ocr-result type at this point.
- What's a blob ref? Glad you asked. 
  - So we have ./src/daybook_core/blobs.rs
  - Additionally, we have in ./src/daybook_types/doc.rs the blob facet type
    - I've just added additional fields to it which diverges from the wit version and it's usage elsewhere. You probably should amend this first. Working with wit is tricky so I'm offering a lot of handholding here.
    - The urls field can only be a blobs repo url in the form of "db+blob:///dazsdj9sj98ad98cajs9d8cjasd". Note, we have empty authority
  - We'll have daybook/mltools-ocr only accept refs to a blob facet to do OCR on. It should just accept a read capable prop-token for this.
    - We should then take this token and read from the ./src/daybook_core/drawer.rs the blob facet 
    - We'll then use the blob repo to resolve a path on disk for the image which we'll feed to mltools::ocr_image
    - For now, let's just hardcode the mltools::Config and model paths at this step. We'll do proper sourcing and whatnot later
- For verification, let's write a wflow in ./src/daybook_wflows/lib.rs called ocr_image
  - It should just add a new note that has the OCR contents
  - we'll then add a tests in ./src/daybook_core/e2e/ that exercises this
  - let's use the image at /tmp/sample.jpg for now. Create a new document that has this blob as content.

Good stuff. So to get this into daybook/mltools-embed interface that has embed_text that accepts a string. Let's mirror the rest of our work from the previous OCR work and make a plan. One item of note is we want to avoid downloads so we'll load the models directly from disk similar to in ocr. The fastembed demo run has downloaded a nomic-ai model at ./target/models/.fastembed_cache that you should use (maybe copy/desymlink it elsewhere) for the hardcoding testing you need to do.

Also, let's make sure both mltools method use spawn_blocking for their major work to avoid blocking tokio.
