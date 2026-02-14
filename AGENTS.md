./README.md
./CONTRIBUTING.md

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
- Don't be lazy with solutions.
- Do not adress TODOs or FIXMEs unless told to do so.
- Prefer to preserve comments unless they are progress comments written by an agent. 
- Don't be lazy with solutions.
- Use RUST_LOG_TEST env var for controlling log levels during testing.
- `printf` and experimental debugging is always quicker than coming up with premature hypothesis.
  - Need to prove/demo a hypothesis, throw together a quick commaind in ./src/xtask/ cli.
- CRITICAL: Never ever use git commands. Never!
- CRITICAL: Tackling somethign tricky?
  - Do small experiments using temporary tests to prove ideas.
  - If refactoring across many items, instead of iterating on all items at once, resolve one item and then apply pattern to the rest.
- Don't be lazy with solutions.
- CRITICAL: if the users request seems like it comes from a place of misunderstanding, push back!
- Don't use single char variable names.
- If you're not able to cleanly read a provided web link through tool calls, pause and ask for a copy/paste of the contents. NEVER ASSUME THE CONTENTS OF A LINK YOU HAVEN'T SEEN!
