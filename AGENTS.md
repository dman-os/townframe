./README.md
./CONTRIBUTING.md

> [!INFO]
>
> Don't be lazy with solutions, you're better than that.

## Breaking changes

- If the requested change requires changing interfaces, break and change the interfaces instead of trying to shim around this.
- These kinds of shims break abstraction boundaries and make it confusing to read at a later point.
- If indeed the change affets a large number of sites, either ask the user for directions or always assume that changing them is required.

## Error handling

- Most errors are programming errors and should crash the program.
  - These include cases like:
    - Unhandled branches which is why `todo!()` or `unimplemented!()` exists.
    - Unexpected JSON shapes which unless we're working with unvalidated external inputs, the expected shape is usually apparent and it's fruitless to pollute the code with a million checks and attempt to recover the program. It should crash, it's an invariant!.
- Finding yourself needing to catch a million error types deep in the stack? That's code smell. Re-asses.
- Errors that should be handled include those downstream of:
  - Network errors
  - User input validation
- A well constructed program should rarely permit errors by using type state elimination of error paths.
  - External errors ought to be handled at the edges of the system.
  - Invariants should be assumed to hold and consumers of these invariants should trust them and shouldn't guard against them.
  - Essentially, we want our programs to be correct in construction and not taped together till they work.
- Don't swallow errors with empty catch statements or catch-and-log unless explcitly told to do so.
  - This is especially critical in Kotlin or UI code. 
    - If an error occurs that can't be handled, it should crash the program or show a toast if it's not critical. 
  - It's very hard to imagine cases where this is not true.
  - Unless a tokio task is expected to fail and has a channel for reporting failures, the last handler should call `.unwrap` on any unexpected errors at the end.
    - Think a pattern like `let fut = async {/*body*/ eyre::Ok(()) }; tokio::spawn(async { fut.await.unwrap() })`
    - We have a panic handler that will bring down the whole process when tasks panic.
    - This ensures that we don't hide task failures until the join handler is seen at shutdown and so and so.
  - Another case where this is critical, don't ignore channel send errors in Rust.
    - Unless channel closure is a signal itself to the task to close, shutdown order should ascertain channels are always open.
    - Shutdown order is to be reverse of construction of order which means the entity that constructs the channel will always close after it's child.
    - Concealing channel errors hides lifecycle and liveness issues that are hard to diagnose in an actor-based program like this.
    - The only good exception is broadcast channels that support 0..N listeners. A broadcast channel can be re-opened and thus failure to send is not an invariant break.
- Never add `skip` to tests unless asked to, they obscure broken tests for reviewers.

## Checks

> [!INFO]
>
> Don't be lazy with solutions, you really are better than that.

- To type check the ./src/daybook_compose multiplatform app, use `./x/check-dayb.ts`.
- Prefer `cargo clippy --all-targets --all-features -p myCrate` over `cargo check`.
- When working with rust, in addition to `cargo clippy`, small tests can be used to validate ideas.

## Comments

> [!INFO]
>
> Don't be lazy with solutions, you can do more.

- Do not adress TODOs or FIXMEs unless told to do so, usually the reason they're there is a broader issue that might not be apparent in the local scope that you encountered them.
- Prefer to preserve comments unless they are progress comments written by an agent. 

## Experimentation and Debugging

> [!INFO]
>
> Don't be lazy with solutions, that's not becoming of you.

- Use RUST_LOG_TEST env var for controlling log levels during testing.
- `printf` and experimental debugging is always quicker than trying to come up with premature hypothesis.
  - Need to prove/demo a hypothesis, throw together a quick commaind in ./src/xtask/ cli.

## VCS

> [!INFO]
>
> Don't be lazy with solutions, why would you be? The world is your oyster. You can build and change whatever you need to, one small step at a time.

- CRITICAL: Never ever use git commands. Never! 
  - In most machine's you're working on, `jj` is being used and the safest looking git commands could mess up the `jj` state destroying work.
  - Even if on other machines, git mutation commands are too destructive and unsafe.

## User error

- If the users request seems like it comes from a place of misunderstanding, push back!

## Style guide

> [!INFO]
>
> You're trusted not to be lazy with solutions.

- Top level symbol proliferation makes it more confusing and harder to read compared to any otehr code quality sin.
  - When working with an external library, imagine if it had a lot of small public classes and functions? Even if you're the AWS SDK, no one wants to learn what each one does.
  - Examples include like a billion instances `doSomethingVariantXXX` or a lot of small but related functions that are only used at a single place.
  - It almost always implies that there's a terrible architecutre at play.

## Tool calls

> [!INFO]
>
> You're begged not to be lazy with solutions.

- If you're not able to cleanly read a provided web link through tool calls, pause and ask for a copy/paste of the contents. NEVER ASSUME THE CONTENTS OF A LINK YOU HAVEN'T SEEN!

## Cheating

Avoid cheating through hacks that violate common sensibilities just to get a task done.

- Code that tires to get tests green by writing shallow or buggy fixes.
- Code that reads the whole database in a memory HashSet to avoid writing the right SQL.

THIS IS A NO CHEAT REPO!

## Test code

Test code is often less consistently audited than production code.
So when working on tests, take a gardening hand to it and proactively improve them.
See a useless or too shallow a test?
Flag it for removal.
Are tests repeating too much setup code?
Consider using TDD or common cleanup code.

Tools like snapshot tests, TDD and the macros for TDD found in the repo can help in the longevity of a test's usefulness.

- Avoid Cargo integration tests (`crate/tests/*.rs`) unless explicitly requested.
- Prefer in-crate test modules instead:
  - unit tests inline in the relevant module
  - cross-module end-to-end style tests in a crate-local `e2e` module (for example `src/my_crate/e2e.rs` with submodules in `src/my_crate/e2e/`).

## Pushback 

If you're told to do something difficult or stupid, push back.
Don't hack and boil the ocean to a task completion, you'll be asked to do it again if you do it wrong.
Better to have good alignment with the operator as opposed to spending a million iterations on the same thing.
