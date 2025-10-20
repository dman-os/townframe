# > *townframe*

Experimental.

Repo guide:

- `./src/daybook_compose/`: Compose multplatform app for daybook.

  - Confirmed to run on desktop and android.

- `./src/daybook_ffi/`: Rust core for daybook app.

  - Accessed in Kotlin through [uniffi](https://lib.rs/uniffi).

    - Use ghjk task `gen-ffi-dayb` to re-generate the bindings and build the library.

  - Uses automerge (throug [`samod`](https://lib.rs/samod)) and SQLite (through [`sqlx`](https://lib.rs/sqlx)) for storage.

- `./src/daybook_core/`: Shared code for daybook app and servers.

- `./src/daybook_sync/`: Automerge sync server for daybook.

- `./src/btress_api/`: Supporting WASI API for all apps.

- `./src/btress_http/`: Http wrapper for `btress_api` that runs on wasmcloud.

- `./src/tests_http/`: E2e tests for the APIs through http.

- `./src/api_utils_rs/`: Utilities for writing WASI APIs.

- `./src/utils_rs/`: General purpose utilities.

- `./src/macros/`: Proc-macro utilities.

- `./src/xtask/`: Genereal purpose scripts.

  - Includes the `cargo x gen` command used to do codegen.

    - Is source of truth for the interfaces for the WASI apis and handles all the boilerplate.

- `./src/granary_web/`: Web app for granary (haitus).

- `./ghjk.ts`: [`ghjk`](https://github.com/metatypedev/ghjk) file.

  - Contans a lot of necessary scripts accessible through `ghjk x`.

  - Provisions a bunch of development tools.

- `./flake.nix`: Nix flake with:

  - Provisions a bunch of development toolchains and libraries.

- `./tools/compose.yml`: docker compose file for supporting services.

  - `profiles` are used to group services together and operate on the groups.

  - Ghjk tasks like `compose-up` and `compose-logs` take profile names.
