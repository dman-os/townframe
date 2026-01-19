# CONTRIBUTING

- Avoid adding dependencies if possible
- Always audit LLM code in an editor with an IDE present.
  - Assume you'll need to slightly improve every result.

## Repo guide

- `./src/daybook_core/`: Rust core for daybook.

  - `./src/daybook_types/`: Core types for daybook.

  - `./src/daybook_cli/`: CLI app for daybook.

  - `./src/daybook_compose/`: Compose multplatform app for daybook.

    - Confirmed to run on desktop and android.

  - Uses automerge (through [`samod`](https://lib.rs/samod)) and SQLite (through [`sqlx`](https://lib.rs/sqlx)) for storage.

  - `./src/daybook_ffi/`: [uniffi](https://lib.rs/uniffi) based bindigns for kotlin.

      - Use ghjk task `gen-ffi-dayb` to re-generate the bindings and build the library.

  - `./src/daybook_wflows/`: wflows for daybook.

- `./src/btress_api/`: Supporting WASI API for all apps.

  - `./src/btress_http/`: Http wrapper for `btress_api` that runs on wasmcloud.

- `./src/tests_http/`: E2e tests for the APIs through http.

- `./src/utils_rs/`: General purpose utilities.

  - `./src/api_utils_rs/`: Utilities for writing WASI APIs.

- `./src/macros/`: Proc-macro utilities.

- `./src/infra/`: Terraform IaC for deployment.

- `./src/xtask/`: General purpose scripts.

  - Includes the `cargo x gen` command used to do codegen.

    - Is source of truth for the interfaces for the WASI apis and handles all the boilerplate.

- `./src/granary_web/`: Web app for granary (haitus).

- `./src/wflow/`: the top level crate for wflow.

  - `./src/wflow_core/`: the core types and logic.
 
  - `./src/wflow_tokio/`: tokio implementation for the wflow engine.

  - `./src/wflow_webui/` (hiatus): web ui for wflow.

  - `./src/wflow_sdk/`: sdk for writing wflows.

  - `./src/wflow_ingress_http/`: http api for wflows.

  - `./src/test_wflows/`: wflows used for tests in `wflow`.


- `./ghjk.ts`: [`ghjk`](https://github.com/metatypedev/ghjk) file.

  - Contans a lot of necessary scripts accessible through `ghjk x`.

  - Provisions a bunch of development tools.

- `./flake.nix`: Nix flake with development environments.

  - Provides four specialized shells for different use cases:
  
    - `dev` (default): Full local development environment
      - All Rust targets (wasm32, Android, native)
      - Android Studio with SDK/NDK
      - Desktop UI libraries (for dioxus/Compose desktop)
      - All interactive tools (rogcat, opentofu, terragrunt, tokio-console, infisical)
      - Use: `nix develop` or `nix develop .#dev`
    
    - `ci-rust`: Minimal CI environment for Rust linting and CLI builds
      - Rust toolchain with wasm32 + native Linux targets
      - Basic build tools (pkg-config, openssl, protobuf)
      - Rust linting tools (cargo-udeps, prek)
      - No Android, no desktop UI libraries, no Java
      - Use: `nix develop .#ci-rust`
    
    - `ci-android`: CI environment for Android builds and Kotlin linting
      - Rust toolchain with Android targets (all 4 ABIs) + wasm32
      - Android SDK/NDK (without Android Studio)
      - OpenJDK 21 (for Gradle/Kotlin)
      - CMake (for wasmcloud builds)
      - Android environment variables configured
      - No desktop UI libraries
      - Use: `nix develop .#ci-android`
    
    - `ci-desktop`: CI environment for Compose desktop builds and Kotlin linting
      - Rust toolchain with wasm32 + native Linux targets
      - OpenJDK 21 (for Gradle/Kotlin)
      - Desktop UI libraries (wayland, gtk3, webkitgtk, etc.)
      - Basic build tools (pkg-config, openssl, protobuf)
      - No Android SDK/NDK, no Android toolchain
      - Use: `nix develop .#ci-desktop`
  
  - For CI workflows, use the specialized shells (`ci-rust`, `ci-android`, or `ci-desktop`) for faster builds.

- `./tools/compose.yml`: docker compose file for supporting services.

  - `profiles` are used to group services together and operate on the groups.

  - Ghjk tasks like `compose-up` and `compose-logs` take profile names.

## Style guide

- Avoid crates with a `src/` directory. 
  - The source files and `Cargo.toml` should reside in the same root.
- Avoid deeply nested directory hierarchies.
- Are you importing that type in too many files? Consider putting it in the `interlude` module of the crate.
