# Local portability patch

Source: published `sledgehammer_bindgen_macro` 0.6.5 (MIT), repository
https://github.com/demonthos/sledgehammer_bindgen/ . The packaged Cargo.toml,
README, and source are preserved here; this directory is excluded from workspace membership.

Two bounded source changes:

1. Gate the generated `flush` method off on Wasm when the macro's `web` feature is disabled.
   Its JS channel field and initialization already use that feature, but the method body
   previously referenced `self.js_channel` and `wasm_bindgen` on all Wasm platforms. Binary-only
   Dioxus component guests consequently failed to compile. Web-enabled flush is unchanged.
2. Zero-initialize each packed opcode word. The original `Vec::set_len` exposed uninitialized
   bytes when an end-of-message opcode left unused slots. The cross-address-space probe observed
   differing native/Wasm bytes in precisely that padding. `Vec::resize` fixes the uninitialized
   read and prevents allocator contents leaking into serialized padding. Opcodes and layout are
   unchanged; only unused slots become deterministic zeros.

The Dayframe guest component tests build the real Dioxus encoder for non-web Wasm and exercise
its byte stream across the component ABI. They are the consumer regression for this patch.
