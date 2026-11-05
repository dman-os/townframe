//! The `ident` interface over `unicode-ident`, the same tables the `pierre-wasmtime` replay's
//! host functions use, so the wrapper and the direct path classify code points identically.
wit_bindgen::generate!({ path: "../wit", world: "ident-provider" });

struct Ident;

impl exports::townframe::highlights::ident::Guest for Ident {
    fn is_id_start(c: char) -> bool {
        unicode_ident::is_xid_start(c)
    }
    fn is_id_continue(c: char) -> bool {
        unicode_ident::is_xid_continue(c)
    }
}

export!(Ident);
