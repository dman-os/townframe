//! The field-level checks the `facet` macro runs: what a field may state, and the bare number that
//! must state itself.

use syn::Token;
use syn::Type;

/// The storage annotation keyword, spelled here because this crate does not depend on the core that
/// owns it: a field's marker is read at expansion time, where the core is not in scope.
const STORAGE_KEYWORD: &str = "x-automerge";

/// A facet's bare `f32`/`f64` or byte-array field must state its spelling. A field marker covers
/// only that field, or the same type under `Option`/`Box`. A bare number or byte array nested in a
/// map, a tuple, or another container — `Vec<Vec<u8>>` included — is refused: the marker on the
/// outer field does not reach it.
pub fn check_field_facets(fields: &syn::Fields) -> syn::Result<()> {
    for field in fields.iter() {
        let Some((kind, coverable)) = classify(&field.ty, 0, true) else {
            continue;
        };
        if coverable && field_states(field) {
            continue;
        }
        let message = if coverable {
            match kind {
                Bare::Float => {
                    "a bare number field must state its spelling: use `facet::Float64` or \
                     `facet::Float32` for the convention's decimal string, or mark this field \
                     `#[schemars(extend(\"x-automerge\" = \"number\"))]` if it is deliberately a \
                     plain json number that a consumer reads as one"
                }
                Bare::Bytes => {
                    "a byte-array field must state its spelling: use `facet::Bytes` for the blob, \
                     `#[serde(with = \"…\" )]` for your own base64, or `#[schemars(extend(\"x-automerge\" \
                     = \"list\"))]` if it is deliberately an array of numbers, not bytes"
                }
            }
        } else {
            "a bare number or byte array nested in a map, a tuple, or another container is not \
             reached by a marker on the outer field, which describes one position and not the ones \
             inside it: use `facet::Float64`/`facet::Bytes` at the nested position, or state the \
             spelling on the type that holds it"
        };
        return Err(syn::Error::new_spanned(field, message));
    }
    Ok(())
}

enum Bare {
    Float,
    Bytes,
}

/// The bare number or byte array a type holds, and whether the field's own marker reaches it.
///
/// `coverable` is the type itself, or the same type under an `Option`/`Box` — the one container that
/// leaves the type at the position the marker's schema is emitted over. A sequence, a map and its
/// key, and a tuple put the bare type somewhere else, so nothing a field says about itself describes
/// them.
fn classify(ty: &Type, depth: usize, coverable: bool) -> Option<(Bare, bool)> {
    if depth > 8 {
        return None;
    }
    if let Some(kind) = leaf(ty) {
        return Some((kind, coverable));
    }
    match ty {
        Type::Path(path) if path.qself.is_none() => {
            let last = path.path.segments.last()?;
            let syn::PathArguments::AngleBracketed(arguments) = &last.arguments else {
                return None;
            };
            let nested = matches!(last.ident.to_string().as_str(), "Option" | "Box");
            arguments
                .args
                .iter()
                .filter_map(|argument| match argument {
                    syn::GenericArgument::Type(ty) => Some(ty),
                    _ => None,
                })
                // A map's value and a tuple's members are both positions the marker does not describe,
                // and the first bare type found is the one worth reporting.
                .find_map(|inner| classify(inner, depth + 1, coverable && nested))
        }
        Type::Slice(items) => classify(&items.elem, depth + 1, false),
        Type::Array(items) => classify(&items.elem, depth + 1, false),
        Type::Tuple(tuple) => tuple
            .elems
            .iter()
            .find_map(|element| classify(element, depth + 1, false)),
        _ => None,
    }
}

/// The bare number or byte vector this type *is*, rather than one it holds.
fn leaf(ty: &Type) -> Option<Bare> {
    match ty {
        Type::Path(path) if path.qself.is_none() => {
            let last = path.path.segments.last()?;
            if last.arguments.is_empty() {
                // The last segment is the primitive's name whatever path spells it: `f64` and
                // `core::primitive::f64` are one type, and only the shorter spelling was caught.
                return match last.ident.to_string().as_str() {
                    "f32" | "f64" => Some(Bare::Float),
                    _ => None,
                };
            }
            (last.ident == "Vec" && first_type(last).is_some_and(is_u8)).then_some(Bare::Bytes)
        }
        Type::Slice(items) if is_u8(&items.elem) => Some(Bare::Bytes),
        Type::Array(items) if is_u8(&items.elem) => Some(Bare::Bytes),
        _ => None,
    }
}

fn is_u8(ty: &Type) -> bool {
    match ty {
        Type::Path(path) if path.qself.is_none() => path
            .path
            .segments
            .last()
            .is_some_and(|last| last.ident == "u8" && last.arguments.is_empty()),
        _ => false,
    }
}

fn first_type(segment: &syn::PathSegment) -> Option<&Type> {
    match &segment.arguments {
        syn::PathArguments::AngleBracketed(arguments) => {
            arguments.args.iter().find_map(|argument| match argument {
                syn::GenericArgument::Type(ty) => Some(ty),
                _ => None,
            })
        }
        _ => None,
    }
}

/// Whether the field states its own spelling: a `serde` conversion of its own, or the storage
/// keyword in the schema it says.
///
/// The marker has to name the storage keyword. An unrelated extension — `extend("example" = "…")` —
/// says something about the schema and nothing about how the value is stored, so it does not excuse a
/// bare number: reading any `extend` as a statement would let a misspelled keyword compile as if it
/// were one, and the plain number would reach a peer's JSON document with nothing to notice it.
fn field_states(field: &syn::Field) -> bool {
    field.attrs.iter().any(|attr| {
        if attr.path().is_ident("serde") {
            states(attr, &["with", "serialize_with", "deserialize_with"])
        } else {
            attr.path().is_ident("schemars") && states(attr, &["with", "schema_with"])
        }
    })
}

/// Whether one attribute states a key in `names`, or extends the storage keyword.
fn states(attr: &syn::Attribute, names: &[&str]) -> bool {
    let mut stated = false;
    // A key this does not read is still consumed, so an unexpected one does not stop the scan:
    // refusing unexpected keys is the item macro's job, and a field's marker is what matters here.
    // A key that does not parse is likewise that macro's error to report, not this scan's.
    let _scanned = attr.parse_nested_meta(|meta| {
        if let Some(ident) = meta.path.get_ident()
            && names.iter().any(|name| *ident == **name)
        {
            stated = true;
        }
        if meta.path.is_ident("extend") {
            let content;
            syn::parenthesized!(content in meta.input);
            let keyword = content.parse::<syn::LitStr>()?;
            content.parse::<Token![=]>()?;
            let _spelling = content.parse::<syn::LitStr>()?;
            stated |= keyword.value() == STORAGE_KEYWORD;
            return Ok(());
        }
        if meta.input.peek(Token![=]) {
            meta.value()?.parse::<syn::Expr>()?;
        } else if meta.input.peek(syn::token::Paren) {
            let content;
            syn::parenthesized!(content in meta.input);
            content.parse::<proc_macro2::TokenStream>()?;
        }
        Ok(())
    });
    stated
}

#[cfg(test)]
mod tests {
    use super::check_field_facets;

    /// One field, scanned the way the item macro scans a struct's.
    fn check(declaration: &str) -> Result<(), String> {
        let item: syn::ItemStruct =
            syn::parse_str(&format!("struct Test {{ {declaration} }}")).expect("parses");
        check_field_facets(&item.fields).map_err(|error| error.to_string())
    }

    fn refused(declaration: &str, expected: &str) {
        let error = check(declaration).expect_err("refused");
        assert!(error.contains(expected), "{error}");
    }

    fn accepted(declaration: &str) {
        check(declaration).unwrap_or_else(|error| panic!("{declaration}: {error}"));
    }

    #[test]
    fn a_bare_number_or_byte_array_must_state_its_spelling() {
        refused("value: f64,", "must state its spelling");
        refused("value: Option<f32>,", "must state its spelling");
        refused("value: Box<f64>,", "must state its spelling");
        refused("value: Vec<u8>,", "must state its spelling");
        accepted("#[schemars(extend(\"x-automerge\" = \"number\"))] value: f64,");
        accepted("#[schemars(extend(\"x-automerge\" = \"list\"))] value: Vec<u8>,");
        accepted("#[serde(with = \"module\")] value: Vec<u8>,");
        accepted("#[schemars(schema_with = \"function\")] value: f64,");
        accepted("value: Float64,");
        accepted("value: Bytes,");
    }

    /// An extension that is not the storage keyword says something about the schema and nothing
    /// about storage: reading any `extend` as a statement would let a misspelled keyword compile.
    #[test]
    fn an_unrelated_extension_is_not_a_statement() {
        refused(
            "#[schemars(extend(\"example\" = \"1.5\"))] value: f64,",
            "must state its spelling",
        );
        // The same attribute list, with the storage keyword present, is a statement.
        accepted(
            "#[schemars(extend(\"example\" = \"1.5\"), extend(\"x-automerge\" = \"number\"))] value: f64,",
        );
        // The scan checks that a statement exists, not that it is the right one: a `number` marker
        // on a byte array states a spelling, and a marker disagreeing with the JSON shape it
        // annotates is refused downstream, where the schema is in hand.
        accepted("#[schemars(extend(\"x-automerge\" = \"number\"))] value: Vec<u8>,");
    }

    /// A marker describes one position. `Option`/`Box` leave the type at that position; a map, a
    /// tuple, a slice and a nested sequence put it somewhere else the marker does not reach.
    #[test]
    fn a_marker_does_not_reach_into_another_container() {
        refused("value: HashMap<String, f64>,", "not reached by a marker");
        refused("value: Vec<Vec<u8>>,", "not reached by a marker");
        refused("value: (u8, f32),", "not reached by a marker");
        refused(
            "value: HashMap<String, Vec<u8>>,",
            "not reached by a marker",
        );
        refused(
            "#[schemars(extend(\"x-automerge\" = \"number\"))] value: HashMap<String, f64>,",
            "not reached by a marker",
        );
        // Option and Box are the containers that keep the type at the marked position.
        accepted("#[schemars(extend(\"x-automerge\" = \"number\"))] value: Option<f64>,");
        accepted("#[schemars(extend(\"x-automerge\" = \"list\"))] value: Box<Vec<u8>>,");
    }

    /// The primitive is the last path segment: `f64` and `core::primitive::f64` are one type, and
    /// `Vec<u8>` is bytes however the `Vec` is spelled.
    #[test]
    fn a_qualified_spelling_is_the_same_type() {
        refused("value: core::primitive::f64,", "must state its spelling");
        refused("value: std::vec::Vec<u8>,", "must state its spelling");
        refused("value: [u8; 4],", "must state its spelling");
        refused("value: [u8],", "must state its spelling");
        accepted("value: std::collections::HashMap<String, String>,");
    }
}
