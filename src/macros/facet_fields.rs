//! The field-level checks the `facet` macro runs: what a field may state, and the bare number that
//! must state itself.

use quote::ToTokens;
use syn::Type;

/// A facet's bare `f32`/`f64` field must state its spelling, because a float position's JSON is
/// either the convention's decimal string — which is what `facet::Float32`/`Float64` declare — or a
/// plain number, and it is refused rather than assumed. The statement is the field's schema: a
/// `serde` with that spells the field's own JSON, or the opt-out's keyword marker.
pub fn check_field_facets(fields: &syn::Fields) -> syn::Result<()> {
    for field in fields.iter() {
        if field_states(field) {
            continue;
        }
        if float_type(&field.ty, 0) {
            return Err(syn::Error::new_spanned(
                field,
                "a bare number field must state its spelling: use `facet::Float64` or \
                 `facet::Float32` for the convention's decimal string, or mark this field \
                 `#[schemars(extend(\"x-automerge\" = \"number\"))]` if it is deliberately a \
                 plain json number that a consumer reads as one",
            ));
        }
        if byte_type(&field.ty, 0) {
            return Err(syn::Error::new_spanned(
                field,
                "a byte-array field must state its spelling: use `facet::Bytes` for the blob, \
                 `#[serde(with = \"…\" )]` for your own base64, or `#[schemars(extend(\"x-automerge\" \
                 = \"list\"))]` if it is deliberately an array of numbers, not bytes",
            ));
        }
    }
    Ok(())
}

/// Whether the field states its own spelling: a `serde` with of its own, or a schema it says —
/// which is also the statement the derived schema carries, since the marker survives into it.
fn field_states(field: &syn::Field) -> bool {
    field.attrs.iter().any(|attr| {
        attr.path().is_ident("serde")
            && ["with", "serialize_with", "deserialize_with"]
                .iter()
                .any(|spelled| attr.to_token_stream().to_string().contains(spelled))
    }) || schemars_states(field)
}

fn schemars_states(field: &syn::Field) -> bool {
    field
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("schemars"))
        .any(|attr| {
            let spelled = attr.to_token_stream().to_string();
            spelled.contains("with") || spelled.contains("extend")
        })
}

/// Whether a type is a bare `u8` and a byte-vec at that: a naked byte array is a CRDT list of
/// scalars, one op per byte, and the author almost certainly meant a blob.
fn byte_type(ty: &Type, depth: usize) -> bool {
    container(ty, depth, |spelled| spelled == "u8")
}

/// Whether a type is a bare `f32`/`f64`, maybe under a wrapper that keeps it a number.
fn float_type(ty: &Type, depth: usize) -> bool {
    container(ty, depth, |spelled| spelled == "f32" || spelled == "f64")
}

/// The bare number `spelled` names, maybe inside `Option`/`Vec`/`Box`/a slice or an array of them.
fn container(ty: &Type, depth: usize, spelled: impl Fn(&str) -> bool) -> bool {
    if depth > 8 {
        return false;
    }
    match ty {
        Type::Path(path) if path.qself.is_none() => {
            let last = path.path.segments.last().expect("a name is a segment");
            if path.path.segments.len() == 1 && spelled(&last.ident.to_string()) {
                return true;
            }
            if last.ident != "Option" && last.ident != "Vec" && last.ident != "Box" {
                return false;
            }
            match &last.arguments {
                syn::PathArguments::AngleBracketed(arguments) => match arguments.args.first() {
                    Some(syn::GenericArgument::Type(inner)) => container(inner, depth + 1, spelled),
                    _ => false,
                },
                _ => false,
            }
        }
        Type::Slice(items) => container(&items.elem, depth + 1, spelled),
        Type::Array(items) => container(&items.elem, depth + 1, spelled),
        _ => false,
    }
}
