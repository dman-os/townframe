//! Attribute macros for the workspace.

mod facet_fields;

use proc_macro::TokenStream;
use quote::{ToTokens, quote};
use syn::punctuated::Punctuated;
use syn::{Attribute, Expr, ExprLit, Item, Lit, Meta, Path, Token};

/// The module every emitted item resolves through, so moving it is one edit.
fn facet_path() -> Path {
    syn::parse_quote!(::big_cabinet_core::facet)
}

/// Declares a facet type.
///
/// Emits `FacetShape`, and `FacetMember` when the type states an id. It does not generate the
/// schema: `schemars` derives that from the same item, and the two are checked against each other
/// where the type is published.
///
/// A struct is **open** when it has exactly one `#[serde(flatten)]` overflow field of type `Extra`,
/// which is what preserves the members the type does not know about, and **closed** when it says
/// `#[facet(closed)]` together with `#[serde(deny_unknown_fields)]`. Anything else is refused:
/// `serde` drops a member it has nowhere to put, so the type and the schema would disagree.
///
/// A union is **adjacently tagged** — `#[serde(tag = "$type", content = "value")]`. It is **open**
/// when exactly one variant is marked `#[facet(other)]`, and then the macro writes the wire form and
/// the schema, because a fallback that carries a payload is not expressible in serde attributes. A
/// union that says `#[facet(closed)]` has no fallback, keeps its own derives, and is the plain serde
/// case.
///
/// A vocabulary of *strings* rather than of named types is a different shape, so it is declared
/// differently: `#[facet(open_string)]`, with `#[facet(other)]` on the one variant that carries a
/// `String`. Each known value's spelling is its variant's name in lowerCamel — `MyValue` is
/// `"myValue"` — or an explicit `#[serde(rename = "…")]`. The macro writes the wire form and the
/// schema, and a string nobody declared survives as it was written. A *closed* vocabulary of strings
/// needs none of this — derive serde and `schemars` and drop the macro.
///
/// A union member is a declared type whose id is its tag, so a variant names a payload and nothing
/// else. `#[facet(id = "…")]` states an id exactly; `#[facet(id_prefix = "org.example.daybook.")]`
/// derives it from the type name, producing the same string `define_enum_and_tag!` builds.
///
/// ```ignore
/// #[facet(id_prefix = "org.example.daybook.")]
/// #[derive(Serialize, Deserialize, JsonSchema)]
/// pub struct Note {
///     pub content: String,
///     #[serde(flatten)]
///     pub extra: Extra,
/// }
/// ```
#[proc_macro_attribute]
pub fn facet(args: TokenStream, item: TokenStream) -> TokenStream {
    match expand(args.into(), item.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[derive(Default)]
struct Args {
    closed: bool,
    open_string: bool,
    tagging: Option<String>,
    id: Option<String>,
    prefix: Option<String>,
}

impl Args {
    fn parse(tokens: proc_macro2::TokenStream) -> syn::Result<Self> {
        let metas: Punctuated<Meta, Token![,]> = if tokens.is_empty() {
            Punctuated::new()
        } else {
            syn::parse::Parser::parse2(Punctuated::<Meta, Token![,]>::parse_terminated, tokens)?
        };
        let mut args = Args::default();
        for meta in metas {
            if meta.path().is_ident("closed") {
                args.closed = true;
            } else if meta.path().is_ident("open_string") {
                args.open_string = true;
            } else if meta.path().is_ident("tagging") {
                let value = meta_string(&meta)?;
                if value != "adjacent" && value != "external" {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "`tagging` is \"adjacent\" or \"external\"",
                    ));
                }
                args.tagging = Some(value);
            } else if meta.path().is_ident("id") {
                if args.id.is_some() || args.prefix.is_some() {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "a type states either `id` or `id_prefix`, once",
                    ));
                }
                args.id = Some(meta_string(&meta)?);
            } else if meta.path().is_ident("id_prefix") {
                if args.id.is_some() || args.prefix.is_some() {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "a type states either `id` or `id_prefix`, once",
                    ));
                }
                let prefix = meta_string(&meta)?;
                if !prefix.ends_with('.') {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "`id_prefix` ends with the separator, e.g. \"org.example.daybook.\"",
                    ));
                }
                args.prefix = Some(prefix);
            } else {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "unknown facet argument: expected `closed`, `open_string`, `tagging = \
                     \"…\"`, `id = \"…\"` or `id_prefix = \"…\"`",
                ));
            }
        }
        Ok(args)
    }
}

fn meta_string(meta: &Meta) -> syn::Result<String> {
    let Meta::NameValue(named) = meta else {
        return Err(syn::Error::new_spanned(meta, "expected a string literal"));
    };
    let Expr::Lit(ExprLit {
        lit: Lit::Str(lit), ..
    }) = &named.value
    else {
        return Err(syn::Error::new_spanned(meta, "expected a string literal"));
    };
    Ok(lit.value())
}

/// The `serde` attributes this macro reads, and the keys it does not recognize.
#[derive(Default)]
struct Serde {
    flatten: usize,
    deny: bool,
    tag: Option<String>,
    content: Option<String>,
    rename: Option<String>,
    rename_all: Option<String>,
    unexpected: Vec<String>,
}

impl Serde {
    fn refuse_unexpected<T: ToTokens>(&self, item: &T, read: &[&str]) -> syn::Result<()> {
        let Some(unexpected) = self.unexpected.first() else {
            return Ok(());
        };
        let read = if read.is_empty() {
            "this type's wire form is written by the macro".to_owned()
        } else {
            format!("the keys read here are {}", read.join(", "))
        };
        Err(syn::Error::new_spanned(
            item,
            format!("`#[serde({unexpected})]` is not read: {read}"),
        ))
    }
}

fn scan_serde(attrs: &[Attribute]) -> syn::Result<Serde> {
    let mut out = Serde::default();
    for attr in attrs {
        if !attr.path().is_ident("serde") || !matches!(attr.meta, Meta::List(_)) {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("flatten") {
                out.flatten += 1;
            } else if meta.path.is_ident("deny_unknown_fields") {
                out.deny = true;
            } else if meta.path.is_ident("tag") {
                out.tag = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("content") {
                out.content = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("rename") {
                out.rename = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("rename_all") {
                out.rename_all = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if let Some(ident) = meta.path.get_ident() {
                out.unexpected.push(ident.to_string());
            }
            // Consume a value or a nested list, so parsing continues past a key we do not read.
            if meta.input.peek(Token![=]) {
                meta.value()?.parse::<Expr>()?;
            } else if meta.input.peek(syn::token::Paren) {
                let content;
                syn::parenthesized!(content in meta.input);
                content.parse::<proc_macro2::TokenStream>()?;
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// The last segment of every path in every `#[derive(...)]` on the item.
fn derived_traits(attrs: &[Attribute]) -> syn::Result<Vec<String>> {
    let mut out = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("derive") {
            continue;
        }
        for path in attr.parse_args_with(Punctuated::<Path, Token![,]>::parse_terminated)? {
            if let Some(segment) = path.segments.last() {
                out.push(segment.ident.to_string());
            }
        }
    }
    Ok(out)
}

fn no_generics(name: &syn::Ident, generics: &syn::Generics) -> syn::Result<()> {
    if generics.params.is_empty() {
        return Ok(());
    }
    Err(syn::Error::new_spanned(
        name,
        "a generic facet type is not supported: the macro emits a plain `FacetShape` impl, and the \
         schema a generic type emits depends on its instantiation",
    ))
}

/// `OcrResult` becomes `ocrResult`, the rule `define_enum_and_tag!` uses for the member key of a tag
/// that has no explicit literal.
fn lower_camel(name: &str) -> String {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn member_impl(name: &syn::Ident, args: &Args) -> proc_macro2::TokenStream {
    let facet = facet_path();
    let id = match (&args.id, &args.prefix) {
        (Some(id), _) => id.clone(),
        (None, Some(prefix)) => format!("{prefix}{}", lower_camel(&name.to_string())),
        (None, None) => return proc_macro2::TokenStream::new(),
    };
    quote! {
        impl #facet::FacetMember for #name {
            const ID: &'static str = #id;
        }
    }
}

fn expand(
    args: proc_macro2::TokenStream,
    item: proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let args = Args::parse(args)?;
    let mut item: Item = syn::parse2(item)?;
    let facet = facet_path();
    let (name, shape) = match &mut item {
        Item::Struct(item) => struct_shape(item, &args, &facet)?,
        Item::Enum(item) if args.open_string => string_enum_shape(item, &args, &facet)?,
        Item::Enum(item) => enum_shape(item, &args, &facet)?,
        other => {
            let what = if args.open_string {
                "#[facet(open_string)] applies to an enum, and its variants are unit variants plus \
                 one `#[facet(other)]` carrying the unrecognized string"
            } else {
                "#[facet] applies to a struct or an enum"
            };
            return Err(syn::Error::new_spanned(other.to_token_stream(), what));
        }
    };
    let member = member_impl(&name, &args);
    Ok(quote! {
        #item
        #member
        #shape
    })
}

fn struct_shape(
    item: &mut syn::ItemStruct,
    args: &Args,
    facet: &Path,
) -> syn::Result<(syn::Ident, proc_macro2::TokenStream)> {
    let name = item.ident.clone();
    no_generics(&name, &item.generics)?;
    facet_fields::check_field_facets(&item.fields)?;
    let serde = scan_serde(&item.attrs)?;
    let mut extras = 0usize;
    for field in item.fields.iter() {
        extras += scan_serde(&field.attrs)?.flatten;
    }
    let openness = if args.closed {
        if extras > 0 {
            return Err(syn::Error::new_spanned(
                &name,
                "a closed object declares no overflow field, but a field is marked \
                 #[serde(flatten)]",
            ));
        }
        if !serde.deny {
            return Err(syn::Error::new_spanned(
                &name,
                "a closed object must reject unknown members and say so in the schema: add \
                 #[serde(deny_unknown_fields)], which is what makes the schema carry \
                 additionalProperties: false",
            ));
        }
        quote!(#facet::Openness::Closed)
    } else {
        if extras == 0 {
            return Err(syn::Error::new_spanned(
                &name,
                "state the openness of this object: add a `#[serde(flatten)] extra: Extra` field to \
                 preserve the members it does not know about, or declare `#[facet(closed)]` \
                 together with `#[serde(deny_unknown_fields)]`",
            ));
        }
        if extras > 1 {
            return Err(syn::Error::new_spanned(
                &name,
                "an open object has exactly one overflow field, but more than one field is marked \
                 #[serde(flatten)]",
            ));
        }
        if serde.deny {
            return Err(syn::Error::new_spanned(
                &name,
                "an open object cannot also be #[serde(deny_unknown_fields)]: deny_unknown_fields \
                 wins over the overflow field, so the type would reject the members it claims to \
                 preserve",
            ));
        }
        quote!(#facet::Openness::Open)
    };
    Ok((
        name.clone(),
        quote! {
            impl #facet::FacetShape for #name {
                const SHAPE: #facet::Shape = #facet::Shape::Object { openness: #openness };
            }
        },
    ))
}

/// A declared member of a union: the variant that carries it, its payload, and the tag it states.
struct MemberVariant {
    ident: syn::Ident,
    payload: syn::Type,
    rename: Option<String>,
}

fn payload_type(variant: &syn::Variant) -> syn::Result<syn::Type> {
    if variant.fields.len() != 1 {
        return Err(syn::Error::new_spanned(
            variant,
            "a union member is one payload type that its tag can name: give the variant a payload \
             and declare an empty struct for a marker member",
        ));
    }
    let field = variant.fields.iter().next().expect("one field");
    Ok(field.ty.clone())
}

fn enum_shape(
    item: &mut syn::ItemEnum,
    args: &Args,
    facet: &Path,
) -> syn::Result<(syn::Ident, proc_macro2::TokenStream)> {
    let name = item.ident.clone();
    no_generics(&name, &item.generics)?;
    let serde = scan_serde(&item.attrs)?;
    serde.refuse_unexpected(&name, &["tag", "content"])?;

    let externally_tagged = args.tagging.as_deref() == Some("external");
    if !externally_tagged {
        if serde.tag.is_none() {
            return Err(syn::Error::new_spanned(
                &name,
                "a union is adjacently tagged: write `#[serde(tag = \"$type\", content = \
                 \"value\")]`. To depart deliberately, name it with `#[facet(tagging = \
                 \"external\")]` so it is recorded and flagged rather than inferred",
            ));
        }
        if serde.content.is_none() {
            return Err(syn::Error::new_spanned(
                &name,
                "a tag without a content key is internal tagging, which puts the tag inside the \
                 payload and needs every payload to be open; write `#[serde(tag = \"$type\", \
                 content = \"value\")]`",
            ));
        }
    }

    let mut members: Vec<MemberVariant> = Vec::new();
    let mut fallback: Option<(syn::Ident, syn::Type)> = None;
    for variant in item.variants.iter_mut() {
        let mut is_fallback = false;
        for attr in &variant.attrs {
            if !attr.path().is_ident("facet") {
                continue;
            }
            let Meta::List(list) = &attr.meta else {
                return Err(syn::Error::new_spanned(
                    &attr.meta,
                    "expected `#[facet(other)]`",
                ));
            };
            if list.tokens.to_string().trim() == "other" {
                is_fallback = true;
            } else {
                return Err(syn::Error::new_spanned(
                    attr,
                    "the only facet attribute a union variant takes is `#[facet(other)]`, marking \
                     the fallback variant that keeps an unrecognized member",
                ));
            }
        }
        variant.attrs.retain(|attr| !attr.path().is_ident("facet"));
        facet_fields::check_field_facets(&variant.fields)?;
        let payload = payload_type(variant)?;
        let variant_serde = scan_serde(&variant.attrs)?;
        if is_fallback {
            variant_serde.refuse_unexpected(variant, &[])?;
            if fallback.is_some() {
                return Err(syn::Error::new_spanned(
                    variant,
                    "a union has exactly one fallback variant",
                ));
            }
            fallback = Some((variant.ident.clone(), payload));
        } else {
            variant_serde.refuse_unexpected(variant, &["rename"])?;
            members.push(MemberVariant {
                ident: variant.ident.clone(),
                payload,
                rename: variant_serde.rename.clone(),
            });
        }
    }

    let open = !args.closed;
    if open && externally_tagged {
        return Err(syn::Error::new_spanned(
            &name,
            "an open union is adjacently tagged: the codec dispatches on the discriminant key, and \
             external tagging has none",
        ));
    }
    if open && fallback.is_none() {
        return Err(syn::Error::new_spanned(
            &name,
            "an open union needs exactly one fallback variant marked `#[facet(other)]`, or \
             `#[facet(closed)]` to declare that an unrecognized member is an error",
        ));
    }
    if !open && fallback.is_some() {
        return Err(syn::Error::new_spanned(
            &name,
            "a variant is marked #[facet(other)] but the union is declared closed: a closed union \
             has no fallback",
        ));
    }

    let tagging = if externally_tagged {
        quote!(#facet::Tagging::Externally)
    } else {
        let tag = serde.tag.as_deref().expect("checked above");
        let content = serde.content.as_deref().expect("checked above");
        quote!(#facet::Tagging::Adjacently { tag: #tag, content: #content })
    };
    let openness = if open {
        quote!(#facet::Openness::Open)
    } else {
        quote!(#facet::Openness::Closed)
    };
    let shape = quote! {
        impl #facet::FacetShape for #name {
            const SHAPE: #facet::Shape = #facet::Shape::Union {
                openness: #openness,
                tagging: #tagging,
            };
        }
    };

    if !open {
        // Everything here is serde's; the assert is only so a tag cannot drift from the id it names.
        let derived = derived_traits(&item.attrs)?;
        for wanted in ["Serialize", "Deserialize", "JsonSchema"] {
            if !derived.iter().any(|name| name == wanted) {
                return Err(syn::Error::new_spanned(
                    &name,
                    format!("a closed union is the plain serde case: derive {wanted} as well"),
                ));
            }
        }
        let mut asserts = Vec::new();
        for member in &members {
            let (ident, payload) = (&member.ident, &member.payload);
            let Some(rename) = &member.rename else {
                return Err(syn::Error::new_spanned(
                    ident,
                    format!(
                        "a member's tag is its id: write `#[serde(rename = \"<the member id>\")]` on \
                         `{ident}`, which the macro checks against the payload's id"
                    ),
                ));
            };
            asserts.push(quote! {
                const _: () = ::core::assert!(
                    #facet::str_eq(#rename, <#payload as #facet::FacetMember>::ID),
                    "this tag and the payload's member id disagree",
                );
            });
        }
        return Ok((name, quote! { #shape #(#asserts)* }));
    }

    // An open union is the macro's to write: a fallback that keeps a member it does not know has no
    // serde spelling, and `schemars` would emit a branch per declared variant and nothing else.
    let derived = derived_traits(&item.attrs)?;
    for replaced in ["Serialize", "Deserialize", "JsonSchema"] {
        if derived.iter().any(|name| name == replaced) {
            return Err(syn::Error::new_spanned(
                &name,
                format!(
                    "an open union's {replaced} is emitted by the macro, because a fallback that \
                     keeps an unrecognized member is not expressible in serde attributes: remove it \
                     from #[derive(...)]"
                ),
            ));
        }
    }
    let tag = serde.tag.as_deref().expect("checked above");
    let content = serde.content.as_deref().expect("checked above");
    let (fallback_ident, fallback_payload) = fallback.expect("checked above");
    // The keys have been read; leaving them would be an attribute nothing registers, and `schemars`
    // registers `serde` on whatever it derives elsewhere.
    item.attrs.retain(|attr| !attr.path().is_ident("serde"));

    let serialize_arms = members.iter().map(|member| {
        let ident = &member.ident;
        quote! {
            Self::#ident(payload) => #facet::serialize_member(serializer, #tag, #content, payload),
        }
    });
    let deserialize_members = members.iter().map(|member| {
        let (ident, payload) = (&member.ident, &member.payload);
        quote! {
            #facet::Member {
                id: <#payload as #facet::FacetMember>::ID,
                read: |member| ::serde_json::from_value::<#payload>(member).map(Self::#ident),
            },
        }
    });
    let schema_members = members.iter().map(|member| {
        let payload = &member.payload;
        quote! {
            #facet::MemberSchema {
                id: <#payload as #facet::FacetMember>::ID,
                schema: generator.subschema_for::<#payload>(),
            },
        }
    });

    Ok((
        name.clone(),
        quote! {
            #shape

            impl ::serde::Serialize for #name {
                fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                where
                    S: ::serde::Serializer,
                {
                    match self {
                        #(#serialize_arms)*
                        Self::#fallback_ident(member) => ::serde::Serialize::serialize(member, serializer),
                    }
                }
            }

            impl<'de> ::serde::Deserialize<'de> for #name {
                fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
                where
                    D: ::serde::Deserializer<'de>,
                {
                    #facet::deserialize_union(
                        deserializer,
                        #tag,
                        #content,
                        &[#(#deserialize_members)*],
                        |member| {
                            ::serde_json::from_value::<#fallback_payload>(member)
                                .map(Self::#fallback_ident)
                        },
                    )
                }
            }

            impl ::schemars::JsonSchema for #name {
                fn schema_name() -> ::std::borrow::Cow<'static, str> {
                    ::std::borrow::Cow::Borrowed(::core::stringify!(#name))
                }

                fn schema_id() -> ::std::borrow::Cow<'static, str> {
                    ::core::concat!(::core::module_path!(), "::", ::core::stringify!(#name)).into()
                }

                /// A union is a position, never a named definition, so it is always inlined.
                fn inline_schema() -> bool {
                    true
                }

                fn json_schema(generator: &mut ::schemars::SchemaGenerator) -> ::schemars::Schema {
                    #facet::union_schema(#tag, #content, #open, ::std::vec![#(#schema_members)*])
                }
            }
        },
    ))
}

/// A known spelling of a string vocabulary, on the variant whose name states it.
struct KnownVariant {
    ident: syn::Ident,
    spelling: String,
}

/// A vocabulary of strings rather than a set of named types, so the macro states the wire form and
/// the schema, and a string nothing knows is kept as it was written.
///
/// A value's spelling is its variant's name in lowerCamel — `MyValue` is `"myValue"`. The macro
/// derives that and refuses anything it cannot derive: a variant that is not PascalCase is rejected
/// rather than mangled, because a macro here states a spelling or rejects one, it never rewrites
/// what the author wrote. The only override is an explicit `#[serde(rename = "…")]`, which is a
/// statement rather than a rule.
fn string_enum_shape(
    item: &mut syn::ItemEnum,
    args: &Args,
    facet: &Path,
) -> syn::Result<(syn::Ident, proc_macro2::TokenStream)> {
    let name = item.ident.clone();
    no_generics(&name, &item.generics)?;
    if args.closed {
        return Err(syn::Error::new_spanned(
            &name,
            "a closed vocabulary needs nothing from the macro: drop `#[facet(open_string)]` and \
             derive `Serialize`, `Deserialize` and `JsonSchema` on the enum instead",
        ));
    }
    let serde = scan_serde(&item.attrs)?;
    serde.refuse_unexpected(&name, &["rename_all"])?;
    // Read, so it is consumed: nothing derives serde on this type, and an attribute nothing
    // registers is a compile error rather than a silent no-op.
    item.attrs.retain(|attr| !attr.path().is_ident("serde"));
    if let Some(case) = &serde.rename_all
        && case != "camelCase"
    {
        return Err(syn::Error::new_spanned(
            &name,
            format!(
                "a vocabulary of strings is lowerCamel, so the only `rename_all` that agrees with it \
                 is \"camelCase\", not \"{case}\": a value's spelling is derived from its variant's \
                 name and never rewritten"
            ),
        ));
    }

    let mut known: Vec<KnownVariant> = Vec::new();
    let mut fallback: Option<syn::Ident> = None;
    for variant in item.variants.iter_mut() {
        let mut is_fallback = false;
        for attr in &variant.attrs {
            if !attr.path().is_ident("facet") {
                continue;
            }
            let Meta::List(list) = &attr.meta else {
                return Err(syn::Error::new_spanned(
                    &attr.meta,
                    "expected `#[facet(other)]` on the variant that keeps an unrecognized string",
                ));
            };
            if list.tokens.to_string().trim() == "other" {
                is_fallback = true;
            } else {
                return Err(syn::Error::new_spanned(
                    attr,
                    "a value's spelling is its variant's name in lowerCamel, so there is nothing to \
                     state here: name the variant `MyValue` for \"myValue\", or state it explicitly \
                     with `#[serde(rename = \"…\")]`",
                ));
            }
        }
        variant.attrs.retain(|attr| !attr.path().is_ident("facet"));
        let variant_serde = scan_serde(&variant.attrs)?;
        variant_serde.refuse_unexpected(variant, &["rename"])?;
        variant.attrs.retain(|attr| !attr.path().is_ident("serde"));

        if is_fallback {
            if variant_serde.rename.is_some() {
                return Err(syn::Error::new_spanned(
                    variant,
                    "the fallback keeps a string nobody declared, so it states no spelling of its own",
                ));
            }
            if variant.fields.len() != 1 {
                return Err(syn::Error::new_spanned(
                    variant,
                    "the fallback carries the unrecognized string: one `String` field",
                ));
            }
            if fallback.is_some() {
                return Err(syn::Error::new_spanned(
                    variant,
                    "a vocabulary has exactly one fallback variant",
                ));
            }
            fallback = Some(variant.ident.clone());
            continue;
        }
        if !variant.fields.is_empty() {
            return Err(syn::Error::new_spanned(
                variant,
                "a known value is a spelling and nothing else: a unit variant",
            ));
        }
        let ident = variant.ident.to_string();
        let spelling = match variant_serde.rename.clone() {
            Some(stated) => stated,
            None if ident.contains('_') => {
                return Err(syn::Error::new_spanned(
                    variant,
                    format!(
                        "`{ident}` is not PascalCase, so there is no lowerCamel spelling to derive \
                         from it: rename the variant, or state the value with \
                         `#[serde(rename = \"…\")]`"
                    ),
                ));
            }
            None => lower_camel(&ident),
        };
        if let Some(other) = known.iter().find(|known| known.spelling == spelling) {
            return Err(syn::Error::new_spanned(
                variant,
                format!("`{spelling}` is already the spelling of `{}`", other.ident),
            ));
        }
        known.push(KnownVariant {
            ident: variant.ident.clone(),
            spelling,
        });
    }
    let Some(fallback) = fallback else {
        return Err(syn::Error::new_spanned(
            &name,
            "a vocabulary of strings needs exactly one fallback variant marked `#[facet(other)]`, \
             carrying a `String`: without one, a string nobody knows is an error",
        ));
    };

    let spellings: Vec<&String> = known.iter().map(|known| &known.spelling).collect();
    let known_idents: Vec<&syn::Ident> = known.iter().map(|known| &known.ident).collect();
    Ok((
        name.clone(),
        quote! {
            impl #facet::FacetShape for #name {
                const SHAPE: #facet::Shape = #facet::Shape::Str { known: &[#(#spellings),*] };
            }

            impl ::serde::Serialize for #name {
                fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                where
                    S: ::serde::Serializer,
                {
                    match self {
                        #( Self::#known_idents => serializer.serialize_str(#spellings), )*
                        Self::#fallback(value) => serializer.serialize_str(value),
                    }
                }
            }

            impl<'de> ::serde::Deserialize<'de> for #name {
                fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
                where
                    D: ::serde::Deserializer<'de>,
                {
                    #facet::deserialize_known_str(
                        deserializer,
                        &[#( (#spellings, || Self::#known_idents), )*],
                        Self::#fallback,
                    )
                }
            }

            impl ::schemars::JsonSchema for #name {
                fn schema_name() -> ::std::borrow::Cow<'static, str> {
                    ::std::borrow::Cow::Borrowed(::core::stringify!(#name))
                }

                fn schema_id() -> ::std::borrow::Cow<'static, str> {
                    ::core::concat!(::core::module_path!(), "::", ::core::stringify!(#name)).into()
                }

                /// A vocabulary is a position, never a name a peer points at, so it is inlined.
                fn inline_schema() -> bool {
                    true
                }

                fn json_schema(
                    _generator: &mut ::schemars::SchemaGenerator,
                ) -> ::schemars::Schema {
                    #facet::open_string_schema(&[#(#spellings),*])
                }
            }
        },
    ))
}
