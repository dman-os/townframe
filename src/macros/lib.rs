//! Attribute macros for the workspace.

mod datum_fields;

use proc_macro::TokenStream;
use quote::ToTokens;
use quote::quote;
use syn::Attribute;
use syn::Expr;
use syn::ExprLit;
use syn::Item;
use syn::Lit;
use syn::Meta;
use syn::Path;
use syn::Token;
use syn::punctuated::Punctuated;

/// The module every emitted item resolves through, so moving it is one edit.
fn datum_path() -> Path {
    syn::parse_quote!(::big_cabinet_core::datum)
}

/// Declares a datum type.
///
/// Emits `DatumShape`, and `DatumMember` when the type states an id. It does not generate the
/// schema: `schemars` derives that from the same item, and the two are checked against each other
/// where the type is published.
///
/// A struct is **open** when it has exactly one `#[serde(flatten)]` overflow field of type `Extra`,
/// which is what preserves the members the type does not know about, and **closed** when it says
/// `#[datum(closed)]` together with `#[serde(deny_unknown_fields)]`. Anything else is refused:
/// `serde` drops a member it has nowhere to put, so the type and the schema would disagree.
///
/// A union is **internally tagged** by default — the member object carries `$type`, which is what a
/// Lexicon union is. Adjacent tagging is explicit, `#[datum(tagging = "adjacent")]` with
/// `#[serde(tag = "$type", content = "value")]`, and external tagging is
/// `#[datum(tagging = "external")]`, which is recorded and flagged. It is **open** when exactly one
/// variant is marked `#[datum(other)]`, and then the macro writes the wire form and the schema,
/// because a fallback that carries a payload is not expressible in serde attributes. A union that
/// says `#[datum(closed)]` has no fallback and keeps its serde derives; the macro still emits
/// `JsonSchema`, because the enum encoding is not the contract.
///
/// A vocabulary of *strings* rather than of named types is a different shape, so it is declared
/// differently: `#[datum(open_string)]`, with `#[datum(other)]` on the one variant that carries a
/// `String`. Each known value's spelling is its variant's name in lowerCamel — `MyValue` is
/// `"myValue"` — or an explicit `#[serde(rename = "…")]`. The macro writes the wire form and the
/// schema, and a string nobody declared survives as it was written. A *closed* vocabulary of strings
/// needs none of this — derive serde and `schemars` and drop the macro.
///
/// A union member is a declared type whose id is its tag, so a variant names a payload and nothing
/// else. `#[datum(id = "…")]` states an id exactly; `#[datum(id_prefix = "org.example.daybook.")]`
/// derives it from the type name, producing the same string `define_enum_and_tag!` builds.
///
/// ```ignore
/// #[datum(id_prefix = "org.example.daybook.")]
/// #[derive(Serialize, Deserialize, JsonSchema)]
/// pub struct Note {
///     pub content: String,
///     #[serde(flatten)]
///     pub extra: Extra,
/// }
/// ```
#[proc_macro_attribute]
pub fn datum(args: TokenStream, item: TokenStream) -> TokenStream {
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
                if value != "adjacent" && value != "external" && value != "internal" {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "`tagging` is \"internal\", \"adjacent\" or \"external\"",
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
                    "unknown datum argument: expected `closed`, `open_string`, `tagging = \
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
        "a generic datum type is not supported: the macro emits a plain `DatumShape` impl, and the \
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

fn member_id(name: &syn::Ident, args: &Args) -> Option<String> {
    match (&args.id, &args.prefix) {
        (Some(id), _) => Some(id.clone()),
        (None, Some(prefix)) => Some(format!("{prefix}{}", lower_camel(&name.to_string()))),
        (None, None) => None,
    }
}

fn last_type_ident(ty: &syn::Type) -> Option<&syn::Ident> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    path.path.segments.last().map(|segment| &segment.ident)
}

fn lexicon_attr(id: &str) -> syn::Attribute {
    let lit = syn::LitStr::new(id, proc_macro2::Span::call_site());
    syn::parse_quote!(#[schemars(extend("x-lexicon-id" = #lit))])
}

fn member_impl(name: &syn::Ident, args: &Args) -> proc_macro2::TokenStream {
    let datum = datum_path();
    let Some(id) = member_id(name, args) else {
        return proc_macro2::TokenStream::new();
    };
    quote! {
        impl #datum::DatumMember for #name {
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
    let datum = datum_path();
    let (name, shape) = match &mut item {
        Item::Struct(item) => struct_shape(item, &args, &datum)?,
        Item::Enum(item) if args.open_string => string_enum_shape(item, &args, &datum)?,
        Item::Enum(item) => enum_shape(item, &args, &datum)?,
        other => {
            let what = if args.open_string {
                "#[datum(open_string)] applies to an enum, and its variants are unit variants plus \
                 one `#[datum(other)]` carrying the unrecognized string"
            } else {
                "#[datum] applies to a struct or an enum"
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
    datum: &Path,
) -> syn::Result<(syn::Ident, proc_macro2::TokenStream)> {
    let name = item.ident.clone();
    no_generics(&name, &item.generics)?;
    if let Some(id) = member_id(&name, args) {
        let derived = derived_traits(&item.attrs)?;
        if derived.iter().any(|name| name == "JsonSchema") {
            item.attrs.push(lexicon_attr(&id));
        }
    }
    datum_fields::check_field_datums(&item.fields)?;
    let serde = scan_serde(&item.attrs)?;
    let mut extras = 0usize;
    for field in item.fields.iter() {
        let field_serde = scan_serde(&field.attrs)?;
        if field_serde.flatten > 0
            && last_type_ident(&field.ty).is_none_or(|ident| ident != "Extra")
        {
            return Err(syn::Error::new_spanned(
                field,
                "the overflow field's last path segment is `Extra`",
            ));
        }
        extras += field_serde.flatten;
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
        quote!(#datum::Openness::Closed)
    } else {
        if extras == 0 {
            return Err(syn::Error::new_spanned(
                &name,
                "state the openness of this object: add a `#[serde(flatten)] extra: Extra` field to \
                 preserve the members it does not know about, or declare `#[datum(closed)]` \
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
        quote!(#datum::Openness::Open)
    };
    Ok((
        name.clone(),
        quote! {
            impl #datum::DatumShape for #name {
                const SHAPE: #datum::Shape = #datum::Shape::Object { openness: #openness };
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

/// Strips the variant's `#[datum(...)]` attributes and reports whether one of them is
/// `#[datum(other)]`, the marker of the fallback variant. `not_a_list` is the error for a bare
/// `#[datum]`, `not_other` the error for any other datum attribute.
fn fallback_marker(
    variant: &mut syn::Variant,
    not_a_list: &str,
    not_other: &str,
) -> syn::Result<bool> {
    let mut is_fallback = false;
    for attr in &variant.attrs {
        if !attr.path().is_ident("datum") {
            continue;
        }
        let Meta::List(list) = &attr.meta else {
            return Err(syn::Error::new_spanned(&attr.meta, not_a_list));
        };
        if list.tokens.to_string().trim() == "other" {
            is_fallback = true;
        } else {
            return Err(syn::Error::new_spanned(attr, not_other));
        }
    }
    variant.attrs.retain(|attr| !attr.path().is_ident("datum"));
    Ok(is_fallback)
}

/// A union declaration once its variants and openness have been checked.
struct Union {
    name: syn::Ident,
    layout: Layout,
    open: bool,
    members: Vec<MemberVariant>,
    /// The `#[datum(other)]` variant and its payload; present exactly when the union is open.
    fallback: Option<(syn::Ident, syn::Type)>,
}

impl Union {
    /// Reads every variant as a member or the fallback, stripping the `#[datum(other)]` marker, and
    /// refuses a declaration whose openness, tagging and fallback disagree.
    fn parse(item: &mut syn::ItemEnum, layout: Layout, open: bool) -> syn::Result<Self> {
        let name = item.ident.clone();
        let mut members: Vec<MemberVariant> = Vec::new();
        let mut fallback: Option<(syn::Ident, syn::Type)> = None;
        for variant in item.variants.iter_mut() {
            let is_fallback = fallback_marker(
                variant,
                "expected `#[datum(other)]`",
                "the only datum attribute a union variant takes is `#[datum(other)]`, marking the \
                 fallback variant that keeps an unrecognized member",
            )?;
            datum_fields::check_field_datums(&variant.fields)?;
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

        if open && matches!(layout, Layout::External) {
            return Err(syn::Error::new_spanned(
                &name,
                "an open union is internally tagged: the codec dispatches on the discriminant key, \
                 and external tagging has none",
            ));
        }
        if open && fallback.is_none() {
            return Err(syn::Error::new_spanned(
                &name,
                "an open union needs exactly one fallback variant marked `#[datum(other)]`, or \
                 `#[datum(closed)]` to declare that an unrecognized member is an error",
            ));
        }
        if !open && fallback.is_some() {
            return Err(syn::Error::new_spanned(
                &name,
                "a variant is marked #[datum(other)] but the union is declared closed: a closed \
                 union has no fallback",
            ));
        }
        if let Some((_, payload)) = &fallback
            && last_type_ident(payload).is_none_or(|ident| ident != "UnknownMember")
        {
            return Err(syn::Error::new_spanned(
                &name,
                "the fallback variant's last path segment is `UnknownMember`",
            ));
        }
        Ok(Self {
            name,
            layout,
            open,
            members,
            fallback,
        })
    }

    fn openness(&self, datum: &Path) -> proc_macro2::TokenStream {
        if self.open {
            quote!(#datum::Openness::Open)
        } else {
            quote!(#datum::Openness::Closed)
        }
    }

    fn shape_impl(&self, datum: &Path) -> proc_macro2::TokenStream {
        let name = &self.name;
        let openness = self.openness(datum);
        let tagging = self.layout.tagging(datum);
        quote! {
            impl #datum::DatumShape for #name {
                const SHAPE: #datum::Shape = #datum::Shape::Union {
                    openness: #openness,
                    tagging: #tagging,
                };
            }
        }
    }

    fn schema_impl(&self, datum: &Path) -> proc_macro2::TokenStream {
        let name = &self.name;
        let open = self.open;
        let layout_tokens = self.layout.tokens(datum);
        let schema_members = self.members.iter().map(|member| {
            let payload = &member.payload;
            quote! {
                #datum::MemberSchema {
                    id: <#payload as #datum::DatumMember>::ID,
                    schema: generator.subschema_for::<#payload>(),
                },
            }
        });
        quote! {
            #[cfg(feature = "schemars")]
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
                    let members = ::std::vec![#(#schema_members)*];
                    #datum::union_schema(&#layout_tokens, #open, members, generator.definitions())
                }
            }
        }
    }

    /// A closed union keeps its serde derives, so each member's tag is its `#[serde(rename)]`, and
    /// the emitted asserts check every tag against the payload's member id at compile time.
    fn closed_tag_asserts(
        &self,
        item: &syn::ItemEnum,
        datum: &Path,
    ) -> syn::Result<proc_macro2::TokenStream> {
        let name = &self.name;
        let derived = derived_traits(&item.attrs)?;
        for wanted in ["Serialize", "Deserialize"] {
            if !derived.iter().any(|name| name == wanted) {
                return Err(syn::Error::new_spanned(
                    name,
                    format!("a closed union keeps its serde derives: derive {wanted} as well"),
                ));
            }
        }
        if derived.iter().any(|name| name == "JsonSchema") {
            return Err(syn::Error::new_spanned(
                name,
                "a closed union's JsonSchema is emitted by the macro: remove it from \
                 #[derive(...)]",
            ));
        }
        let mut asserts = Vec::new();
        for member in &self.members {
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
                    #datum::str_eq(#rename, <#payload as #datum::DatumMember>::ID),
                    "this tag and the payload's member id disagree",
                );
            });
        }
        Ok(quote! { #(#asserts)* })
    }

    /// An open union is the macro's to write: a fallback that keeps a member it does not know has
    /// no serde spelling, and `schemars` would emit a branch per declared variant and nothing else.
    fn open_codec(
        &self,
        item: &mut syn::ItemEnum,
        datum: &Path,
    ) -> syn::Result<proc_macro2::TokenStream> {
        let name = &self.name;
        let derived = derived_traits(&item.attrs)?;
        for replaced in ["Serialize", "Deserialize", "JsonSchema"] {
            if derived.iter().any(|name| name == replaced) {
                return Err(syn::Error::new_spanned(
                    name,
                    format!(
                        "an open union's {replaced} is emitted by the macro, because a fallback \
                         that keeps an unrecognized member is not expressible in serde attributes: \
                         remove it from #[derive(...)]"
                    ),
                ));
            }
        }
        let (fallback_ident, fallback_payload) = self.fallback.as_ref().expect("checked in parse");
        // The keys have been read; leaving them would be an attribute nothing registers, and
        // `schemars` registers `serde` on whatever it derives elsewhere.
        item.attrs.retain(|attr| !attr.path().is_ident("serde"));

        let layout_tokens = self.layout.tokens(datum);
        let serialize_arms = self.members.iter().map(|member| {
            let ident = &member.ident;
            quote! {
                Self::#ident(payload) => #datum::serialize_member(serializer, &#layout_tokens, payload),
            }
        });
        let deserialize_members = self.members.iter().map(|member| {
            let (ident, payload) = (&member.ident, &member.payload);
            quote! {
                #datum::Member {
                    id: <#payload as #datum::DatumMember>::ID,
                    read: |member| ::serde_json::from_value::<#payload>(member).map(Self::#ident),
                },
            }
        });
        Ok(quote! {
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
                    #datum::deserialize_union(
                        deserializer,
                        &#layout_tokens,
                        &[#(#deserialize_members)*],
                        |member| {
                            ::serde_json::from_value::<#fallback_payload>(member)
                                .map(Self::#fallback_ident)
                        },
                    )
                }
            }
        })
    }
}

fn enum_shape(
    item: &mut syn::ItemEnum,
    args: &Args,
    datum: &Path,
) -> syn::Result<(syn::Ident, proc_macro2::TokenStream)> {
    let name = item.ident.clone();
    no_generics(&name, &item.generics)?;
    let serde = scan_serde(&item.attrs)?;
    serde.refuse_unexpected(&name, &["tag", "content"])?;
    let layout = union_layout(&name, args, &serde)?;
    let union = Union::parse(item, layout, !args.closed)?;
    if matches!(union.layout, Layout::Internal) && serde.tag.is_none() && !union.open {
        item.attrs.push(syn::parse_quote!(#[serde(tag = "$type")]));
    }

    let shape = union.shape_impl(datum);
    let schema_impl = union.schema_impl(datum);
    if !union.open {
        let asserts = union.closed_tag_asserts(item, datum)?;
        return Ok((name, quote! { #shape #schema_impl #asserts }));
    }
    let codec = union.open_codec(item, datum)?;
    Ok((
        name,
        quote! {
            #shape

            #codec

            #schema_impl
        },
    ))
}

enum Layout {
    Internal,
    Adjacent,
    External,
}

impl Layout {
    /// The `Tagging` the union's `DatumShape` states.
    fn tagging(&self, datum: &Path) -> proc_macro2::TokenStream {
        match self {
            Layout::Internal => quote!(#datum::Tagging::Internally { tag: "$type" }),
            Layout::Adjacent => {
                quote!(#datum::Tagging::Adjacently { tag: "$type", content: "value" })
            }
            Layout::External => quote!(#datum::Tagging::Externally),
        }
    }

    /// The `UnionLayout` the emitted codec and schema dispatch on.
    fn tokens(&self, datum: &Path) -> proc_macro2::TokenStream {
        match self {
            Layout::Internal => quote!(#datum::UnionLayout::Internal { tag: "$type" }),
            Layout::Adjacent => {
                quote!(#datum::UnionLayout::Adjacent { tag: "$type", content: "value" })
            }
            Layout::External => quote!(#datum::UnionLayout::External),
        }
    }
}

fn union_layout(name: &syn::Ident, args: &Args, serde: &Serde) -> syn::Result<Layout> {
    match args.tagging.as_deref() {
        Some("external") => {
            if serde.tag.is_some() || serde.content.is_some() {
                return Err(syn::Error::new_spanned(
                    name,
                    "external tagging has no tag or content key",
                ));
            }
            Ok(Layout::External)
        }
        Some("adjacent") => {
            if serde.tag.as_deref() != Some("$type") || serde.content.as_deref() != Some("value") {
                return Err(syn::Error::new_spanned(
                    name,
                    "adjacent tagging is pinned: `#[serde(tag = \"$type\", content = \"value\")]`",
                ));
            }
            Ok(Layout::Adjacent)
        }
        Some("internal") | None => {
            if serde.content.is_some() {
                return Err(syn::Error::new_spanned(
                    name,
                    "a content key is adjacent tagging: name it with `#[datum(tagging = \
                     \"adjacent\")]` and pin the content key to \"value\"",
                ));
            }
            if let Some(tag) = &serde.tag
                && tag != "$type"
            {
                return Err(syn::Error::new_spanned(name, "the internal tag is `$type`"));
            }
            Ok(Layout::Internal)
        }
        Some(_) => unreachable!("parse rejects any other tagging"),
    }
}

/// A known spelling of a string vocabulary, on the variant whose name states it.
struct KnownVariant {
    ident: syn::Ident,
    spelling: String,
}

/// Reads every variant as a known spelling or the fallback, stripping the `datum` and `serde`
/// attributes it consumed: nothing derives serde on a vocabulary, so a leftover is a compile error.
fn vocabulary_variants(item: &mut syn::ItemEnum) -> syn::Result<(Vec<KnownVariant>, syn::Ident)> {
    let mut known: Vec<KnownVariant> = Vec::new();
    let mut fallback: Option<syn::Ident> = None;
    for variant in item.variants.iter_mut() {
        let is_fallback = fallback_marker(
            variant,
            "expected `#[datum(other)]` on the variant that keeps an unrecognized string",
            "a value's spelling is its variant's name in lowerCamel, so there is nothing to state \
             here: name the variant `MyValue` for \"myValue\", or state it explicitly with \
             `#[serde(rename = \"…\")]`",
        )?;
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
            &item.ident,
            "a vocabulary of strings needs exactly one fallback variant marked `#[datum(other)]`, \
             carrying a `String`: without one, a string nobody knows is an error",
        ));
    };
    Ok((known, fallback))
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
    datum: &Path,
) -> syn::Result<(syn::Ident, proc_macro2::TokenStream)> {
    let name = item.ident.clone();
    no_generics(&name, &item.generics)?;
    if args.closed {
        return Err(syn::Error::new_spanned(
            &name,
            "a closed vocabulary needs nothing from the macro: drop `#[datum(open_string)]` and \
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

    let (known, fallback) = vocabulary_variants(item)?;
    let spellings: Vec<&String> = known.iter().map(|known| &known.spelling).collect();
    let known_idents: Vec<&syn::Ident> = known.iter().map(|known| &known.ident).collect();
    Ok((
        name.clone(),
        quote! {
            impl #datum::DatumShape for #name {
                const SHAPE: #datum::Shape = #datum::Shape::Str { known: &[#(#spellings),*] };
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
                    #datum::deserialize_known_str(
                        deserializer,
                        &[#( (#spellings, || Self::#known_idents), )*],
                        Self::#fallback,
                    )
                }
            }

            #[cfg(feature = "schemars")]
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
                    #datum::open_string_schema(&[#(#spellings),*])
                }
            }
        },
    ))
}
