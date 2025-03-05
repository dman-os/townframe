use proc_macro::TokenStream;
use quote::quote;
use syn::*;

#[proc_macro_derive(HttpError, attributes(http))]
pub fn http_error_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let variants = match &input.data {
        Data::Enum(e) => &e.variants,
        _ => {
            return syn::Error::new_spanned(name, "HttpError can only be derived for enums")
                .to_compile_error()
                .into()
        }
    };

    let mut status_arms = Vec::new();
    let mut descs = Vec::new();

    // let mut desc_arms = Vec::new();
    for variant in variants {
        let variant_ident = &variant.ident;
        let Some(attr) = variant.attrs.iter().find(|a| a.path().is_ident("http")) else {
            return syn::Error::new_spanned(variant, "Missing #[http(...)] attribute")
                .to_compile_error()
                .into();
        };
        let mut status_code: Option<Expr> = None;
        let mut desc: Option<Expr> = None;
        if let Err(err) = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("code") {
                let content;
                parenthesized!(content in meta.input);
                status_code = Some(content.parse()?);
                return Ok(());
            }
            if meta.path.is_ident("desc") {
                let content;
                parenthesized!(content in meta.input);
                desc = Some(content.parse()?);
                return Ok(());
            }
            Err(meta.error("unrecognized repr"))
        }) {
            return syn::Error::new_spanned(
                variant,
                format!("error parsing #[http(...)] attribute: {err}"),
            )
            .to_compile_error()
            .into();
        }
        let Some(status_code) = status_code else {
            return syn::Error::new_spanned(variant, "`code` missing from #[http(...)] attribute")
                .to_compile_error()
                .into();
        };
        let Some(desc) = desc else {
            return syn::Error::new_spanned(variant, "`desc` missing from #[http(...)] attribute")
                .to_compile_error()
                .into();
        };
        let status_code = quote! {
            ::utils_rs::api::StatusCode::from(#status_code)
        };
        let desc = quote! {
            String::from(#desc)
        };
        descs.push(quote! {
            (#status_code, #desc)
        });
        match &variant.fields {
            syn::Fields::Named(_) => {
                status_arms.push(quote! {
                    #name::#variant_ident{ .. } => #status_code
                });
            }
            syn::Fields::Unnamed(_) => {
                status_arms.push(quote! {
                    #name::#variant_ident(..) => #status_code
                });
            }
            syn::Fields::Unit => {
                status_arms.push(quote! {
                    #name::#variant_ident => #status_code
                });
            }
        }
    }

    let expanded = quote! {
        impl ::utils_rs::api::ErrorResp for #name {
            fn error_responses() -> Vec<(StatusCode, String)> {
                vec![
                    #(#descs),*
                ]
            }
        }
        impl From<&#name> for ::utils_rs::api::StatusCode {
            fn from(value: &#name) -> ::utils_rs::api::StatusCode {
                match value {
                    #(#status_arms),*
                }
            }
        }
    };

    TokenStream::from(expanded)
}
