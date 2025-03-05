#![allow(clippy::all)]

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, DataEnum};

#[proc_macro_derive(HttpStatus, attributes(status))]
pub fn http_status_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let variants = match &input.data {
        Data::Enum(e) => &e.variants,
        _ => return syn::Error::new_spanned(name, "HttpStatus can only be derived for enums")
            .to_compile_error()
            .into(),
    };

    let mut match_arms = Vec::new();
    for variant in variants {
        let variant_ident = &variant.ident;
        let status_code = variant.attrs.iter()
            .find(|a| a.path().is_ident("status"))
            .and_then(|attr| attr.parse_args::<syn::Expr>().ok())
            .unwrap_or_else(|| {
                syn::Error::new_spanned(variant, "Missing #[status(...)] attribute")
                    .to_compile_error()
                    .into()
            });

        match_arms.push(quote! {
            #name::#variant_ident => ::axum::http::StatusCode::from(#status_code)
        });
    }

    let expanded = quote! {
        impl From<#name> for ::axum::http::StatusCode {
            fn from(value: #name) -> ::axum::http::StatusCode {
                match value {
                    #(#match_arms),*
                }
            }
        }
    };

    TokenStream::from(expanded)
}
