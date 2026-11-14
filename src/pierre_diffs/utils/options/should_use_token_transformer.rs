// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/shouldUseTokenTransformer.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the option bag is `ShouldUseTokenTransformerProps`, whose callbacks are presence flags (a callback is an opaque closure owned by the consuming app; only `!= null` matters here).

// Token interaction callbacks require token metadata in the markup to fire at
// all, so attaching any token callback enables the transformer;
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShouldUseTokenTransformerProps {
    pub has_on_token_click: bool,
    pub has_on_token_enter: bool,
    pub has_on_token_leave: bool,
    pub use_token_transformer: Option<bool>,
}

pub fn should_use_token_transformer(options: Option<&ShouldUseTokenTransformerProps>) -> bool {
    options.is_some_and(|options| {
        options.use_token_transformer == Some(true)
            || options.has_on_token_click
            || options.has_on_token_enter
            || options.has_on_token_leave
    })
}
