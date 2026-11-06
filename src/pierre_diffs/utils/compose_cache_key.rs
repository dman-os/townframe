// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/composeCacheKey.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `JSON.stringify` of the segment array is `serde_json::to_string`; both escape control characters as lowercase `\\u00xx` and leave U+2028/2029 alone.

const CACHE_KEY_VERSION: u32 = 1;

/// Encodes caller-controlled segments without delimiter ambiguity.
pub fn compose_cache_key(scope: &str, segments: &[&str]) -> String {
    let mut parts = Vec::with_capacity(segments.len() + 1);
    parts.push(scope);
    parts.extend_from_slice(segments);
    let json = serde_json::to_string(&parts).expect("a list of strings always serializes");
    format!("ck{CACHE_KEY_VERSION}:{json}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_json_encoded_and_versioned() {
        assert_eq!(
            compose_cache_key("diff", &["a", "b\"c"]),
            "ck1:[\"diff\",\"a\",\"b\\\"c\"]"
        );
        assert_ne!(
            compose_cache_key("s", &["a,b"]),
            compose_cache_key("s", &["a", "b"])
        );
    }
}
