// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/arePrePropertiesEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `PrePropertiesConfig | undefined` is an `Option`; a custom property whose value is `undefined` is a `None` value and an absent key reads as `undefined`, as in the JavaScript lookup.

use crate::types::options::CustomPreProperties;
use crate::types::options::PrePropertiesConfig;

pub fn are_pre_properties_equal(
    props_a: Option<&PrePropertiesConfig>,
    props_b: Option<&PrePropertiesConfig>,
) -> bool {
    let (Some(props_a), Some(props_b)) = (props_a, props_b) else {
        return props_a.is_none() && props_b.is_none();
    };
    are_custom_properties_equal(
        props_a.custom_properties.as_ref(),
        props_b.custom_properties.as_ref(),
    ) && props_a.pre_type == props_b.pre_type
        && props_a.diff_indicators == props_b.diff_indicators
        && props_a.disable_background == props_b.disable_background
        && props_a.disable_line_numbers == props_b.disable_line_numbers
        && props_a.overflow == props_b.overflow
        && props_a.split == props_b.split
        && props_a.total_lines == props_b.total_lines
}

fn are_custom_properties_equal(
    custom_properties_a: Option<&CustomPreProperties>,
    custom_properties_b: Option<&CustomPreProperties>,
) -> bool {
    // `EMPTY_CUSTOM_PROPERTIES` stands in for an absent bag.
    let empty = CustomPreProperties::new();
    let custom_properties_a = custom_properties_a.unwrap_or(&empty);
    let custom_properties_b = custom_properties_b.unwrap_or(&empty);
    if std::ptr::eq(custom_properties_a, custom_properties_b) {
        return true;
    }
    if custom_properties_a.len() != custom_properties_b.len() {
        return false;
    }
    for (key, value_a) in custom_properties_a {
        let value_b = custom_properties_b.get(key).and_then(Option::as_ref);
        if value_a.as_ref() != value_b {
            return false;
        }
    }
    true
}
