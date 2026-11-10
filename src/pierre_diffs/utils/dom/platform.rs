// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/platform.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: there is no `navigator`/`window`: the host passes `PlatformInfo` (`navigator.platform`, `navigator.userAgentData.platform`, `navigator.userAgent`, and whether `window.safari.pushNotification` exists) and the detection is a pure function of it, so the memoization (`_isMacLike` ...) and `resetPlatformDetection` (a test-process cache flush) are not ported. `/macOS|MacIntel|iPhone|iPad|iPod/i`, `/Linux/i`, `/\bFirefox\//` and `/^((?!chrome|android).)*safari/i` are hand-written with the regexes' semantics (ASCII case folding; `.` does not cross a line terminator).

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlatformInfo {
    /// `navigator.platform`
    pub platform: Option<String>,
    /// `navigator.userAgentData.platform`
    pub user_agent_data_platform: Option<String>,
    /// `navigator.userAgent`
    pub user_agent: String,
    /// `'safari' in window && 'pushNotification' in window.safari`
    pub has_safari_push_notification: bool,
}

impl PlatformInfo {
    // `navigator?.platform ?? navigator?.userAgentData?.platform ?? 'unknown'`
    fn platform(&self) -> &str {
        self.platform
            .as_deref()
            .or(self.user_agent_data_platform.as_deref())
            .unwrap_or("unknown")
    }
}

fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

pub fn is_mac_like(info: &PlatformInfo) -> bool {
    ["macOS", "MacIntel", "iPhone", "iPad", "iPod"]
        .iter()
        .any(|needle| contains_ignore_ascii_case(info.platform(), needle))
}

pub fn is_linux(info: &PlatformInfo) -> bool {
    contains_ignore_ascii_case(info.platform(), "Linux")
}

pub fn is_safari(info: &PlatformInfo) -> bool {
    info.has_safari_push_notification || safari_user_agent(&info.user_agent)
}

// `/^((?!chrome|android).)*safari/i`: some `safari` such that no `chrome` or
// `android` starts at an earlier position and no line terminator precedes it.
fn safari_user_agent(user_agent: &str) -> bool {
    let bytes = user_agent.as_bytes();
    let starts = |position: usize, needle: &str| {
        bytes
            .get(position..position + needle.len())
            .is_some_and(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
    };
    let mut position = 0;
    while position <= bytes.len() {
        if starts(position, "safari") {
            return true;
        }
        if starts(position, "chrome") || starts(position, "android") {
            return false;
        }
        let Some(character) = user_agent[position..].chars().next() else {
            return false;
        };
        // `.` matches anything but a line terminator.
        if matches!(character, '\n' | '\r' | '\u{2028}' | '\u{2029}') {
            return false;
        }
        position += character.len_utf8();
    }
    false
}

// Gecko-based Firefox builds (desktop and Android). Firefox on iOS (FxiOS)
// renders with WebKit and is intentionally excluded.
pub fn is_firefox(info: &PlatformInfo) -> bool {
    let user_agent = info.user_agent.as_str();
    user_agent.match_indices("Firefox/").any(|(index, _)| {
        // `\b`: a word character (`[A-Za-z0-9_]`) on exactly one side; `F` is one.
        user_agent[..index]
            .chars()
            .next_back()
            .is_none_or(|before| !(before.is_ascii_alphanumeric() || before == '_'))
    })
}
