// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/getFiletypeFromFileName.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; the module-level custom-extension map is an
// owned `CustomExtensions`, so the upstream `afterEach` reset is a fresh value
// per test. `does not read inherited properties of the extension map` has no
// Rust counterpart as a hazard (a sorted slice has no prototype chain) but
// keeps its assertions.

use crate::utils::get_filetype_from_file_name::CustomExtensions;
use crate::utils::get_filetype_from_file_name::get_filetype_from_file_name;
use crate::utils::get_filetype_from_file_name::get_filetype_with_custom;

#[test]
fn detects_languages_from_simple_and_compound_extensions() {
    assert_eq!(get_filetype_from_file_name("foo.ts"), "typescript");
    assert_eq!(get_filetype_from_file_name("src/nested/foo.py"), "python");
    assert_eq!(
        get_filetype_from_file_name("src/app.component.ts"),
        "angular-ts"
    );
    assert_eq!(get_filetype_from_file_name("src/main.odin"), "odin");
}

#[test]
fn matches_exact_filenames_at_the_top_level() {
    assert_eq!(get_filetype_from_file_name("Dockerfile"), "dockerfile");
    assert_eq!(get_filetype_from_file_name("Makefile"), "makefile");
    assert_eq!(get_filetype_from_file_name("CMakeLists.txt"), "cmake");
}

#[test]
fn matches_exact_filenames_nested_in_a_path() {
    assert_eq!(
        get_filetype_from_file_name("docker/Dockerfile"),
        "dockerfile"
    );
    assert_eq!(
        get_filetype_from_file_name("build/tools/Makefile"),
        "makefile"
    );
    assert_eq!(
        get_filetype_from_file_name("project/CMakeLists.txt"),
        "cmake"
    );
    assert_eq!(
        get_filetype_from_file_name("project\\win\\Dockerfile"),
        "dockerfile"
    );
}

#[test]
fn prefers_a_real_extension_over_the_basename_retry() {
    // The retry only runs once the full path has matched nothing, so an extension still wins.
    assert_eq!(
        get_filetype_from_file_name("docker/Dockerfile.ts"),
        "typescript"
    );
}

#[test]
fn falls_back_to_plain_text_for_unknown_files() {
    assert_eq!(get_filetype_from_file_name("notes"), "text");
    assert_eq!(get_filetype_from_file_name("path/to/notes"), "text");
    assert_eq!(get_filetype_from_file_name(""), "text");
}

#[test]
fn does_not_read_inherited_properties_of_the_extension_map() {
    for name in [
        "constructor",
        "toString",
        "src/constructor",
        "src/toString",
        "src/__proto__",
        "foo.constructor",
        "foo.valueOf",
    ] {
        assert_eq!(get_filetype_from_file_name(name), "text", "{name}");
    }
}

#[test]
fn honors_an_explicit_plain_text_override_on_a_full_path() {
    let mut custom = CustomExtensions::default();
    assert!(custom.set("src/example.ts", "text"));
    assert_eq!(get_filetype_with_custom("src/example.ts", &custom), "text");
    assert_eq!(
        get_filetype_with_custom("src/other.ts", &custom),
        "typescript"
    );
    // Setting the same mapping again is a no-op that does not bump the version.
    let version = custom.version();
    assert!(!custom.set("src/example.ts", "text"));
    assert_eq!(custom.version(), version);
}
