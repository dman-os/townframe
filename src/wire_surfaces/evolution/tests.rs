use super::*;
use serde_json::json;

/// A record Lexicon whose one object has `object`'s keywords, `revision` and a `defs` entry.
fn record(object: Value) -> Value {
    json!({
        "lexicon": 1, "id": "x.y.z",
        "defs": { "main": { "type": "record", "key": "any", "record": object } },
    })
}

/// The Lexicon with `revision` stated.
fn at_revision(mut lexicon: Value, revision: u64) -> Value {
    lexicon["revision"] = json!(revision);
    lexicon
}

fn object(properties: Value, required: Value) -> Value {
    json!({ "type": "object", "required": required, "properties": properties })
}

fn overall(old: &Value, new: &Value) -> &'static str {
    let differences = classify(old, new);
    if differences.is_empty() {
        "same"
    } else if differences
        .iter()
        .any(|difference| difference.severity == Severity::Breaking)
    {
        "breaking"
    } else {
        "compatible"
    }
}

utils_rs::table_tests! {
    classifies,
    (old, new, expected),
    {
        assert_eq!(overall(&old, &new), expected, "{:?}", classify(&old, &new));
    }
}

classifies! {
    identical: (
        record(object(json!({"a": {"type": "string"}}), json!(["a"]))),
        record(object(json!({"a": {"type": "string"}}), json!(["a"]))),
        "same"
    ),
    description_is_ignored: (
        record(object(json!({"a": {"type": "string", "description": "one"}}), json!([]))),
        record(object(json!({"a": {"type": "string", "description": "two"}}), json!([]))),
        "same"
    ),
    new_optional_property_is_compatible: (
        record(object(json!({"a": {"type": "string"}}), json!(["a"]))),
        record(object(json!({"a": {"type": "string"}, "b": {"type": "integer"}}), json!(["a"]))),
        "compatible"
    ),
    new_required_property_is_breaking: (
        record(object(json!({"a": {"type": "string"}}), json!(["a"]))),
        record(object(json!({"a": {"type": "string"}, "b": {"type": "integer"}}), json!(["a", "b"]))),
        "breaking"
    ),
    optional_property_becoming_required_is_breaking: (
        record(object(json!({"a": {"type": "string"}}), json!([]))),
        record(object(json!({"a": {"type": "string"}}), json!(["a"]))),
        "breaking"
    ),
    removed_property_is_breaking: (
        record(object(json!({"a": {"type": "string"}, "b": {"type": "string"}}), json!([]))),
        record(object(json!({"a": {"type": "string"}}), json!([]))),
        "breaking"
    ),
    renamed_property_is_breaking: (
        record(object(json!({"a": {"type": "string"}}), json!([]))),
        record(object(json!({"b": {"type": "string"}}), json!([]))),
        "breaking"
    ),
    changed_type_is_breaking: (
        record(object(json!({"a": {"type": "string"}}), json!([]))),
        record(object(json!({"a": {"type": "integer"}}), json!([]))),
        "breaking"
    ),
    changed_bound_is_breaking: (
        record(object(json!({"a": {"type": "string", "maxLength": 8}}), json!([]))),
        record(object(json!({"a": {"type": "string", "maxLength": 4}}), json!([]))),
        "breaking"
    ),
    changed_ref_is_breaking: (
        record(object(json!({"a": {"type": "ref", "ref": "#one"}}), json!([]))),
        record(object(json!({"a": {"type": "ref", "ref": "#two"}}), json!([]))),
        "breaking"
    ),
    nested_new_optional_property_is_compatible: (
        record(object(json!({"a": object(json!({"x": {"type": "string"}}), json!([]))}), json!([]))),
        record(object(json!({"a": object(json!({"x": {"type": "string"}, "y": {"type": "string"}}), json!([]))}), json!([]))),
        "compatible"
    ),
    nested_changed_type_is_breaking: (
        record(object(json!({"a": {"type": "array", "items": {"type": "string"}}}), json!([]))),
        record(object(json!({"a": {"type": "array", "items": {"type": "integer"}}}), json!([]))),
        "breaking"
    ),
    new_known_value_is_compatible: (
        record(object(json!({"a": {"type": "string", "knownValues": ["x"]}}), json!([]))),
        record(object(json!({"a": {"type": "string", "knownValues": ["x", "y"]}}), json!([]))),
        "compatible"
    ),
    lost_known_value_is_breaking: (
        record(object(json!({"a": {"type": "string", "knownValues": ["x", "y"]}}), json!([]))),
        record(object(json!({"a": {"type": "string", "knownValues": ["x"]}}), json!([]))),
        "breaking"
    ),
    closed_enum_losing_a_value_is_breaking: (
        record(object(json!({"a": {"type": "string", "enum": ["x", "y"]}}), json!([]))),
        record(object(json!({"a": {"type": "string", "enum": ["x"]}}), json!([]))),
        "breaking"
    ),
    closed_enum_gaining_a_value_is_breaking: (
        record(object(json!({"a": {"type": "string", "enum": ["x"]}}), json!([]))),
        record(object(json!({"a": {"type": "string", "enum": ["x", "y"]}}), json!([]))),
        "breaking"
    ),
    open_union_gaining_a_member_is_compatible: (
        record(object(json!({"a": {"type": "union", "refs": ["#one"]}}), json!([]))),
        record(object(json!({"a": {"type": "union", "refs": ["#one", "#two"]}}), json!([]))),
        "compatible"
    ),
    open_union_losing_a_member_is_breaking: (
        record(object(json!({"a": {"type": "union", "refs": ["#one", "#two"]}}), json!([]))),
        record(object(json!({"a": {"type": "union", "refs": ["#one"]}}), json!([]))),
        "breaking"
    ),
    closed_union_losing_a_member_is_breaking: (
        record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one", "#two"]}}), json!([]))),
        record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one"]}}), json!([]))),
        "breaking"
    ),
    closed_union_gaining_a_member_is_breaking: (
        record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one"]}}), json!([]))),
        record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one", "#two"]}}), json!([]))),
        "breaking"
    ),
    union_becoming_closed_is_breaking: (
        record(object(json!({"a": {"type": "union", "refs": ["#one"]}}), json!([]))),
        record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one"]}}), json!([]))),
        "breaking"
    ),
    union_becoming_open_is_compatible: (
        record(object(json!({"a": {"type": "union", "closed": true, "refs": ["#one"]}}), json!([]))),
        record(object(json!({"a": {"type": "union", "closed": false, "refs": ["#one"]}}), json!([]))),
        "compatible"
    ),
    removed_def_is_breaking: (
        json!({"defs": {"main": {"type": "object", "properties": {}}, "one": {"type": "string"}}}),
        json!({"defs": {"main": {"type": "object", "properties": {}}}}),
        "breaking"
    ),
    new_def_is_compatible: (
        json!({"defs": {"main": {"type": "object", "properties": {}}}}),
        json!({"defs": {"main": {"type": "object", "properties": {}}, "one": {"type": "string"}}}),
        "compatible"
    ),
    new_method_error_is_compatible: (
        json!({"defs": {"main": {"type": "query", "errors": [{"name": "A"}]}}}),
        json!({"defs": {"main": {"type": "query", "errors": [{"name": "A"}, {"name": "B"}]}}}),
        "compatible"
    ),
    removed_method_error_is_breaking: (
        json!({"defs": {"main": {"type": "query", "errors": [{"name": "A"}, {"name": "B"}]}}}),
        json!({"defs": {"main": {"type": "query", "errors": [{"name": "A"}]}}}),
        "breaking"
    ),
    a_property_named_revision_is_not_the_revision: (
        record(object(json!({"revision": {"type": "integer"}}), json!([]))),
        record(object(json!({"revision": {"type": "string"}}), json!([]))),
        "breaking"
    ),
}

utils_rs::table_tests! {
    judges,
    (old, new, verdict),
    {
        let found = violations("x.y.z", Surface::of(&old), Surface::of(&new));
        match verdict {
            "ok" => assert!(found.is_empty(), "{found:?}"),
            needle => assert!(
                found.iter().any(|line| line.contains(needle)),
                "no `{needle}` in {found:?}",
            ),
        }
    }
}

judges! {
    no_change_needs_no_bump: (
        record(object(json!({}), json!([]))),
        record(object(json!({}), json!([]))),
        "ok"
    ),
    compatible_change_with_bump_passes: (
        at_revision(record(object(json!({}), json!([]))), 1),
        at_revision(record(object(json!({"a": {"type": "string"}}), json!([]))), 2),
        "ok"
    ),
    compatible_change_first_bump_from_no_revision_passes: (
        record(object(json!({}), json!([]))),
        at_revision(record(object(json!({"a": {"type": "string"}}), json!([]))), 1),
        "ok"
    ),
    compatible_change_without_bump_fails: (
        at_revision(record(object(json!({}), json!([]))), 1),
        at_revision(record(object(json!({"a": {"type": "string"}}), json!([]))), 1),
        "need a bumped `revision`"
    ),
    a_file_with_no_revision_is_unstable_and_changes_freely: (
        record(object(json!({"a": {"type": "string"}}), json!([]))),
        record(object(json!({"a": {"type": "integer"}}), json!([]))),
        "ok"
    ),
    breaking_change_fails_even_with_a_bump: (
        at_revision(record(object(json!({"a": {"type": "string"}}), json!([]))), 1),
        at_revision(record(object(json!({"a": {"type": "integer"}}), json!([]))), 2),
        "mints a new NSID"
    ),
    breaking_change_beside_a_compatible_one_fails: (
        at_revision(record(object(json!({"a": {"type": "string"}}), json!([]))), 1),
        at_revision(record(object(json!({"b": {"type": "string"}}), json!([]))), 3),
        "removed or renamed"
    ),
}

/// A datum's Lexicon and sidecar from `(name, extras)` properties: the extras are the
/// stability keywords, which only the sidecar carries, and the Lexicon's `revision` is the
/// highest revision they name. A property named `u` is an open union of `members`, whose own
/// `extras` are keyed by member id.
fn datum(properties: Value) -> (Value, Value) {
    let mut lexicon_properties = Map::new();
    let mut sidecar_properties = Map::new();
    let mut revision = 0;
    let mut named = |extras: &Value| {
        let declared = Declared::read(extras.as_object().unwrap()).unwrap();
        revision = revision.max(declared.revision().unwrap_or(0));
    };
    for (name, extras) in properties.as_object().unwrap() {
        let (lexicon, sidecar) = if name == "u" {
            let members: Vec<&str> = extras["members"]
                .as_array()
                .unwrap()
                .iter()
                .map(|member| member.as_str().unwrap())
                .collect();
            let mut branches: Vec<Value> = members
                .iter()
                .map(|id| {
                    let mut branch = json!({
                        "type": "object",
                        "properties": { "$type": { "const": id } },
                    });
                    if let Some(more) = extras["extras"].get(*id) {
                        named(more);
                        branch
                            .as_object_mut()
                            .unwrap()
                            .extend(more.as_object().unwrap().clone());
                    }
                    branch
                })
                .collect();
            branches.push(json!({
                "type": "object",
                "properties": { "$type": { "type": "string", "not": { "enum": members } } },
            }));
            (
                json!({ "type": "union", "refs": members }),
                json!({ "oneOf": branches }),
            )
        } else {
            named(extras);
            let mut sidecar = json!({ "type": "string" });
            sidecar
                .as_object_mut()
                .unwrap()
                .extend(extras.as_object().unwrap().clone());
            (json!({ "type": "string" }), sidecar)
        };
        lexicon_properties.insert(name.clone(), lexicon);
        sidecar_properties.insert(name.clone(), sidecar);
    }
    let mut lexicon = record(object(Value::Object(lexicon_properties), json!([])));
    if revision > 0 {
        lexicon["revision"] = json!(revision);
    }
    let sidecar = json!({ "type": "object", "properties": sidecar_properties });
    (lexicon, sidecar)
}

utils_rs::table_tests! {
    stability,
    (old, new, verdict),
    {
        let (old, new) = (datum(old), datum(new));
        let found = violations(
            "x.y.z",
            Surface { lexicon: &old.0, sidecar: Some(&old.1) },
            Surface { lexicon: &new.0, sidecar: Some(&new.1) },
        );
        match verdict {
            "ok" => assert!(found.is_empty(), "{found:?}"),
            needle => assert!(
                found.iter().any(|line| line.contains(needle)),
                "no `{needle}` in {found:?}",
            ),
        }
    }
}

stability! {
    an_unannotated_field_is_unstable_and_changes_freely: (
        json!({"a": {}, "b": {}}),
        json!({"a": {"type": "integer"}}),
        "ok"
    ),
    a_new_field_carrying_the_new_revision_is_fine: (
        json!({"a": {"x-since": 1}}),
        json!({"a": {"x-since": 1}, "b": {"x-since": 2}}),
        "ok"
    ),
    a_new_stable_field_below_the_revision_is_refused: (
        json!({"a": {"x-since": 1}}),
        json!({"a": {"x-since": 1}, "b": {"x-since": 2}, "c": {"x-since": 3}}),
        "new stable item `/properties/b` must carry `since = 3`"
    ),
    a_stable_field_is_never_removed: (
        json!({"a": {"x-since": 1}, "b": {"x-since": 1}}),
        json!({"a": {"x-since": 1}}),
        "`/properties/b` was stable and was removed; deprecate it instead"
    ),
    a_deprecated_field_may_not_be_removed_either: (
        json!({"a": {}, "b": {"x-since": 1, "x-deprecated": {"since": 3, "note": "use a"}}}),
        json!({"a": {}}),
        "`/properties/b` was stable and was removed"
    ),
    a_new_unannotated_field_beside_stable_ones_is_unstable: (
        json!({"a": {"x-since": 1}}),
        json!({"a": {"x-since": 1}, "b": {}}),
        "ok"
    ),
    an_unannotated_field_may_be_dropped_beside_stable_ones: (
        json!({"a": {"x-since": 1}, "b": {}}),
        json!({"a": {"x-since": 1}}),
        "ok"
    ),
    deprecating_at_the_new_revision_is_fine: (
        json!({"a": {}, "b": {"x-since": 1}}),
        json!({"a": {}, "b": {"x-since": 1, "x-deprecated": {"since": 2, "note": "use a"}}}),
        "ok"
    ),
    deprecating_at_another_revision_is_refused: (
        json!({"a": {}, "b": {"x-since": 1}, "c": {"x-since": 2}}),
        json!({"a": {}, "b": {"x-since": 1, "x-deprecated": {"since": 2, "note": "use a"}}, "c": {"x-since": 3}}),
        "newly deprecated since 2, and the file's revision is 3"
    ),
    a_stable_field_cannot_become_unstable: (
        json!({"a": {"x-since": 1}}),
        json!({"a": {"x-unstable": "drafts"}}),
        "`/properties/a` was stable and became unstable"
    ),
    a_stable_field_cannot_silently_lose_its_since: (
        json!({"a": {"x-since": 1}}),
        json!({"a": {}}),
        "`/properties/a` was stable and became unstable"
    ),
    a_stable_fields_since_is_fixed: (
        json!({"a": {"x-since": 2}}),
        json!({"a": {"x-since": 3}}),
        "changed its `since`"
    ),
    stabilizing_a_field_needs_the_new_revision: (
        json!({"a": {}, "b": {"x-unstable": "drafts"}}),
        json!({"a": {}, "b": {"x-since": 2}}),
        "ok"
    ),
    stabilizing_a_field_below_the_revision_is_refused: (
        json!({"a": {"x-since": 1}, "b": {}}),
        json!({"a": {"x-since": 1}, "b": {"x-since": 2}, "c": {"x-since": 3}}),
        "new stable item `/properties/b` must carry `since = 3`"
    ),
    tightening_a_stable_fields_constraint_is_refused: (
        json!({"a": {"x-since": 1, "maxLength": 8}}),
        json!({"a": {"x-since": 1, "maxLength": 4}}),
        "breaking sidecar change at `/properties/a/maxLength`"
    ),
    loosening_a_stable_fields_constraint_is_fine: (
        json!({"a": {"x-since": 1, "maxLength": 4}}),
        json!({"a": {"x-since": 1, "maxLength": 8}}),
        "ok"
    ),
    changing_a_stable_fields_storage_form_is_refused: (
        json!({"a": {"x-since": 1, "x-automerge": "text"}}),
        json!({"a": {"x-since": 1, "x-automerge": "timestamp"}}),
        "the storage form changed"
    ),
    tightening_an_unstable_fields_constraint_is_fine: (
        json!({"a": {"maxLength": 8}}),
        json!({"a": {"maxLength": 4}}),
        "ok"
    ),
    a_new_union_member_carries_the_new_revision: (
        json!({"u": {"members": ["a.one"], "extras": {"a.one": {"x-since": 1}}}}),
        json!({"u": {"members": ["a.one", "a.two"], "extras": {"a.one": {"x-since": 1}, "a.two": {"x-since": 2}}}}),
        "ok"
    ),
    a_new_stable_union_member_below_the_revision_is_refused: (
        json!({"u": {"members": ["a.one"], "extras": {"a.one": {"x-since": 1}}}}),
        json!({"u": {"members": ["a.one", "a.two"], "extras": {"a.one": {"x-since": 1}, "a.two": {"x-since": 2}}}, "z": {"x-since": 3}}),
        "new stable item `/properties/u/$union/a.two` must carry `since = 3`"
    ),
    a_stable_union_member_is_never_dropped: (
        json!({"u": {"members": ["a.one", "a.two"], "extras": {"a.one": {"x-since": 1}, "a.two": {"x-since": 1}}}}),
        json!({"u": {"members": ["a.one"], "extras": {"a.one": {"x-since": 1}}}}),
        "`/properties/u/$union/a.two` was stable and was removed"
    ),
    an_unstable_union_member_may_be_dropped: (
        json!({"u": {"members": ["a.one", "a.two"], "extras": {"a.one": {"x-since": 1}}}}),
        json!({"u": {"members": ["a.one"], "extras": {"a.one": {"x-since": 1}}}}),
        "ok"
    ),
}

/// A method has no sidecar, so its stability sits beside its `revision`: a file that names one
/// is stable, and any other is unstable.
#[test]
fn a_file_is_stable_when_it_names_a_revision() {
    let unstable = json!({"defs": {"main": {"type": "query", "errors": [{"name": "A"}]}}});
    let changed = json!({"defs": {"main": {"type": "procedure"}}});
    assert!(violations("x.y.z", Surface::of(&unstable), Surface::of(&changed)).is_empty());

    let stable = at_revision(unstable.clone(), 1);
    let stable_changed = at_revision(changed.clone(), 1);
    assert!(!violations("x.y.z", Surface::of(&stable), Surface::of(&stable_changed)).is_empty());
    // Stabilizing is a change nobody is held to; destabilizing is refused.
    let stabilized = violations(
        "x.y.z",
        Surface::of(&unstable),
        Surface::of(&stable_changed),
    );
    assert!(stabilized.is_empty(), "{stabilized:?}");
    let destabilized = violations("x.y.z", Surface::of(&stable), Surface::of(&changed));
    assert!(
        destabilized
            .iter()
            .any(|line| line.contains("the file was stable and became unstable")),
        "{destabilized:?}"
    );
}
