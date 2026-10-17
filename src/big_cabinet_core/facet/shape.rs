//! What a declaration states about itself, and the check that the schema it emits agrees.
//!
//! The Rust type decides whether a round trip keeps what it does not understand; the schema is what
//! a peer validates against, and neither implies the other. `schemars` emits no
//! `additionalProperties` for a struct with no overflow field and JSON Schema reads absence as
//! permissive, so a schema can claim an object is open while `serde` drops every member the type
//! does not declare. `check_shape` runs where a type is published.

/// Whether a value keeps the members it does not know about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Openness {
    /// unknown members are preserved
    Open,
    /// unknown members are an error
    Closed,
}

/// Where a union's discriminant sits relative to its payload.
///
/// There is no `Internally`, deliberately: it puts the discriminant inside the payload, which
/// requires every payload to be an object and collides with a payload that is itself a tagged
/// union. It is a shape we convert *to*, never one we author.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tagging {
    /// `{"<tag>": "<member id>", "<content>": <payload>}`
    Adjacently {
        tag: &'static str,
        content: &'static str,
    },
    /// `{"<member id>": <payload>}`
    Externally,
}

/// What a facet declaration is, on the Rust side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    Object {
        openness: Openness,
    },
    Union {
        openness: Openness,
        tagging: Tagging,
    },
    /// A string vocabulary: every known spelling, and no constraint on any other string. A *closed*
    /// vocabulary is a plain derived enum and declares nothing here — serde and `schemars` cannot
    /// disagree about the values of an enum whose variants both of them read.
    Str {
        known: &'static [&'static str],
    },
}

/// Emitted by the `facet` macro for a declared type; not written by hand.
pub trait FacetShape {
    const SHAPE: Shape;
}

/// A decision worth surfacing rather than a mistake worth refusing.
#[cfg(feature = "schemars")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeFlag {
    /// a closed object can never gain a member compatibly
    ClosedObject,
    /// an unrecognized member key is an error at every reader, including a newer one
    ClosedUnion,
    /// the union departs from adjacent tagging, so a consumer has to be told how to read it
    NonAdjacentTagging,
}

/// The declaration and the schema do not say the same thing.
#[cfg(feature = "schemars")]
#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum ShapeError {
    /// {name}: the schema is not an object
    NotAnObject { name: String },
    /// {name}: declared {declared:?} but the schema states no openness. Absence is permissive, so an unstated schema is indistinguishable from a correctly open one; `schemars` emits the key only for a type with a `#[serde(flatten)]` overflow field
    Unstated { name: String, declared: Openness },
    /// {name}: openness is constrained by a shape rather than stated as a boolean
    Constrained { name: String },
    /// {name}: declared {declared:?} but the schema says {schema_says_open}
    Disagrees {
        name: String,
        declared: Openness,
        schema_says_open: bool,
    },
    /// {name}: the schema is neither a `oneOf` nor an `anyOf`, so it is not a union
    NotAUnion { name: String },
    /// {name}: branch {index} of the union is not an adjacent tag and payload pair
    NotAdjacent { name: String, index: usize },
    /// {name}: declared {declared:?} but {fallbacks} branch(es) accept any discriminant
    FallbackDisagrees {
        name: String,
        declared: Openness,
        fallbacks: usize,
    },
    /// {name}: the schema is not a string
    NotAString { name: String },
    /// {name}: the schema lists {schema:?} as known but the declaration lists {declared:?}
    KnownValuesDisagree {
        name: String,
        declared: Vec<String>,
        schema: Vec<String>,
    },
}

/// Checks the schema a facet type emits against the shape its declaration claims.
///
/// Called with the schema that is about to be published; `name` only makes the error readable. A
/// flag is a decision with a real cost, an error is the declaration and the schema disagreeing.
#[cfg(feature = "schemars")]
pub fn check_shape(
    name: &str,
    schema: &schemars::Schema,
    declared: Shape,
) -> Result<Vec<ShapeFlag>, ShapeError> {
    let document = serde_json::to_value(schema).expect("a schema serializes to a JSON document");
    match declared {
        Shape::Object { openness } => check_object(name, &document, openness),
        Shape::Union { openness, tagging } => check_union(name, &document, openness, tagging),
        Shape::Str { known } => check_str(name, &document, known),
    }
}

#[cfg(feature = "schemars")]
fn check_str(
    name: &str,
    document: &serde_json::Value,
    known: &'static [&'static str],
) -> Result<Vec<ShapeFlag>, ShapeError> {
    let object = document
        .as_object()
        .ok_or_else(|| ShapeError::NotAString { name: name.into() })?;
    if object.get("type").and_then(serde_json::Value::as_str) != Some("string") {
        return Err(ShapeError::NotAString { name: name.into() });
    }
    let mut schema: Vec<String> = object
        .get("knownValues")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let mut declared: Vec<String> = known.iter().map(|value| (*value).to_owned()).collect();
    schema.sort();
    declared.sort();
    if schema != declared {
        return Err(ShapeError::KnownValuesDisagree {
            name: name.into(),
            declared,
            schema,
        });
    }
    Ok(Vec::new())
}

/// `additionalProperties` cannot see properties contributed by `allOf` or a `$ref` sibling, so a
/// composed object states its openness with `unevaluatedProperties` instead.
#[cfg(feature = "schemars")]
fn openness_key(document: &serde_json::Value) -> &'static str {
    if document.get("allOf").is_some() || document.get("$ref").is_some() {
        "unevaluatedProperties"
    } else {
        "additionalProperties"
    }
}

#[cfg(feature = "schemars")]
fn check_object(
    name: &str,
    document: &serde_json::Value,
    openness: Openness,
) -> Result<Vec<ShapeFlag>, ShapeError> {
    let object = document
        .as_object()
        .ok_or_else(|| ShapeError::NotAnObject { name: name.into() })?;
    let declared_open = matches!(openness, Openness::Open);
    let key = openness_key(document);
    let Some(additional) = object.get(key) else {
        return Err(ShapeError::Unstated {
            name: name.into(),
            declared: openness,
        });
    };
    let serde_json::Value::Bool(schema_says_open) = additional else {
        return Err(ShapeError::Constrained { name: name.into() });
    };
    if *schema_says_open != declared_open {
        return Err(ShapeError::Disagrees {
            name: name.into(),
            declared: openness,
            schema_says_open: *schema_says_open,
        });
    }
    Ok(match openness {
        Openness::Open => Vec::new(),
        Openness::Closed => vec![ShapeFlag::ClosedObject],
    })
}

#[cfg(feature = "schemars")]
fn check_union(
    name: &str,
    document: &serde_json::Value,
    openness: Openness,
    tagging: Tagging,
) -> Result<Vec<ShapeFlag>, ShapeError> {
    let object = document
        .as_object()
        .ok_or_else(|| ShapeError::NotAUnion { name: name.into() })?;
    let branches = object
        .get("oneOf")
        .or_else(|| object.get("anyOf"))
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| ShapeError::NotAUnion { name: name.into() })?;

    // A branch whose discriminant is unconstrained is the fallback: it is what keeps a member the
    // reader has never heard of, so its presence is how the schema states openness.
    let mut fallbacks = 0usize;
    for (index, branch) in branches.iter().enumerate() {
        match tagging {
            Tagging::Adjacently { tag, content } => {
                let not_adjacent = || ShapeError::NotAdjacent {
                    name: name.into(),
                    index,
                };
                let properties = branch
                    .get("properties")
                    .and_then(serde_json::Value::as_object)
                    .ok_or_else(not_adjacent)?;
                let required = branch.get("required").and_then(serde_json::Value::as_array);
                let declares =
                    |key: &str| required.is_some_and(|names| names.iter().any(|name| name == key));
                if !properties.contains_key(tag)
                    || !properties.contains_key(content)
                    || !declares(tag)
                    || !declares(content)
                {
                    return Err(not_adjacent());
                }
            }
            Tagging::Externally => {
                // `{"<member id>": <payload>}`, and the fallback constrains the member *names*
                // rather than declaring one, so it has no properties at all.
                let Some(properties) = branch
                    .get("properties")
                    .and_then(serde_json::Value::as_object)
                else {
                    fallbacks += 1;
                    continue;
                };
                if properties.len() != 1 {
                    return Err(ShapeError::NotAdjacent {
                        name: name.into(),
                        index,
                    });
                }
            }
        }
        let discriminants = branch
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .map(|properties| {
                properties
                    .values()
                    .filter(|schema| schema.get("const").is_some())
                    .count()
            })
            .unwrap_or(0);
        if discriminants == 0 {
            fallbacks += 1;
        }
    }

    let declared_open = matches!(openness, Openness::Open);
    if declared_open != (fallbacks == 1) {
        return Err(ShapeError::FallbackDisagrees {
            name: name.into(),
            declared: openness,
            fallbacks,
        });
    }
    let mut flags = Vec::new();
    if !declared_open {
        flags.push(ShapeFlag::ClosedUnion);
    }
    if matches!(tagging, Tagging::Externally) {
        flags.push(ShapeFlag::NonAdjacentTagging);
    }
    Ok(flags)
}
