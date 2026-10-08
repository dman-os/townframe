//! What one plug declares: the definitions it carries, which of them are facets, and the context
//! they are read in.
//!
//! A definition is not standalone. A facet's schema refers to the types it names by `#/$defs/<name>`,
//! so it is read in a context — the way an OpenAPI document resolves against its own components and
//! the documents it includes, and the way an AT Proto record resolves against every NSID in the
//! world. Ours is narrower and explicit: **the plug, plus the plugs it references**.
//!
//! # The definitions are carried, not looked up
//!
//! There is no registry. A plug carries the schema of every type its facets reach, *including* one
//! that belongs to another plug, so a plug can be read on its own and a definition can disappear
//! from the world without taking its consumers with it. [`Declaration::of`] hoists those definitions
//! out of a facet's schema into the manifest, so a type two facets both name is carried once, and a
//! facet's own schema is its definition with nothing extra inlined.
//!
//! # Some checks are only possible here
//!
//! `check_shape` runs per declaration, inside [`Declaration::of`], because only the compiler knows
//! the shape a type *claims*. Everything plural is checked in [`Bundle::validate`]: two facets
//! claiming one tag, two definitions disagreeing about a name they both declare, and a reference
//! that resolves in neither this plug nor a dependency.

use crate::facet::{FacetMember, FacetShape, ShapeFlag, check_shape};
use schemars::{JsonSchema, Schema};
use serde_json::{Map, Value};
use std::collections::HashSet;

/// A facet a plug declares: the tag a facet key carries, the name a definition refers to it by, its
/// definition, and the decisions that declaration makes at a cost.
#[derive(Debug, Clone)]
pub struct FacetDecl {
    pub id: String,
    pub name: String,
    pub schema: Schema,
    pub flags: Vec<ShapeFlag>,
}

/// A named type a declaration reaches, which is not a facet: it is written at its positions, nothing
/// keys a facet on it, and it has no display or reference behaviour of its own.
#[derive(Debug, Clone)]
pub struct TypeDecl {
    pub name: String,
    pub schema: Schema,
}

/// One facet and the definitions its schema brought with it, before the plug has collected them.
#[derive(Debug, Clone)]
pub struct Declaration {
    pub facet: FacetDecl,
    pub types: Vec<TypeDecl>,
}

impl Declaration {
    /// Declares one facet type.
    ///
    /// The id comes from the type, the definition is the schema `schemars` derived for it, and every
    /// other definition that schema reached is hoisted out to travel beside it. The shape the type
    /// claims is checked against its definition here, which is the one place both are in hand.
    pub fn of<T>() -> Result<Self, crate::facet::ShapeError>
    where
        T: FacetMember + FacetShape + JsonSchema,
    {
        let derived = schemars::schema_for!(T);
        let schema = without_definitions(&derived);
        let flags = check_shape(T::ID, &schema, T::SHAPE)?;
        Ok(Self {
            facet: FacetDecl {
                id: T::ID.to_owned(),
                name: T::schema_name().into_owned(),
                schema,
                flags,
            },
            types: definitions(&derived),
        })
    }
}

/// Everything one plug declares.
#[derive(Debug, Clone, Default)]
pub struct Universe {
    pub facets: Vec<FacetDecl>,
    pub types: Vec<TypeDecl>,
}

/// A definition in scope, and what declares it.
type Declared<'a> = (&'a str, &'a Schema, &'a str);

impl Universe {
    /// Collects the facets and the definitions they brought with them.
    ///
    /// A definition two facets both brought is carried once. A name two of them disagree about is
    /// carried twice, deliberately, so that [`Bundle::validate`] reports it rather than one of them
    /// quietly winning.
    pub fn collect(declarations: impl IntoIterator<Item = Declaration>) -> Self {
        let mut facets: Vec<FacetDecl> = Vec::new();
        let mut types: Vec<TypeDecl> = Vec::new();
        for declaration in declarations {
            for decl in declaration.types {
                if declaration.facet.name == decl.name {
                    continue;
                }
                if types
                    .iter()
                    .any(|seen| seen.name == decl.name && same_schema(&seen.schema, &decl.schema))
                {
                    continue;
                }
                types.push(decl);
            }
            facets.push(declaration.facet);
        }
        Self { facets, types }
    }
}

/// The context a definition is read in: one plug's declarations, and the declarations of the plugs it
/// refers to, nearest first.
#[derive(Debug, Clone)]
pub struct Bundle<'a> {
    pub plug: &'a Universe,
    pub dependencies: Vec<&'a Universe>,
}

impl<'a> Bundle<'a> {
    pub fn new(plug: &'a Universe, dependencies: Vec<&'a Universe>) -> Self {
        Self { plug, dependencies }
    }

    /// Every definition in scope, with the name it is declared under.
    pub fn definitions(&self) -> Vec<Declared<'a>> {
        std::iter::once(self.plug)
            .chain(self.dependencies.iter().copied())
            .flat_map(|universe| {
                universe
                    .facets
                    .iter()
                    .map(|facet| (facet.name.as_str(), &facet.schema, facet.id.as_str()))
                    .chain(
                        universe
                            .types
                            .iter()
                            .map(|decl| (decl.name.as_str(), &decl.schema, "(a type)")),
                    )
            })
            .collect()
    }

    /// The definition a name resolves to, which is what makes the context a context.
    pub fn resolve(&self, name: &str) -> Option<&'a Schema> {
        self.definitions()
            .into_iter()
            .find(|(declared, ..)| *declared == name)
            .map(|(_, schema, _)| schema)
    }

    /// Checks what only the whole context can check.
    ///
    /// Returns the flags every declaration carried, for whoever reads the manifest.
    pub fn validate(&self) -> Result<Vec<ShapeFlag>, UniverseError> {
        let declared = self.definitions();

        let mut seen_ids: HashSet<&str> = HashSet::new();
        let mut flags: Vec<ShapeFlag> = Vec::new();
        for facet in &self.plug.facets {
            if !seen_ids.insert(&facet.id) {
                return Err(UniverseError::DuplicateId {
                    id: facet.id.clone(),
                });
            }
            flags.extend(facet.flags.iter().copied());
        }

        let mut by_name: Vec<Declared<'a>> = Vec::new();
        for (name, schema, owner) in declared.iter().copied() {
            let disagreeing = by_name
                .iter()
                .copied()
                .find(|(declared, seen, _)| *declared == name && !same_schema(seen, schema));
            if let Some((_, _, first)) = disagreeing {
                return Err(UniverseError::Conflict {
                    name: name.to_owned(),
                    first: first.to_owned(),
                    second: owner.to_owned(),
                });
            }
            if !by_name.iter().any(|(declared, ..)| *declared == name) {
                by_name.push((name, schema, owner));
            }
        }

        let names: HashSet<&str> = by_name.iter().map(|(name, ..)| *name).collect();
        for (name, schema, _owner) in declared.iter().copied() {
            for target in references(schema) {
                if !names.contains(target.as_str()) {
                    return Err(UniverseError::Undeclared {
                        name: target,
                        referenced_by: name.to_owned(),
                    });
                }
            }
        }
        Ok(flags)
    }

    /// The single document a peer validates a value against: every definition in scope, and the
    /// named facet's definition as the root.
    ///
    /// This is what makes the context cost a consumer nothing. The manifest carries definitions
    /// flat, because that is what lets two facets share one; a reader that just wants to check a
    /// value takes this instead of assembling `$defs` itself.
    pub fn document(&self, facet_name: &str) -> Option<Schema> {
        let mut defs = Map::new();
        for (name, schema, _owner) in self.definitions() {
            defs.entry(name.to_owned())
                .or_insert_with(|| serde_json::to_value(schema).expect("a schema is a JSON value"));
        }
        if !defs.contains_key(facet_name) {
            return None;
        }
        let mut root = Map::new();
        root.insert(
            "$schema".to_owned(),
            Value::from("https://json-schema.org/draft/2020-12/schema"),
        );
        root.insert(
            "$ref".to_owned(),
            Value::from(format!("#/$defs/{facet_name}")),
        );
        root.insert("$defs".to_owned(), Value::Object(defs));
        Schema::try_from(Value::Object(root)).ok()
    }
}

/// The declaration and the manifest do not agree, or the manifest does not stand on its own.
#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum UniverseError {
    /// two facets of one plug claim the tag {id}
    DuplicateId { id: String },
    /// {name} is declared twice, as {first} and as {second}, and the two disagree
    Conflict {
        name: String,
        first: String,
        second: String,
    },
    /// {referenced_by} refers to a definition named {name}, which neither this plug nor a dependency declares
    Undeclared { name: String, referenced_by: String },
}

/// The named types a schema reaches, hoisted out of it.
fn definitions(schema: &Schema) -> Vec<TypeDecl> {
    let document = serde_json::to_value(schema).expect("a schema serializes to a JSON document");
    let Some(defs) = document.get("$defs").and_then(Value::as_object) else {
        return Vec::new();
    };
    defs.iter()
        .filter_map(|(name, schema)| {
            Schema::try_from(schema.clone())
                .ok()
                .map(|schema| TypeDecl {
                    name: name.clone(),
                    schema,
                })
        })
        .collect()
}

/// The same schema without the definitions it was carrying, which is the definition itself.
fn without_definitions(schema: &Schema) -> Schema {
    let mut document =
        serde_json::to_value(schema).expect("a schema serializes to a JSON document");
    if let Some(object) = document.as_object_mut() {
        object.remove("$defs");
    }
    Schema::try_from(document).expect("a definition is a valid JSON Schema")
}

/// Every `#/$defs/<name>` a schema points at, anywhere inside it.
fn references(schema: &Schema) -> Vec<String> {
    let document = serde_json::to_value(schema).expect("a schema serializes to a JSON document");
    let mut out = Vec::new();
    collect_references(&document, &mut out);
    out
}

fn collect_references(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            for (key, nested) in object {
                if key == "$ref"
                    && let Some(target) = nested.as_str().and_then(|it| it.strip_prefix("#/$defs/"))
                {
                    out.push(target.to_owned());
                    continue;
                }
                collect_references(nested, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_references(item, out);
            }
        }
        _ => {}
    }
}

fn same_schema(one: &Schema, two: &Schema) -> bool {
    serde_json::to_value(one).ok() == serde_json::to_value(two).ok()
}
