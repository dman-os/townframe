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

use crate::facet::FacetMember;
use crate::facet::FacetShape;
use crate::facet::Shape;
use crate::facet::ShapeFlag;
use crate::facet::Tagging;
use crate::facet::check_shape;
use crate::facet::lexicon::project_facet;
use schemars::JsonSchema;
use schemars::Schema;
use serde_json::Map;
use serde_json::Value;
use std::collections::HashSet;

/// A facet a plug declares: the tag a facet key carries, the name a definition refers to it by, its
/// definition, and the decisions that declaration makes at a cost.
///
/// The Lexicon it publishes is not a field: it is projected from the definitions the whole bundle
/// carries ([`Bundle::lexicons`]), because a reference a facet makes resolves against the plug and
/// its dependencies and not against the declaration alone.
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
    /// Declares one facet type, refusing a schema that is not lexicon-aligned.
    ///
    /// The id comes from the type, the definition is the schema `schemars` derived for it, and every
    /// other definition that schema reached is hoisted out to travel beside it. The shape the type
    /// claims is checked against its definition here, which is the one place both are in hand, and so
    /// is the Lexicon the definition has to project to — refused here, at authoring time, rather than
    /// at the publish that would carry it.
    pub fn of<T>() -> Result<Self, crate::facet::ShapeError>
    where
        T: FacetMember + FacetShape + JsonSchema,
    {
        Self::declared::<T>(Publish::Yes)
    }

    /// The schema bundle without the lexicon gate.
    ///
    /// The codec's document is a `$ref` bundle a walk can store under. A type such as one with a
    /// deliberate plain number is a legitimate codec position and is not canonical JSON, so it
    /// assembles here and is refused by [`Bundle::lexicons`] rather than silently published.
    pub fn assemble<T>() -> Result<Self, crate::facet::ShapeError>
    where
        T: FacetMember + FacetShape + JsonSchema,
    {
        Self::declared::<T>(Publish::No)
    }

    fn declared<T>(publish: Publish) -> Result<Self, crate::facet::ShapeError>
    where
        T: FacetMember + FacetShape + JsonSchema,
    {
        let derived = schemars::schema_for!(T);
        let document = serde_json::to_value(&derived).expect("a schema serializes");
        let schema = without_definitions(&derived);
        let flags = check_shape(T::ID, &schema, T::SHAPE)?;
        if let Publish::Yes = publish {
            check_aligned(T::ID, T::SHAPE, &document)?;
        }
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

/// Whether a declaration has to be publishable as a Lexicon, or only usable as a codec document.
enum Publish {
    Yes,
    No,
}

/// The alignment gate: this declaration's schema has to project to a Lexicon file.
///
/// Adjacent and external unions are explicit departures from the internally tagged member a Lexicon
/// union is, so they are refused here and stay representable in the sidecar only.
fn check_aligned(id: &str, shape: Shape, document: &Value) -> Result<(), crate::facet::ShapeError> {
    if let Shape::Union {
        tagging: Tagging::Adjacently { .. } | Tagging::Externally,
        ..
    } = shape
    {
        return Err(crate::facet::ShapeError::NotLexiconAligned {
            name: id.to_owned(),
            reason: "only an internally tagged union is a lexicon union".to_owned(),
        });
    }
    project_facet(id, document).map_err(|reason| crate::facet::ShapeError::NotLexiconAligned {
        name: id.to_owned(),
        reason,
    })?;
    Ok(())
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
        let declarations: Vec<Declaration> = declarations.into_iter().collect();
        let mut facets: Vec<FacetDecl> = Vec::new();
        let mut types: Vec<TypeDecl> = Vec::new();
        // Two passes: a union member's own declaration and the definition a member-using facet's
        // schema hoists for it name the same type, and the hoisted copy is a `$ref` target without
        // the facet's document frame (`$schema`, `title`). Declaration order is arbitrary, so the
        // facets are all known before the types are screened against them.
        for declaration in &declarations {
            facets.push(declaration.facet.clone());
        }
        for declaration in declarations {
            for decl in declaration.types {
                let facet_agrees = facets.iter().any(|facet| {
                    facet.name == decl.name && same_schema(&facet.schema, &decl.schema)
                });
                let schema_frame = |document: Value| {
                    let mut document = document;
                    if let Value::Object(object) = &mut document {
                        object.remove("$schema");
                        object.remove("title");
                    }
                    document
                };
                let framed_agrees = facets.iter().any(|facet| {
                    facet.name == decl.name
                        && schema_frame(serde_json::to_value(&facet.schema).expect("a schema"))
                            == serde_json::to_value(&decl.schema).expect("a schema")
                });
                if facet_agrees || framed_agrees || declaration.facet.name == decl.name {
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
    /// A union member's definition as the member declares it. The overlay of a union position
    /// matches a supplement branch by its discriminant and applies the branch — which is this
    /// definition with the discriminant added — to the member's compiled definition; when the
    /// whole union schema was handed over instead, every branch paired to the same definition.
    pub fn member_definition(&self, name: &str) -> Option<&Schema> {
        self.resolve(name)
    }

    pub fn document(&self, facet_name: &str) -> Option<Schema> {
        let defs = self.defs_in_scope();
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

    /// Every Lexicon file this bundle publishes, one per facet, in the order the plug declares them.
    ///
    /// Projected against the whole bundle rather than against one declaration, so a definition a
    /// facet reaches resolves here the same way it does in [`Bundle::document`] — including one
    /// carried by a dependency plug. This is the publish step, and it is where a declaration that
    /// was [`assemble`d](Declaration::assemble) without the alignment gate is refused instead of
    /// being handed to a peer that cannot read it.
    pub fn lexicons(&self) -> Result<Vec<Lexicon>, LexiconError> {
        let defs = Value::Object(self.defs_in_scope());
        self.plug
            .facets
            .iter()
            .map(|facet| {
                // A declaration is checked against its own schema at authoring time, but the union
                // tagging it settled on is only a decision recorded in `flags`: refuse to publish a
                // facet whose union a consumer cannot read as an internal `$type` union.
                if facet.flags.contains(&ShapeFlag::NonInternalTagging) {
                    return Err(LexiconError::NotAligned {
                        id: facet.id.clone(),
                        reason: "only an internally tagged union is a lexicon union".to_owned(),
                    });
                }
                let mut document =
                    serde_json::to_value(&facet.schema).expect("a schema is a JSON value");
                document
                    .as_object_mut()
                    .expect("a schema is an object")
                    .insert("$defs".to_owned(), defs.clone());
                let file = project_facet(&facet.id, &document).map_err(|reason| {
                    LexiconError::NotAligned {
                        id: facet.id.clone(),
                        reason,
                    }
                })?;
                Ok(Lexicon {
                    id: facet.id.clone(),
                    file,
                })
            })
            .collect()
    }

    /// Every definition in scope, flattened, nearest plug first: the context a reference resolves in.
    fn defs_in_scope(&self) -> Map<String, Value> {
        let mut defs = Map::new();
        for (name, schema, _owner) in self.definitions() {
            defs.entry(name.to_owned())
                .or_insert_with(|| serde_json::to_value(schema).expect("a schema is a JSON value"));
        }
        defs
    }
}

/// One facet's published Lexicon: the NSID it is published under, and the file itself.
#[derive(Debug, Clone)]
pub struct Lexicon {
    pub id: String,
    pub file: Value,
}

/// A facet this bundle cannot publish a Lexicon for.
#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum LexiconError {
    /// {id} is not lexicon-aligned: {reason}
    NotAligned { id: String, reason: String },
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

/// Every `#/$defs/<name>` a schema points at, in schema positions.
///
/// JSON values used by `const`, `default`, `enum`, and Cabinet data metadata
/// are literals, not schemas. In particular, a property whose name is `$ref`
/// is still a property schema and is visited through the `properties` map.
fn references(schema: &Schema) -> Vec<String> {
    let document = serde_json::to_value(schema).expect("a schema serializes to JSON");
    let mut out = Vec::new();
    collect_schema_references(&document, &mut out);
    out
}

fn collect_schema_references(value: &Value, out: &mut Vec<String>) {
    let Value::Object(object) = value else {
        return;
    };
    if let Some(target) = object
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|it| it.strip_prefix("#/$defs/"))
    {
        out.push(target.to_owned());
    }
    for (key, nested) in object {
        match key.as_str() {
            "$defs" | "definitions" | "properties" | "patternProperties" | "dependentSchemas" => {
                let Some(schemas) = nested.as_object() else {
                    continue;
                };
                for schema in schemas.values() {
                    collect_schema_references(schema, out);
                }
            }
            "additionalProperties"
            | "additionalItems"
            | "propertyNames"
            | "contains"
            | "contentSchema"
            | "not"
            | "if"
            | "then"
            | "else"
            | "unevaluatedProperties"
            | "unevaluatedItems"
            | "items" => collect_schema_references(nested, out),
            "prefixItems" | "allOf" | "anyOf" | "oneOf" => {
                let Some(schemas) = nested.as_array() else {
                    continue;
                };
                for schema in schemas {
                    collect_schema_references(schema, out);
                }
            }
            _ => {}
        }
    }
}

fn same_schema(one: &Schema, two: &Schema) -> bool {
    serde_json::to_value(one).ok() == serde_json::to_value(two).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn universe_with_schema(schema: Value, types: Vec<TypeDecl>) -> Universe {
        Universe {
            facets: vec![FacetDecl {
                id: "org.example.schema.test".to_owned(),
                name: "Main".to_owned(),
                schema: Schema::try_from(schema).expect("test schema"),
                flags: Vec::new(),
            }],
            types,
        }
    }

    fn carried_type(name: &str) -> TypeDecl {
        TypeDecl {
            name: name.to_owned(),
            schema: Schema::try_from(json!({"type": "string"})).expect("carried schema"),
        }
    }

    #[test]
    fn validation_walks_schema_positions_not_literal_values() {
        let schema = json!({
            "type": "object",
            "properties": {
                "$ref": {"$ref": "#/$defs/Carried"}
            },
            "const": {"$ref": "#/$defs/NotCarried"},
            "default": {"$ref": "#/$defs/NotCarried"},
            "enum": [{"$ref": "#/$defs/NotCarried"}],
            "data": {"$ref": "#/$defs/NotCarried"}
        });
        let universe = universe_with_schema(schema, vec![carried_type("Carried")]);
        Bundle::new(&universe, Vec::new())
            .validate()
            .expect("literal values are not schema references");
    }

    #[test]
    fn validation_rejects_missing_reference_in_a_schema_position() {
        let schema = json!({
            "type": "object",
            "properties": {"value": {"$ref": "#/$defs/NotCarried"}}
        });
        let universe = universe_with_schema(schema, Vec::new());
        assert!(matches!(
            Bundle::new(&universe, Vec::new()).validate(),
            Err(UniverseError::Undeclared { name, .. }) if name == "NotCarried"
        ));
    }
}
