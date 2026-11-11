use super::ActorMetadata;
use super::DatumMetadata;
use super::Dmeta;
use super::DocId;
use super::Uuid7;
use crate::DatumKey;
use crate::datum::CodecError;
use crate::datum::CompiledSchema;
use crate::datum::hydrate_value;
use crate::datum::reconcile_json_atomic;
use automerge::ObjId;
use automerge::ObjType;
use automerge::ReadDoc;
use automerge::ScalarValue;
use automerge::transaction::Transactable;
use automerge::transaction::Transaction;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error(transparent)]
    Codec(Box<CodecError>),
    #[error(transparent)]
    Hydrate(#[from] autosurgeon::HydrateError),
    #[error(transparent)]
    Automerge(#[from] automerge::AutomergeError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("immutable {index} binding {uuid} is already bound to something else")]
    ReboundBinding { index: &'static str, uuid: String },
    #[error("expected {expected:?} at {object}/{prop:?}")]
    UnexpectedStorage {
        object: ObjId,
        prop: automerge::Prop,
        expected: ObjType,
    },
}

impl From<CodecError> for StorageError {
    fn from(error: CodecError) -> Self {
        Self::Codec(Box::new(error))
    }
}

/// Dmeta's schema, and the member schemas of the parts one write replaces.
///
/// A write validates and reconciles only what it changes, each against the declaration of its own
/// position, so those member schemas are compiled once alongside dmeta's rather than on every
/// write.
#[derive(Debug, Clone)]
pub struct DmetaSchema {
    root: CompiledSchema,
    created_at: CompiledSchema,
    updated_at: CompiledSchema,
    datum: CompiledSchema,
    actor: CompiledSchema,
}

impl DmetaSchema {
    pub fn new(root: CompiledSchema) -> Result<Self, CodecError> {
        Ok(Self {
            created_at: root.member(&["createdAt"])?,
            updated_at: root.member(&["updatedAt"])?,
            // `datums` and `actors` are keyed maps: every member shares one declaration.
            datum: root.member(&["datums", ""])?,
            actor: root.member(&["actors", ""])?,
            root,
        })
    }

    /// The schema of dmeta as a whole, which hydration reads against.
    pub fn root(&self) -> &CompiledSchema {
        &self.root
    }
}

/// What one write changes in dmeta. Everything else stored there is left exactly as it is: not
/// re-validated, not reconciled, and not rewritten.
#[derive(Debug, Clone, Copy)]
pub struct DmetaWrite<'a> {
    /// The datums whose metadata the write replaces, signatures included, together with the
    /// aliases this dmeta binds to them.
    pub datums: &'a BTreeSet<DatumKey>,
    /// The writing actor, bound if the document does not hold it yet.
    pub actor: Uuid7,
    /// The `docUuid` the write's signatures carry, bound if the document does not hold it yet.
    pub doc_uuid: Uuid7,
}

/// What one write reads of the stored dmeta before it decides what to write; the read-side twin of
/// [`DmetaWrite`].
#[derive(Debug, Clone, Copy)]
pub struct DmetaRead<'a> {
    /// The datums the write touches: their metadata and the aliases bound to them are read.
    pub datums: &'a BTreeSet<DatumKey>,
    /// The writing actor, whose binding is read if the document holds it.
    pub actor: Uuid7,
    /// The document the write is made on, whose `docUuid` bindings are read.
    pub document: &'a DocId,
}

impl Dmeta {
    /// Hydrate only the parts of the dmeta stored at `object`/`prop` that `read` names, leaving
    /// every other field of the result at its empty value.
    ///
    /// What comes back is: `createdAt`; the `docUuids` bindings that name `read.document`; the
    /// each touched datum's metadata, each hydrated against its own member schema, with `uuids`
    /// unioned across every concurrent copy of the datum's entry; and the writing actor's binding. That is exactly
    /// what a write needs to pick its aliases and `docUuid`, to carry the touched datums' `extra`
    /// forward and to see whether its actor is already bound, and exactly what
    /// [`Dmeta::write_atomic`] then writes back, so a write's read cost does not depend on how many
    /// datums the document holds in its metadata.
    ///
    /// A datum's aliases live under its own entry (`datums[key].uuids`), so finding them is a read
    /// of that entry, whatever the number of datums the document holds.
    ///
    /// Like [`Dmeta::hydrate`], a conflicted container is read through its winning copy.
    pub fn hydrate_for<D: ReadDoc>(
        doc: &D,
        object: &ObjId,
        prop: autosurgeon::Prop<'_>,
        schema: &DmetaSchema,
        read: &DmetaRead<'_>,
    ) -> Result<Self, StorageError> {
        let native = native_prop(&prop);
        let Some((automerge::Value::Object(ObjType::Map), map)) =
            doc.get(object, native.clone())?
        else {
            return Err(StorageError::UnexpectedStorage {
                object: object.clone(),
                prop: native,
                expected: ObjType::Map,
            });
        };
        let created_at = serde_json::from_value(hydrate_value(
            doc,
            &map,
            "createdAt".into(),
            &schema.created_at,
        )?)?;
        let mut dmeta = Dmeta::new(created_at);
        let document = read.document.to_string();
        if let Some((_, bindings)) = doc.get(&map, "docUuids")? {
            for binding in doc.map_range(&bindings, ..) {
                if value_str(&binding.value) == Some(document.as_str()) {
                    dmeta
                        .doc_uuids
                        .insert(uuid7(&binding.key)?, read.document.clone());
                }
            }
        }
        if let Some((_, datums)) = doc.get(&map, "datums")? {
            for key in read.datums {
                let name = key.to_string();
                if doc.get(&datums, name.as_str())?.is_some() {
                    let json = hydrate_value(doc, &datums, name.as_str().into(), &schema.datum)?;
                    let mut metadata: DatumMetadata = serde_json::from_value(json)?;
                    metadata.uuids = datum_aliases(doc, object, native.clone(), &name)?
                        .into_iter()
                        .map(|uuid| (uuid, true))
                        .collect();
                    dmeta.datums.insert(key.clone(), metadata);
                }
            }
        }
        if let Some((_, actors)) = doc.get(&map, "actors")? {
            let name = read.actor.to_string();
            if doc.get(&actors, name.as_str())?.is_some() {
                let json = hydrate_value(doc, &actors, name.as_str().into(), &schema.actor)?;
                dmeta
                    .actors
                    .insert(read.actor, serde_json::from_value(json)?);
            }
        }
        Ok(dmeta)
    }

    /// Hydrate dmeta, including actor creation timestamps held by non-winning concurrent maps.
    /// Ordinary map hydration selects one actor binding; `createdAt` is instead a union across every
    /// copy because each independent introduction contributed one timestamp to that binding.
    pub fn hydrate<D: ReadDoc>(
        doc: &D,
        object: &ObjId,
        prop: autosurgeon::Prop<'_>,
        schema: &CompiledSchema,
    ) -> Result<Self, StorageError> {
        let mut json = hydrate_value(doc, object, prop.clone(), schema)?;
        for (uuid, timestamps) in actor_created_at(doc, object, prop.clone())? {
            let Some(created_at) = json
                .get_mut("actors")
                .and_then(|actors| actors.get_mut(&uuid))
                .and_then(|actor| actor.get_mut("createdAt"))
                .and_then(Value::as_array_mut)
            else {
                continue;
            };
            *created_at = timestamps
                .into_iter()
                .map(|millis| {
                    Value::String(
                        jiff::Timestamp::from_millisecond(millis)
                            .expect("a timestamp that a document holds is in range")
                            .to_string(),
                    )
                })
                .collect();
        }
        let mut dmeta: Dmeta = serde_json::from_value(json)?;
        // The winning copy of a datum's entry shows only its own aliases; the rest are unioned in.
        for (key, metadata) in &mut dmeta.datums {
            metadata.uuids = datum_aliases(doc, object, native_prop(&prop), &key.to_string())?
                .into_iter()
                .map(|uuid| (uuid, true))
                .collect();
        }
        Ok(dmeta)
    }

    /// Write the parts of this dmeta that `write` names over the dmeta stored at `object`/`prop`,
    /// preserving the identity of every signature entry it retains.
    ///
    /// Only the named parts are validated and reconciled, each against its own member schema:
    /// dmeta's `updatedAt`, each named datum's metadata, and the bindings the write needs (the
    /// actor, the `docUuid`, and the named datums' aliases). A binding is immutable, so one already
    /// stored is checked against this dmeta's and never rewritten, and one this dmeta adds must not
    /// collide with what any concurrent copy of the index holds. The rest of the stored dmeta is
    /// untouched, so it cannot be removed or rebound by a write that did not name it.
    ///
    /// A named datum's competing signature set is replaced outright. The typed value only sees the
    /// winning copy of a conflicted container, but a concurrent first write of a slot's metadata
    /// leaves two metadata maps (or two signature lists), and the losing copy's entries are still
    /// current signatures; the write retires those too. Datums it does not name keep their losing
    /// copies exactly as they are, since this write never saw them.
    ///
    /// A document with no dmeta yet first gets what no write names, `createdAt` and any members
    /// this declaration does not know, through the codec against the whole dmeta schema; the
    /// named parts then go in as in any other write.
    pub fn write_atomic<'a>(
        &self,
        mut tx: Transaction<'a>,
        schema: &DmetaSchema,
        object: &ObjId,
        prop: autosurgeon::Prop<'_>,
        write: &DmetaWrite<'_>,
    ) -> Result<Transaction<'a>, StorageError> {
        let native = native_prop(&prop);
        match tx.get(object, native.clone()) {
            Ok(Some(_)) => {}
            Ok(None) => {
                let skeleton = Dmeta {
                    extra: self.extra.clone(),
                    ..Dmeta::new(self.created_at)
                };
                tx = reconcile_json_atomic(
                    tx,
                    &schema.root,
                    object,
                    prop.clone(),
                    serde_json::to_value(skeleton).expect("dmeta serializes to JSON"),
                )?;
            }
            Err(error) => {
                tx.rollback();
                return Err(error.into());
            }
        }
        let actor = &self.actors[&write.actor];
        let prepared = (|| -> Result<_, StorageError> {
            let copies = maps(&tx, object, native.clone())?;
            let map = ensure_object(&mut tx, object, native.clone(), ObjType::Map)?;
            let doc_uuid = self.doc_uuids[&write.doc_uuid].to_string();
            bind_string(
                &mut tx,
                &copies,
                &map,
                "docUuids",
                write.doc_uuid,
                &doc_uuid,
            )?;
            let actors = ensure_object(&mut tx, &map, "actors".into(), ObjType::Map)?;
            let new_actor = !bound(&tx, &copies, &actors, "actors", write.actor, |held, id| {
                actor_matches(&tx, held, id, actor)
            })?;
            let mut updated_at =
                serde_json::to_value(&self.updated_at).expect("timestamps serialize to JSON");
            let base = tx
                .get(&map, "updatedAt")?
                .map(|_| hydrate_value(&tx, &map, "updatedAt".into(), &schema.updated_at))
                .transpose()?;
            reconcile_set(&mut tx, &map, "updatedAt", &mut updated_at, base.as_ref())?;
            let datums = ensure_object(&mut tx, &map, "datums".into(), ObjType::Map)?;
            let mut entries = Vec::with_capacity(write.datums.len());
            for datum_key in write.datums {
                let key = datum_key.to_string();
                let mut submitted = serde_json::to_value(&self.datums[datum_key])
                    .expect("datum metadata serializes to JSON");
                let base = tx
                    .get(&datums, key.as_str())?
                    .map(|_| hydrate_value(&tx, &datums, key.as_str().into(), &schema.datum))
                    .transpose()?;
                let datum = ensure_object(&mut tx, &datums, key.as_str().into(), ObjType::Map)?;
                retire_losing_signatures(&mut tx, &datums, &key, &datum)?;
                // Aliases are immutable and a write only adds: one held by any copy of the
                // entry (under any copy of `datums`) stays where it is, and the rest go into the
                // winning copy's `uuids`. `submitted` then names exactly what the winning copy
                // holds, so reconciling the entry below neither removes an alias nor copies a
                // losing copy's into it.
                let held = datum_aliases(&tx, object, native.clone(), &key)?;
                let uuids = ensure_object(&mut tx, &datum, "uuids".into(), ObjType::Map)?;
                for uuid in self.datums[datum_key].uuids.keys() {
                    if !held.contains(uuid) {
                        tx.put(&uuids, uuid.to_string(), true)?;
                    }
                }
                submitted["uuids"] = Value::Object(
                    tx.map_range(&uuids, ..)
                        .map(|binding| (binding.key.to_string(), Value::Bool(true)))
                        .collect(),
                );
                for field in ["updatedAt", "deletedAt", "signatures"] {
                    reconcile_set(
                        &mut tx,
                        &datum,
                        field,
                        &mut submitted[field],
                        base.as_ref().and_then(|metadata| metadata.get(field)),
                    )?;
                }
                entries.push((key, submitted));
            }
            Ok((actors, new_actor, datums, entries))
        })();
        let (actors, new_actor, datums, entries) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                tx.rollback();
                return Err(error);
            }
        };
        if new_actor {
            tx = reconcile_json_atomic(
                tx,
                &schema.actor,
                &actors,
                write.actor.to_string().as_str().into(),
                serde_json::to_value(actor).expect("an actor binding serializes to JSON"),
            )?;
        }
        for (key, submitted) in entries {
            tx = reconcile_json_atomic(tx, &schema.datum, &datums, key.as_str().into(), submitted)?;
        }
        Ok(tx)
    }
}

fn value_str<'a>(value: &'a automerge::ValueRef<'_>) -> Option<&'a str> {
    match value {
        automerge::ValueRef::Scalar(automerge::ScalarValueRef::Str(text)) => Some(text.as_ref()),
        _ => None,
    }
}

fn uuid7(text: &str) -> Result<Uuid7, StorageError> {
    Ok(serde_json::from_value(Value::String(text.to_owned()))?)
}

fn native_prop(prop: &autosurgeon::Prop<'_>) -> automerge::Prop {
    match prop {
        autosurgeon::Prop::Key(key) => automerge::Prop::Map(key.to_string()),
        autosurgeon::Prop::Index(index) => automerge::Prop::Seq(*index as usize),
    }
}

fn actor_created_at<D: ReadDoc>(
    doc: &D,
    object: &ObjId,
    prop: autosurgeon::Prop<'_>,
) -> Result<BTreeMap<String, BTreeSet<i64>>, StorageError> {
    let native = native_prop(&prop);
    let mut created_at = BTreeMap::new();
    for (value, map) in doc.get_all(object, native.clone())? {
        if value != automerge::Value::Object(ObjType::Map) {
            return Err(StorageError::UnexpectedStorage {
                object: object.clone(),
                prop: native,
                expected: ObjType::Map,
            });
        }
        for (value, actors) in doc.get_all(&map, "actors")? {
            if value != automerge::Value::Object(ObjType::Map) {
                return Err(StorageError::UnexpectedStorage {
                    object: map.clone(),
                    prop: "actors".into(),
                    expected: ObjType::Map,
                });
            }
            for binding in doc.map_range(&actors, ..) {
                let uuid = binding.key.to_string();
                let timestamps = created_at.entry(uuid.clone()).or_insert_with(BTreeSet::new);
                for (value, actor) in doc.get_all(&actors, binding.key.as_ref())? {
                    if value != automerge::Value::Object(ObjType::Map) {
                        return Err(StorageError::UnexpectedStorage {
                            object: actors.clone(),
                            prop: binding.key.as_ref().into(),
                            expected: ObjType::Map,
                        });
                    }
                    for (value, list) in doc.get_all(&actor, "createdAt")? {
                        if value != automerge::Value::Object(ObjType::List) {
                            return Err(StorageError::UnexpectedStorage {
                                object: actor.clone(),
                                prop: "createdAt".into(),
                                expected: ObjType::List,
                            });
                        }
                        for index in 0..doc.length(&list) {
                            for (value, _) in doc.get_all(&list, index)? {
                                if let Some(ScalarValue::Timestamp(millis)) = value.to_scalar() {
                                    timestamps.insert(*millis);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(created_at)
}

/// The aliases bound to the datum at `key`: the union of `uuids` across every copy of dmeta at
/// `object`/`prop`, every copy of `datums` in each, and every copy of the datum's entry in each.
/// Concurrent creators of a datum, or of the containers above it, leave several copies and
/// Automerge shows one, so a reader must not stop at the visible one.
///
/// The writer's choice of alias (here, through `hydrate_for` and `write_atomic`) and the verifier's
/// reconstruction (`Reconstruction::datum_metas` in big_cabinet's `claim_history`) must read this
/// same union: if either saw a narrower set, the writer could choose an alias that is not the
/// smallest the verifier reconstructs and a genuine signature would be rejected.
fn datum_aliases<D: ReadDoc>(
    doc: &D,
    object: &ObjId,
    prop: automerge::Prop,
    key: &str,
) -> Result<BTreeSet<Uuid7>, StorageError> {
    let mut aliases = BTreeSet::new();
    for (_, dmeta) in doc.get_all(object, prop)? {
        for (_, datums) in doc.get_all(&dmeta, "datums")? {
            for (_, entry) in doc.get_all(&datums, key)? {
                for (_, index) in doc.get_all(&entry, "uuids")? {
                    for binding in doc.map_range(&index, ..) {
                        aliases.insert(uuid7(&binding.key)?);
                    }
                }
            }
        }
    }
    Ok(aliases)
}

/// Empty every signature list under `key` other than the winning list of the winning metadata map.
fn retire_losing_signatures(
    tx: &mut Transaction<'_>,
    datums: &ObjId,
    key: &str,
    winner: &ObjId,
) -> Result<(), StorageError> {
    let winning_list = match tx.get(winner, "signatures")? {
        Some((automerge::Value::Object(ObjType::List), list)) => Some(list),
        _ => None,
    };
    let mut losing = Vec::new();
    for (_, metadata) in tx.get_all(datums, key)? {
        for (value, list) in tx.get_all(&metadata, "signatures")? {
            if value == automerge::Value::Object(ObjType::List)
                && Some(&list) != winning_list.as_ref()
            {
                losing.push(list);
            }
        }
    }
    for list in losing {
        for index in (0..tx.length(&list)).rev() {
            tx.delete(&list, index)?;
        }
    }
    Ok(())
}

fn ensure_object(
    tx: &mut Transaction<'_>,
    object: &ObjId,
    prop: automerge::Prop,
    expected: ObjType,
) -> Result<ObjId, StorageError> {
    match tx.get(object, prop.clone())? {
        Some((automerge::Value::Object(kind), id)) if kind == expected => Ok(id),
        Some(_) => Err(StorageError::UnexpectedStorage {
            object: object.clone(),
            prop,
            expected,
        }),
        None => Ok(tx.put_object(object, prop, expected)?),
    }
}

/// Every copy of the map at `object`/`prop`: concurrent creations of one container leave several,
/// and a binding written into one is visible through whichever wins.
fn maps(
    tx: &Transaction<'_>,
    object: &ObjId,
    prop: automerge::Prop,
) -> Result<Vec<ObjId>, StorageError> {
    tx.get_all(object, prop.clone())?
        .into_iter()
        .map(|(value, id)| match value {
            automerge::Value::Object(ObjType::Map) => Ok(id),
            _ => Err(StorageError::UnexpectedStorage {
                object: object.clone(),
                prop: prop.clone(),
                expected: ObjType::Map,
            }),
        })
        .collect()
}

/// Whether the winning `index` map already binds `uuid`, after checking that every value any copy
/// of dmeta binds it to is what `matches` accepts. A binding is immutable: the write must neither
/// change one that is stored nor add one that a concurrent copy holds differently, since that copy
/// would win or lose by Automerge's rules rather than by what was bound first.
fn bound(
    tx: &Transaction<'_>,
    copies: &[ObjId],
    winner: &ObjId,
    index: &'static str,
    uuid: Uuid7,
    matches: impl Fn(&automerge::Value<'_>, &ObjId) -> Result<bool, StorageError>,
) -> Result<bool, StorageError> {
    let uuid = uuid.to_string();
    for copy in copies {
        for bindings in maps(tx, copy, index.into())? {
            for (value, id) in tx.get_all(&bindings, uuid.as_str())? {
                if !matches(&value, &id)? {
                    return Err(StorageError::ReboundBinding { index, uuid });
                }
            }
        }
    }
    Ok(tx.get(winner, uuid.as_str())?.is_some())
}

/// Bind `uuid` to `wanted` in one of the string indexes (`docUuids`).
fn bind_string(
    tx: &mut Transaction<'_>,
    copies: &[ObjId],
    map: &ObjId,
    index: &'static str,
    uuid: Uuid7,
    wanted: &str,
) -> Result<(), StorageError> {
    let bindings = ensure_object(tx, map, index.into(), ObjType::Map)?;
    let held = bound(tx, copies, &bindings, index, uuid, |value, _| {
        Ok(value.to_str() == Some(wanted))
    })?;
    if !held {
        tx.put(&bindings, uuid.to_string(), wanted)?;
    }
    Ok(())
}

/// Whether a stored actor binding is `actor`'s: its key, URL and certificate. `createdAt` is display
/// metadata every concurrent introduction contributes to, not part of the binding.
fn actor_matches(
    tx: &Transaction<'_>,
    held: &automerge::Value<'_>,
    id: &ObjId,
    actor: &ActorMetadata,
) -> Result<bool, StorageError> {
    if *held != automerge::Value::Object(ObjType::Map) {
        return Ok(false);
    }
    let fields = [
        (
            "pubkey",
            Some(ScalarValue::Str(actor.pubkey.to_string().into())),
        ),
        (
            "actorUrl",
            Some(ScalarValue::Str(actor.actor_url.as_str().into())),
        ),
        (
            "delegation",
            actor
                .delegation
                .as_ref()
                .map(|delegation| ScalarValue::Bytes(delegation.as_slice().to_vec())),
        ),
    ];
    for (key, expected) in fields {
        let held = tx.get_all(id, key)?;
        let matched = match expected {
            None => held.is_empty(),
            Some(expected) => {
                !held.is_empty()
                    && held
                        .iter()
                        .all(|(value, _)| value.to_scalar() == Some(&expected))
            }
        };
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

fn reconcile_set(
    tx: &mut Transaction<'_>,
    object: &ObjId,
    field: &'static str,
    submitted: &mut Value,
    base: Option<&Value>,
) -> Result<(), StorageError> {
    let list = ensure_object(tx, object, field.into(), ObjType::List)?;
    let mut wanted: BTreeMap<String, (usize, Value)> = BTreeMap::new();
    let Value::Array(values) = std::mem::take(submitted) else {
        unreachable!("dmeta sets are arrays");
    };
    for mut value in values {
        if field != "signatures" {
            let instant: jiff::Timestamp = value
                .as_str()
                .expect("a timestamp is a string")
                .parse()
                .expect("a typed timestamp parses");
            value = Value::String(
                jiff::Timestamp::from_millisecond(instant.as_millisecond())
                    .expect("a timestamp's milliseconds are in range")
                    .to_string(),
            );
        }
        let key = serde_json::to_string(&value).expect("a set value serializes");
        let entry = wanted.entry(key).or_insert((0, value));
        if field == "signatures" || entry.0 == 0 {
            entry.0 += 1;
        }
    }
    let observed = match base {
        Some(Value::Array(values)) => values.as_slice(),
        None => &[],
        Some(_) => {
            return Err(StorageError::UnexpectedStorage {
                object: object.clone(),
                prop: field.into(),
                expected: ObjType::List,
            });
        }
    };
    assert_eq!(tx.length(&list), observed.len());
    let mut retained = Vec::with_capacity(wanted.len());
    let mut removed = Vec::new();
    let mut matched = BTreeSet::new();
    for (index, value) in observed.iter().enumerate() {
        let identity = serde_json::to_string(value).expect("an observed set value serializes");
        if let Some((remaining, _)) = wanted.get_mut(&identity)
            && *remaining > 0
        {
            *remaining -= 1;
            matched.insert(identity);
            retained.push(value.clone());
        } else {
            removed.push(index);
        }
    }
    for index in removed.into_iter().rev() {
        tx.delete(&list, index)?;
    }
    for (identity, (remaining, value)) in wanted {
        if remaining == 0 || matched.contains(&identity) {
            continue;
        }
        if field == "signatures" {
            tx.insert_object(&list, retained.len(), ObjType::Map)?;
        } else {
            let instant: jiff::Timestamp = value
                .as_str()
                .expect("a timestamp is a string")
                .parse()
                .expect("a normalized timestamp parses");
            tx.insert(
                &list,
                retained.len(),
                ScalarValue::Timestamp(instant.as_millisecond()),
            )?;
        }
        retained.push(value);
    }
    *submitted = Value::Array(retained);
    Ok(())
}

#[cfg(test)]
mod tests;
