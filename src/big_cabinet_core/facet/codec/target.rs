//! The target-aware half of the codec: a submitted value checked against the native storage that
//! already holds the position it goes into, not only against its declarations.
//!
//! [`Stored::check`](super::Stored::check) asserts what a value looks like before anything is
//! written. This adds the second check a write inside a document needs, against the base view a
//! transaction was opened at, and the two checks order the same way the contract does: where the
//! position declares, the declaration is what wins — the representation its write produces is the
//! one the position keeps, whatever the native storage underneath holds. Where the position
//! declares nothing, the storage it already holds decides: a text diffs, a counter takes the
//! delta it needs, bytes and an instant take their spellings, and an unchanged submission is not
//! written at all, so the native value keeps its object and the conflicts that converge on it.
//!
//! The plan of decisions is consumed by [`Stored::reconcile`](super::Stored::reconcile), which is
//! the same write the codec always used, so the interpretation is shared: resolution, tag dispatch
//! and the codec's conversions live in one place, and this walk is a traversal rather than a second
//! decoder.
//!
//! ## The base view
//!
//! `reconcile_json` performs none of the heads handling itself. The caller builds the transaction
//! it writes inside, and the contract — omission removes at the *supplied* base heads, `AtCurrent`
//! is an explicit choice, no retries — is met by building that transaction at the heads the write
//! is based on (`transaction_at(&heads)`), since every read here sees that view. A transaction
//! built at current heads checks a different contract. On an error the caller owns the
//! transaction's pending effects: the checks that need converting are made before a mutation, an
//! error raised while writing propagates unchanged, and the write's operations are not promised
//! atomic across them.

use super::walk::{
    Declared, Kind, Number, Path, PathKey, Position, at, discriminant_matches, number_kind,
    type_mentions,
};
use super::{Codec, CodecError, decode_base64, instant_millis, kind};
use automerge::{ObjId, ObjType, ScalarValue, transaction::Transactable};
use autosurgeon::reconcile::{CounterReconciler, TextReconciler};
use autosurgeon::{Prop, ReadDoc as AutoDoc, Reconciler};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;

/// Reconcile a JSON value at a position of a document, against both its declarations and the
/// native storage that holds it.
///
/// The `doc` is the transaction the write happens in; build it at the base heads the write is
/// based on (`transaction_at(&heads)`), because the checks here — like the write itself — read
/// that view. The result is the value a read-modify-write commits: unchanged content wrote
/// nothing, so what the base kept is what a later read sees, in its native storage.
///
/// What this does not decide:
///
/// - Lists are matched positionally, the mechanism the write always used; an index write is not
///   identity-safe, and a keyed correspondence for submitted lists is its own open question.
/// - Content at a position nothing declares and no base holds writes plain JSON: what native
///   storage fresh unknown content should take is its own open question.
pub fn reconcile_json<D>(
    doc: &mut D,
    obj: &ObjId,
    prop: Prop<'_>,
    document: &Value,
    value: Value,
) -> Result<(), CodecError>
where
    D: AutoDoc,
    D: Transactable,
{
    let path = Path::root();
    let position = Position::new(document, Some(document));
    // Declarations first, exactly as `Stored::check` does them: a value the declarations cannot
    // hold is refused before the document is read for its position at all.
    position
        .check(&value, &path)
        .map_err(|err| at(&path, err))?;
    let mut plan = Plan::default();
    checked(&position, doc, obj, &prop, &value, &path, &mut plan).map_err(|err| at(&path, err))?;
    let stored = super::Stored::with_plan(document, value, plan);
    // The write the codec always made, now carrying the decisions the base was read into; a
    // document refusing an operation on it propagates to the caller that owns the transaction.
    autosurgeon::reconcile_prop(doc, obj, prop, stored).map_err(CodecError::from)
}

/// The decisions a checked write consults while it writes, keyed by the path they were checked
/// at. Both passes render a position's chain the same way, which is why an entry is found where
/// it was checked.
#[derive(Debug, Default)]
pub(super) struct Plan {
    decisions: HashMap<PathKey, Op>,
}

impl Plan {
    /// The decision made at this position, if the base was read for it. The structural key is
    /// what makes an entry found only where it was checked, whatever a member is named.
    pub(super) fn get(&self, path: &Path<'_>) -> Option<&Op> {
        self.decisions.get(&path.structural())
    }

    /// Record the decision made at a position.
    fn set(&mut self, path: &Path<'_>, op: Op) {
        self.decisions.insert(path.structural(), op);
    }
}

/// The one decision a write makes at a position the base holds, as it was checked against the
/// base view. Conversions recorded here were checked before the write that uses them.
#[derive(Debug)]
pub(super) enum Op {
    /// The submitted value is the one the base holds: nothing is written, so the native value
    /// keeps its object and the conflicts that converge on it.
    Skip,
    /// Diff the native text the position holds to this string, the machinery a declared text
    /// writes through; an identical text is no ops and the object is never replaced.
    Text(String),
    /// Add the checked delta to the counter the position holds, rather than putting a fresh one
    /// over what concurrent writers incremented after the base. For counters the position
    /// declares nothing about, this is also the fallback's conversion.
    Delta(i64),
    /// Put this scalar, spelled in the kind the storage the position already holds.
    Scalar(Spelled),
}

/// A scalar spelled the way the kind of storage it will sit in spells things.
#[derive(Debug, Clone)]
pub(super) enum Spelled {
    Bytes(Vec<u8>),
    Timestamp(i64),
    I64(i64),
    F64(f64),
}

impl Op {
    /// Write a checked decision through the reconciler a position's write was handed.
    pub(super) fn write<R: Reconciler>(&self, mut reconciler: R) -> Result<(), R::Error> {
        match self {
            Op::Skip => Ok(()),
            Op::Text(stated) => reconciler.text()?.update(stated),
            Op::Delta(delta) => reconciler.counter()?.increment(*delta),
            Op::Scalar(spelled) => match spelled {
                // spelled in the kind the position already holds: an integer stays an integer,
                // bytes stay bytes, and a text's diff is what keeps it a text
                Spelled::Bytes(value) => reconciler.bytes(value),
                Spelled::Timestamp(millis) => reconciler.timestamp(*millis),
                Spelled::I64(value) => reconciler.i64(*value),
                Spelled::F64(value) => reconciler.f64(*value),
            },
        }
    }
}

/// Whether the base's scalar is the kind this position's declaration produces, which is what a
/// skip requires: a native kind the declaration does not spell is changed content, however equal
/// the values, and the declaration's write converges it instead. What the declaration IS is
/// [`Declared`]'s business, and the plain markers and stated kinds are what it says.
fn compatible(scalar: &ScalarValue, declared: Declared, schema: Option<&Value>) -> bool {
    match declared {
        Declared::Codec(Codec::Bytes) => matches!(scalar, ScalarValue::Bytes(_)),
        Declared::Codec(Codec::Timestamp) => matches!(scalar, ScalarValue::Timestamp(_)),
        Declared::Codec(Codec::Counter) => matches!(scalar, ScalarValue::Counter(_)),
        // The one native kind a float's declaration produces is the `F64` its write puts; an
        // integer or an unsigned there is the token-kind drift the declaration fixes.
        Declared::Codec(Codec::Float) => matches!(scalar, ScalarValue::F64(_)),
        // texts are objects, and never reach the scalar decisions
        Declared::Codec(Codec::Text) => false,
        // A plain position keeps only the kind its declaration produces.
        Declared::Plain => {
            if matches!(scalar, ScalarValue::Null) {
                return schema.is_some_and(|stated| type_mentions(stated, "null"));
            }
            match number_kind(schema) {
                Number::Float => matches!(scalar, ScalarValue::F64(_)),
                Number::Signed => matches!(scalar, ScalarValue::Int(_)),
                Number::Unsigned => matches!(scalar, ScalarValue::Uint(_)),
                // What no number kind covers: a string position keeps a Str and a boolean
                // position a boolean; nothing else sits at a stated kind.
                Number::Token => matches!(scalar, ScalarValue::Str(_) | ScalarValue::Boolean(_)),
            }
        }
        // Where nothing is declared at all, the native kind is what nothing declared left, and it
        // is the kind a submission compares against untouched.
        Declared::Undeclared => true,
    }
}

/// Whether the base's scalar and the submitted value are the same value, spelled however each
/// side spells it: a base64 field says the same bytes with and without padding, an instant says
/// the RFC 3339 of its millisecond.
fn same_scalar(scalar: &ScalarValue, value: &Value) -> bool {
    match (scalar, value) {
        (ScalarValue::Null, Value::Null) => true,
        (ScalarValue::Boolean(stated), Value::Bool(submitted)) => stated == submitted,
        (ScalarValue::Int(stated), Value::Number(submitted)) => submitted.as_i64() == Some(*stated),
        (ScalarValue::Uint(stated), Value::Number(submitted)) => {
            submitted.as_u64() == Some(*stated)
        }
        (ScalarValue::F64(stated), Value::Number(submitted)) => submitted
            .as_f64()
            .is_some_and(|submitted| submitted == *stated),
        (ScalarValue::Str(stated), Value::String(submitted)) => submitted == stated.as_str(),
        (ScalarValue::Counter(stated), Value::Number(submitted)) => {
            submitted.as_i64() == Some(i64::from(stated))
        }
        (ScalarValue::Bytes(stated), Value::String(submitted)) => {
            decode_base64(submitted).as_deref() == Some(stated.as_slice())
        }
        (ScalarValue::Timestamp(stated), Value::String(submitted)) => {
            instant_millis(submitted) == Some(*stated)
        }
        _ => false,
    }
}

/// What the base holds at a position and what submitting there means, one node at a time: the
/// deletions an omitted member asks for are the write's own, so this walk decides only what stays
/// (a skip), what changes through native storage (a text diff, a checked counter delta, a spelled
/// scalar) and what the declaration's own write takes.
fn checked<D: AutoDoc>(
    position: &Position<'_>,
    doc: &D,
    obj: &ObjId,
    prop: &Prop<'_>,
    value: &Value,
    path: &Path<'_>,
    plan: &mut Plan,
) -> Result<(), CodecError> {
    let read = |source| CodecError::Automerge { source };
    let Some((stored, obj_id)) = doc.get(obj, prop).map_err(read)? else {
        // Nothing the base holds: a fresh write. The declarations have been checked, and a write
        // into nothing has no native storage to keep anything of.
        return Ok(());
    };
    let schema = resolve(position, value, path)?;
    let declaration = super::walk::declared(schema).map_err(|err| at(path, err))?;
    match (&stored, value) {
        // Children of a map the base holds are checked at their own positions; the keys the value
        // omits are what the put deletes, the correspondence a map write has always taken.
        (automerge::Value::Object(ObjType::Map | ObjType::Table), Value::Object(members)) => {
            for (key, member) in members {
                let child = Path::key(path, key);
                let child_position = position.member(schema, key);
                let child_prop = Prop::Key(Cow::Owned(key.to_owned()));
                checked(
                    &child_position,
                    doc,
                    &obj_id,
                    &child_prop,
                    member,
                    &child,
                    plan,
                )?;
            }
            Ok(())
        }
        // Children of a list the base holds, by position: the same mechanism the write uses, and
        // the limitation documented on the entry point.
        (automerge::Value::Object(ObjType::List), Value::Array(items)) => {
            for (index, item) in items.iter().enumerate() {
                let child = Path::index(path, index);
                let child_position = position.element(schema, index);
                let child_prop = Prop::Index(index as u32);
                checked(
                    &child_position,
                    doc,
                    &obj_id,
                    &child_prop,
                    item,
                    &child,
                    plan,
                )?;
            }
            Ok(())
        }
        // A string submitted where the base holds a text. A text the position declares is one a
        // submission diffs against, so the equal one is a skip and a changed one the codec's own
        // write; nothing declared, the storage is the diff the fallback takes; and a plain
        // declaration spells a Str, and the declaration is what the position keeps, equal
        // or changed, so its write replaces the text rather than preserving it.
        (automerge::Value::Object(ObjType::Text), Value::String(stated)) => {
            match declaration {
                Declared::Codec(Codec::Text) => {
                    let text = doc.text(&obj_id).map_err(read)?;
                    if text == *stated {
                        plan.set(path, Op::Skip);
                    }
                    Ok(())
                }
                Declared::Undeclared => {
                    let text = doc.text(&obj_id).map_err(read)?;
                    if text == *stated {
                        plan.set(path, Op::Skip);
                    } else {
                        plan.set(path, Op::Text(stated.to_owned()));
                    }
                    Ok(())
                }
                // the declaration spells something else — a plain Str, another form — and its
                // write is what the position keeps; no plan entry carries it
                _ => Ok(()),
            }
        }
        // A text with a submission of another kind: no spelling of the text fits, so the result
        // is the plain put a kind switch has always been.
        (automerge::Value::Object(ObjType::Text), _) => Ok(()),
        // A scalar decision: skip, delta or refusal, per the kind the base holds.
        (automerge::Value::Scalar(scalar), _) => {
            scalar_checked(scalar, value, declaration, schema, path, plan)
        }
        // Everything else is a plain kind switch — a container over a scalar, one container kind
        // over another — which the write's own paths have always taken.
        _ => Ok(()),
    }
}

/// The scalar decisions: the base holds a scalar at a position whose declaration was checked;
/// what is left is whether the submission is the value it holds, whether the delta it would need
/// is one the counter can arrive at, and what kind a changed value is spelled in.
fn scalar_checked(
    scalar: &ScalarValue,
    value: &Value,
    declared: Declared,
    schema: Option<&Value>,
    path: &Path<'_>,
    plan: &mut Plan,
) -> Result<(), CodecError> {
    // A future Automerge's scalar cannot be compared, projected or spelled without guessing: it
    // is refused wherever it is and whatever is submitted.
    if let ScalarValue::Unknown { .. } = scalar {
        return Err(at(
            path,
            CodecError::Undeclared {
                held: "a scalar this build does not know",
            },
        ));
    }
    // A counter the base holds is changed by the checked delta it needs, and not by the fresh
    // put a `set` would be — at the counters the position declares, and where it declares nothing
    // at all (the fallback's number is the value it sums to). Where the position declares a plain
    // kind, the declaration's integer wins over the counter, equal or changed.
    if let ScalarValue::Counter(held) = scalar {
        let declared_counter = matches!(declared, Declared::Codec(Codec::Counter));
        let nothing_declared = declared == Declared::Undeclared;
        if !declared_counter && !nothing_declared {
            return Ok(());
        }
        let held = i64::from(held);
        let Some(submitted) = value.as_i64() else {
            return Err(at(path, CodecError::NotACounter { found: kind(value) }));
        };
        let delta = submitted.checked_sub(held).ok_or_else(|| {
            at(
                path,
                CodecError::CounterDelta {
                    base: held,
                    submitted,
                },
            )
        })?;
        plan.set(
            path,
            if delta == 0 {
                Op::Skip
            } else {
                Op::Delta(delta)
            },
        );
        return Ok(());
    }
    if compatible(scalar, declared, schema) && same_scalar(scalar, value) {
        plan.set(path, Op::Skip);
        return Ok(());
    }
    match declared {
        // A changed value a declaration produces is the declaration's write, which its checks
        // already made total: the plain kinds included, since their conversion converges what a
        // drift left a kind behind at.
        Declared::Codec(_) | Declared::Plain => Ok(()),
        // The conversions below are the fallback of the positions nothing declares: the storage
        // the base holds is what decides what a changed submission is spelled in.
        Declared::Undeclared => match (scalar, value) {
            // Undeclared native bytes spell the canonical base64 of what they are: a submission
            // that says other bytes changes them, and one that is not bytes is refused.
            (ScalarValue::Bytes(_), _) => match value.as_str().and_then(decode_base64) {
                Some(byte) => {
                    plan.set(path, Op::Scalar(Spelled::Bytes(byte)));
                    Ok(())
                }
                None => Err(at(path, CodecError::NotBytes { found: kind(value) })),
            },
            // And an instant is a submission that parses to the millisecond it must hold.
            (ScalarValue::Timestamp(_), _) => {
                let Some(millis) = value.as_str().and_then(instant_millis) else {
                    return Err(at(
                        path,
                        CodecError::NotAnInstant {
                            stated: kind(value).to_owned(),
                        },
                    ));
                };
                plan.set(path, Op::Scalar(Spelled::Timestamp(millis)));
                Ok(())
            }
            // An integer stays an integer: a non-negative submission would put the token's
            // unsigned kind over it, which is a kind drift rather than the change, and a value
            // too big for the kind that holds it is refused rather than drifted.
            (ScalarValue::Int(_), Value::Number(submitted)) => match submitted.as_i64() {
                Some(whole) => {
                    plan.set(path, Op::Scalar(Spelled::I64(whole)));
                    Ok(())
                }
                None => Err(at(
                    path,
                    CodecError::Unrepresentable {
                        held: "an integer",
                        found: submitted.to_string(),
                    },
                )),
            },
            // Similarly for a submission that cannot be unsigned.
            (ScalarValue::Uint(_), Value::Number(submitted)) if submitted.as_u64().is_none() => {
                Err(at(
                    path,
                    CodecError::Unrepresentable {
                        held: "a whole number",
                        found: submitted.to_string(),
                    },
                ))
            }
            // A float holds one token of either kind: the integer tokens would put an unsigned
            // over it, and the fractional ones are what a float write puts anyway.
            (ScalarValue::F64(_), Value::Number(submitted)) if submitted.as_u64().is_some() => {
                plan.set(
                    path,
                    Op::Scalar(Spelled::F64(
                        submitted.as_f64().expect("a json number is a number"),
                    )),
                );
                Ok(())
            }
            _ => Ok(()),
        },
    }
}

/// The schema a submitted value resolves to at a position: the same resolution the write consults,
/// so the check that planned and the write that planned it agree about where a codec applies.
fn resolve<'a>(
    position: &Position<'a>,
    value: &Value,
    path: &Path<'_>,
) -> Result<Option<&'a Value>, CodecError> {
    position
        .resolved(
            Kind::of_json(value),
            |key, declaration| {
                Ok(discriminant_matches(
                    value.get(key).and_then(Value::as_str),
                    declaration,
                ))
            },
            |err| err,
        )
        .map_err(|err| at(path, err))
}
