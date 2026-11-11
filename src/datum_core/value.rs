//! The leaf and container impls of [`DatumShape`], and the overflow map of an open object.

use crate::DatumShape;
use crate::Shape;
use indexmap::IndexMap;
use serde_json::Value;

/// The members of an object that its declaration does not name.
///
/// An open object carries one of these as its `#[serde(flatten)]` field, and that field is the only
/// thing that makes `schemars` emit `additionalProperties: true`. An `IndexMap` rather than
/// `serde_json::Map` so a pass-through keeps the order a peer sent.
pub type Extra = IndexMap<String, Value>;

macro_rules! values {
    ($($ty:ty),* $(,)?) => {
        $(impl DatumShape for $ty {
            const SHAPE: Shape = Shape::Value;
        })*
    };
}

values!(
    (),
    bool,
    char,
    str,
    String,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    f32,
    f64,
    Value,
    crate::UnknownMember,
);

macro_rules! containers {
    ($($container:path),* $(,)?) => {
        $(impl<T: DatumShape> DatumShape for $container {
            const SHAPE: Shape = Shape::Value;
        })*
    };
}

containers!(Option<T>, Vec<T>);

impl<T: DatumShape + ?Sized> DatumShape for Box<T> {
    const SHAPE: Shape = Shape::Value;
}

impl<T: DatumShape + ?Sized> DatumShape for std::sync::Arc<T> {
    const SHAPE: Shape = Shape::Value;
}

impl<T: DatumShape + ?Sized> DatumShape for std::rc::Rc<T> {
    const SHAPE: Shape = Shape::Value;
}

impl<T: DatumShape + ?Sized> DatumShape for &T {
    const SHAPE: Shape = Shape::Value;
}

impl<T: DatumShape> DatumShape for [T] {
    const SHAPE: Shape = Shape::Value;
}

impl<T: DatumShape, const N: usize> DatumShape for [T; N] {
    const SHAPE: Shape = Shape::Value;
}

impl<K: DatumShape, V: DatumShape> DatumShape for std::collections::BTreeMap<K, V> {
    const SHAPE: Shape = Shape::Value;
}

impl<K: DatumShape, V: DatumShape, S> DatumShape for std::collections::HashMap<K, V, S> {
    const SHAPE: Shape = Shape::Value;
}

impl<K: DatumShape, V: DatumShape, S> DatumShape for IndexMap<K, V, S> {
    const SHAPE: Shape = Shape::Value;
}

macro_rules! tuples {
    ($(($($member:ident),+)),* $(,)?) => {
        $(impl<$($member: DatumShape),+> DatumShape for ($($member,)+) {
            const SHAPE: Shape = Shape::Value;
        })*
    };
}

tuples!(
    (A),
    (A, B),
    (A, B, C),
    (A, B, C, D),
    (A, B, C, D, E),
    (A, B, C, D, E, F),
    (A, B, C, D, E, F, G),
    (A, B, C, D, E, F, G, H),
    (A, B, C, D, E, F, G, H, I),
    (A, B, C, D, E, F, G, H, I, J),
    (A, B, C, D, E, F, G, H, I, J, K),
    (A, B, C, D, E, F, G, H, I, J, K, L),
    (A, B, C, D, E, F, G, H, I, J, K, L, M),
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N),
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O),
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P),
);

#[cfg(feature = "jiff")]
values!(jiff::Timestamp);
#[cfg(feature = "automerge")]
values!(automerge::ChangeHash);
