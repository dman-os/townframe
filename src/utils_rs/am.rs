use crate::interlude::*;
use automerge::ObjId;
use autosurgeon::{Hydrate, HydrateError, ReadDoc, Reconcile, Reconciler};

pub mod autosurgeon_date {
    use super::*;

    pub fn reconcile<R: Reconciler>(
        ts: &OffsetDateTime,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        reconciler.timestamp(ts.unix_timestamp())
    }

    struct Wrapper(i64);
    impl Hydrate for Wrapper {
        fn hydrate_timestamp(ts: i64) -> Result<Self, HydrateError> {
            Ok(Self(ts))
        }
    }

    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &ObjId,
        prop: autosurgeon::Prop<'a>,
    ) -> Result<OffsetDateTime, HydrateError> {
        let Wrapper(inner) = Wrapper::hydrate(doc, obj, prop)?;
        OffsetDateTime::from_unix_timestamp(inner).map_err(|err| {
            HydrateError::unexpected(
                "an valid unix timestamp",
                format!("error parsing timestamp int {err}"),
            )
        })
    }
}

pub mod automerge_skip {
    use super::*;
    use autosurgeon::{HydrateError, ReadDoc, Reconciler};

    pub fn reconcile<T: Default, R: Reconciler>(
        _value: &T,
        _reconciler: R,
    ) -> Result<(), R::Error> {
        Ok(())
    }

    pub fn hydrate<'a, D: ReadDoc, T: Default>(
        _doc: &D,
        _obj: &ObjId,
        _prop: autosurgeon::Prop<'a>,
    ) -> Result<T, HydrateError> {
        Ok(T::default())
    }
}

pub mod autosurgeon_tuple_2 {
    use super::*;

    pub fn reconcile<R: Reconciler, T1: Reconcile, T2: Reconcile>(
        (one, two): &(T1, T2),
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        use autosurgeon::reconcile::SeqReconciler;
        let mut seq = reconciler.seq()?;
        let len = seq.len()?;
        if len == 0 {
            seq.insert(0, one)?;
            seq.insert(1, two)?;
        } else if len == 1 {
            seq.set(0, one)?;
            seq.insert(1, two)?;
        } else {
            seq.set(0, one)?;
            seq.set(1, two)?;
            for ii in len..2 {
                seq.delete(ii)?;
            }
        }
        Ok(())
    }
}
