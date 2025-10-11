use super::*;
use automerge::ObjId;

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
