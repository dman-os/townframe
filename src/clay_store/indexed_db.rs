use crate::BatchOp;
use crate::OrderedStore;
use crate::StoreError;
use crate::prefix_successor;
use js_sys::Array;
use js_sys::Promise;
use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::IdbDatabase;
use web_sys::IdbFactory;
use web_sys::IdbKeyRange;
use web_sys::IdbObjectStore;
use web_sys::IdbOpenDbRequest;
use web_sys::IdbRequest;
use web_sys::IdbTransaction;
use web_sys::IdbTransactionMode;

const OBJECT_STORE: &str = "entries";

/// Browser store over one IndexedDB object store. Keys and values are
/// `Uint8Array`s; IndexedDB orders binary keys bytewise, as the contract needs.
pub struct IndexedDbStore {
    db: IdbDatabase,
}

// Durable collections hold their store behind `Send + Sync` handles because collections are
// shared across threads natively and the host's cleanup hooks require it. The `IdbDatabase`
// JS handle is neither, but a wasm32 build without the `atomics` target feature has no
// threads at all: every handle is only ever touched by the one thread that made it, so the
// bounds hold vacuously. With `atomics` (shared-memory workers) a JS handle really cannot
// cross threads, so this fails to compile there instead of becoming unsound.
#[cfg(not(target_feature = "atomics"))]
// SAFETY: see above; there is no second thread to send to.
unsafe impl Send for IndexedDbStore {}
#[cfg(not(target_feature = "atomics"))]
// SAFETY: see above; there is no second thread to share with.
unsafe impl Sync for IndexedDbStore {}

fn js_error(value: JsValue) -> StoreError {
    StoreError::backend(format!("{value:?}"))
}

/// A promise settled by the request's `success`/`error` events. Handlers are
/// attached synchronously, so awaiting later cannot miss the event.
fn settled(request: &IdbRequest) -> JsFuture {
    JsFuture::from(Promise::new(&mut |resolve, reject| {
        request.set_onsuccess(Some(&resolve));
        request.set_onerror(Some(&reject));
    }))
}

/// A promise that resolves on transaction `complete` and rejects on abort or
/// error; a failed request aborts its transaction, so this is the commit signal.
fn committed(tx: &IdbTransaction) -> JsFuture {
    JsFuture::from(Promise::new(&mut |resolve, reject| {
        tx.set_oncomplete(Some(&resolve));
        tx.set_onerror(Some(&reject));
        tx.set_onabort(Some(&reject));
    }))
}

fn key_range(prefix: &[u8]) -> Result<IdbKeyRange, StoreError> {
    let lower = Uint8Array::from(prefix);
    match prefix_successor(prefix) {
        Some(upper) => IdbKeyRange::bound_with_lower_open_and_upper_open(
            &lower,
            &Uint8Array::from(upper.as_slice()),
            false,
            true,
        ),
        None => IdbKeyRange::lower_bound_with_open(&lower, false),
    }
    .map_err(js_error)
}

fn to_bytes(value: &JsValue) -> Vec<u8> {
    // Binary keys come back as `ArrayBuffer`, values as `Uint8Array`; anything
    // else means we read something we did not write, an invariant break.
    if let Some(bytes) = value.dyn_ref::<Uint8Array>() {
        bytes.to_vec()
    } else if value.is_instance_of::<js_sys::ArrayBuffer>() {
        Uint8Array::new(value).to_vec()
    } else {
        panic!("indexeddb entry is neither Uint8Array nor ArrayBuffer: {value:?}")
    }
}

impl IndexedDbStore {
    /// Opens (creating on first use) the named database in the current window
    /// or worker.
    pub async fn open(name: &str) -> Result<Self, StoreError> {
        let factory: IdbFactory =
            js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("indexedDB"))
                .map_err(js_error)?
                .dyn_into()
                .expect("global scope has no indexedDB");
        let request: IdbOpenDbRequest = factory.open_with_u32(name, 1).map_err(js_error)?;
        let on_upgrade = Closure::<dyn FnMut(web_sys::Event)>::new(move |event: web_sys::Event| {
            let request: IdbOpenDbRequest = event
                .target()
                .expect("event without target")
                .unchecked_into();
            let db: IdbDatabase = request
                .result()
                .expect("upgrade without result")
                .unchecked_into();
            db.create_object_store(OBJECT_STORE)
                .expect("creating the object store");
        });
        request.set_onupgradeneeded(Some(on_upgrade.as_ref().unchecked_ref()));
        settled(&request).await.map_err(js_error)?;
        let db: IdbDatabase = request.result().map_err(js_error)?.unchecked_into();
        Ok(Self { db })
    }

    fn transaction(
        &self,
        mode: IdbTransactionMode,
    ) -> Result<(IdbTransaction, IdbObjectStore), StoreError> {
        let tx = self
            .db
            .transaction_with_str_and_mode(OBJECT_STORE, mode)
            .map_err(js_error)?;
        let store = tx.object_store(OBJECT_STORE).map_err(js_error)?;
        Ok((tx, store))
    }
}

impl OrderedStore for IndexedDbStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let (_tx, store) = self.transaction(IdbTransactionMode::Readonly)?;
        let request = store.get(&Uint8Array::from(key)).map_err(js_error)?;
        let value = settled(&request)
            .await
            .map_err(js_error)
            .and_then(|_| request.result().map_err(js_error))?;
        Ok((!value.is_undefined()).then(|| to_bytes(&value)))
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let (_tx, store) = self.transaction(IdbTransactionMode::Readonly)?;
        let range = key_range(prefix)?;
        // Both requests ride one readonly transaction, so keys and values come
        // from the same snapshot, and both are in flight before either awaits.
        let keys = store.get_all_keys_with_key(&range).map_err(js_error)?;
        let values = store.get_all_with_key(&range).map_err(js_error)?;
        let (keys_done, values_done) = (settled(&keys), settled(&values));
        keys_done.await.map_err(js_error)?;
        values_done.await.map_err(js_error)?;
        let keys: Array = keys.result().map_err(js_error)?.unchecked_into();
        let values: Array = values.result().map_err(js_error)?.unchecked_into();
        Ok(keys
            .iter()
            .zip(values.iter())
            .map(|(key, value)| (to_bytes(&key), to_bytes(&value)))
            .collect())
    }

    async fn apply(&self, batch: Vec<BatchOp>) -> Result<(), StoreError> {
        let (tx, store) = self.transaction(IdbTransactionMode::Readwrite)?;
        let done = committed(&tx);
        // Requests execute in issue order within the transaction. Any failure
        // aborts the whole transaction, which is what makes the batch atomic;
        // if issuing itself throws, abort explicitly before reporting.
        let issued: Result<(), StoreError> = (|| {
            for op in &batch {
                match op {
                    BatchOp::Put(key, value) => {
                        store
                            .put_with_key(
                                &Uint8Array::from(value.as_slice()),
                                &Uint8Array::from(key.as_slice()),
                            )
                            .map_err(js_error)?;
                    }
                    BatchOp::Delete(key) => {
                        store
                            .delete(&Uint8Array::from(key.as_slice()))
                            .map_err(js_error)?;
                    }
                    BatchOp::DeletePrefix(prefix) => {
                        let range = key_range(prefix)?;
                        store.delete(&range).map_err(js_error)?;
                    }
                }
            }
            Ok(())
        })();
        if let Err(error) = issued {
            tx.abort().map_err(js_error)?;
            // The abort rejects `done`; the issuing error is the cause.
            done.await.expect_err("an aborted transaction rejects");
            return Err(error);
        }
        done.await.map_err(js_error)?;
        Ok(())
    }
}
