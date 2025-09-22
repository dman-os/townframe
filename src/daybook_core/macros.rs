/// FIXME: this requires $t_event to be clone
/// due to exported trait methods not being able to detect
/// Arc<$t_event> is tag
#[macro_export]
macro_rules! repo_listeners {
    (
        $t_repo:ty,
        $t_event:ty
    ) => {
        pastey::paste! {
            // Define a foreign trait that Kotlin will implement.
            #[uniffi::export(with_foreign)]
            pub trait [<$t_event Listener>]: Send + Sync + 'static {
                fn [<on_ $t_event:snake>](&self, event: $t_event);
            }

            #[uniffi::export]
            impl $t_repo {
                // Register a listener; returns a handle that unregisters on drop.
                //
                // UniFFI expects callback parameters to be plain trait objects (Box<dyn Trait>) rather than Arc<dyn Trait>.
                #[tracing::instrument(err, skip(self, listener))]
                async fn ffi_register_listener(
                    self: Arc<Self>,
                    listener: Arc<dyn [<$t_event Listener>]>,
                ) -> Result<Arc<$crate::repos::ListenerRegistration>, FfiError> {
                    let id = Uuid::new_v4();
                    {
                        let mut lock = self.registry.list.lock();
                        lock.push((id, $crate::repos::ErasedListener::new(move |ev| {
                            listener.[<on_ $t_event:snake>]($t_event::clone(&ev))
                        })));
                        // strong is dropped here; we only keep Weak to avoid leaks.
                    }
                    Ok(Arc::new($crate::repos::ListenerRegistration {
                        registry: Arc::downgrade(&self.registry),
                        id,
                    }))
                }
            }
        }
    };
}
