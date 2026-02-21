/// FIXME: this requires $t_event to be clone
/// due to exported trait methods not being able to detect
/// Arc<$t_event> is tag
#[macro_export]
macro_rules! uniffi_repo_listeners {
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
                #[tracing::instrument(skip(self, listener))]
                fn ffi_register_listener(
                    self: Arc<Self>,
                    listener: Arc<dyn [<$t_event Listener>]>,
                ) -> Arc<daybook_core::repos::ListenerRegistration> {
                    $crate::listener_bridge::register_uniffi_listener(self.as_ref(), move |ev| {
                        // UniFFI with_foreign does not support Arc callback args.
                        listener.[<on_ $t_event:snake>]($t_event::clone(&ev));
                    })
                }
            }
        }
    };
}
