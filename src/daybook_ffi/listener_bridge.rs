use crate::interlude::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

pub fn register_uniffi_listener<R, E, F>(
    repo: &R,
    on_event: F,
) -> Arc<daybook_core::repos::ListenerRegistration>
where
    R: daybook_core::repos::Repo<Event = E>,
    E: Send + Sync + 'static,
    F: Fn(Arc<E>) + Send + 'static,
{
    use daybook_core::repos::SubscribeOpts;

    let handle = repo.subscribe(SubscribeOpts::new(256));
    let registration = handle.registration();
    let dropped_warned = Arc::new(AtomicBool::new(false));

    let join_handle = std::thread::spawn(move || loop {
        match handle.recv_lossy_blocking() {
            Ok(event) => on_event(event),
            Err(daybook_core::repos::RecvError::Dropped { .. }) => {
                let seen = dropped_warned.swap(true, Ordering::AcqRel);
                if !seen {
                    warn!("uniffi listener queue is full; dropping events");
                }
            }
            Err(daybook_core::repos::RecvError::Closed) => break,
        }
    });
    let join_handle = Arc::new(Mutex::new(Some(join_handle)));
    registration.add_on_unregister(Arc::new(move || {
        let Some(join_handle) = join_handle
            .lock()
            .expect("listener join mutex poisoned")
            .take()
        else {
            return;
        };
        if let Err(error) = join_handle.join() {
            warn!(?error, "uniffi listener thread panicked while joining");
        }
    }));

    registration
}
