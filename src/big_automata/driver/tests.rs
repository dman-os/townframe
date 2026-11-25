//! The driver's law tests, one `law_N_...` per law of the parent module, over [`rig`]'s scripted
//! peer and backend on a paused clock, plus the full-sync layer's.
//!
//! The machine's own laws and its whole-machine simulation live beside it; what is judged
//! here is only what the driver adds. Every wait is bounded in virtual time (`within`), so a
//! driver that stalls fails the test.

mod backend;
mod crashes;
mod full_sync;
mod laws;
mod mutual;
mod reconnect;
mod rig;
