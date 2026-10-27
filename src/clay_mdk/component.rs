//! The MDK's opaque published-component identity. Contract crates carry this instead of a
//! backend type, so the same contract compiles for host-linked micros and (once the
//! collections waist lands) for guests. Only the host can issue a live reference; the value
//! itself is transport data and mints no authority.

/// Opaque reference to a component its owner published through the MDK. The native backend
/// wraps the host reference; the guest-side form arrives with the collections WIT waist.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ComponentRef(
    #[cfg(feature = "in_process")] clay_host::ComponentRef,
    #[cfg(not(feature = "in_process"))] (),
);

#[cfg(feature = "in_process")]
impl ComponentRef {
    /// Wrap a host-issued reference. Host-side adapter and test vocabulary; the reference's
    /// liveness and grants remain host-enforced.
    pub fn from_host(reference: clay_host::ComponentRef) -> Self {
        Self(reference)
    }

    /// The host-side reference. Native views (host-linked Dioxus components) mount through it;
    /// portable contract data must not.
    pub fn host(&self) -> clay_host::ComponentRef {
        self.0
    }
}
