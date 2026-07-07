//! Composite keyhive listener that forwards every event into the runtime's
//! event channel as a typed [`RuntimeEvt`] message.
//!
//! Constructed with **only** a sender — no keyhive/storage handle
//! (avoids the reference cycle the playbook warns about). Pure forwarder.

use crate::runtime::RuntimeEvt;
use beekem::operation::CgkaOperation;
use future_form::{FutureForm, Sendable};
use keyhive_core::listener::{
    cgka::CgkaListener,
    membership::MembershipListener,
    prekey::PrekeyListener,
};
use keyhive_core::principal::{
    group::{delegation::Delegation, revocation::Revocation},
    identifier::Identifier,
    individual::op::{add_key::AddKeyOp, rotate_key::RotateKeyOp},
};
use keyhive_crypto::signed::Signed;
use keyhive_crypto::signer::memory::MemorySigner;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Listens for keyhive events and forwards them to the big_repo runtime.
///
/// Implements every keyhive listener trait. Each `on_*` packs the event into
/// the matching [`RuntimeEvt`] variant and sends it over the unbounded channel.
/// No async work beyond the send.
///
/// Cloning is cheap (the sender is `Clone`).
#[derive(Clone, Debug)]
pub struct BigRepoKeyhiveListener {
    pub(crate) evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
}

impl PrekeyListener<Sendable> for BigRepoKeyhiveListener {
    fn on_prekeys_expanded<'a>(
        &'a self,
        new_prekey: &'a Arc<Signed<AddKeyOp>>,
    ) -> <Sendable as FutureForm>::Future<'a, ()> {
        let _ = self.evt_tx.send(RuntimeEvt::PrekeyExpanded {
            new_prekey: new_prekey.clone(),
        });
        Sendable::ready(())
    }

    fn on_prekey_rotated<'a>(
        &'a self,
        rotate_key: &'a Arc<Signed<RotateKeyOp>>,
    ) -> <Sendable as FutureForm>::Future<'a, ()> {
        let _ = self.evt_tx.send(RuntimeEvt::PrekeyRotated {
            rotate_key: rotate_key.clone(),
        });
        Sendable::ready(())
    }
}

impl CgkaListener<Sendable> for BigRepoKeyhiveListener {
    fn on_cgka_op<'a>(
        &'a self,
        data: &'a Arc<Signed<CgkaOperation>>,
    ) -> <Sendable as FutureForm>::Future<'a, ()> {
        let _ = self.evt_tx.send(RuntimeEvt::CgkaOp {
            data: data.clone(),
        });
        Sendable::ready(())
    }
}

/// Concrete impl for `Sendable` runtime with `MemorySigner` / `Vec<u8>`.
/// The [`MembershipListener`] trait's delegation/revocation types carry the
/// signer and content-ref generics, so the impl must be concrete to match
/// [`RuntimeEvt`]'s payload types.
impl MembershipListener<Sendable, MemorySigner, Vec<u8>> for BigRepoKeyhiveListener {
    fn on_delegation<'a>(
        &'a self,
        target: Identifier,
        data: &'a Arc<
            Signed<Delegation<Sendable, MemorySigner, Vec<u8>, BigRepoKeyhiveListener>>,
        >,
    ) -> <Sendable as FutureForm>::Future<'a, ()> {
        let _ = self.evt_tx.send(RuntimeEvt::DelegationReceived {
            target,
            data: data.clone(),
        });
        Sendable::ready(())
    }

    fn on_revocation<'a>(
        &'a self,
        target: Identifier,
        data: &'a Arc<
            Signed<Revocation<Sendable, MemorySigner, Vec<u8>, BigRepoKeyhiveListener>>,
        >,
    ) -> <Sendable as FutureForm>::Future<'a, ()> {
        let _ = self.evt_tx.send(RuntimeEvt::RevocationReceived {
            target,
            data: data.clone(),
        });
        Sendable::ready(())
    }
}
