//! The service's own actor's durable holds on drawers (ADR 049 section 6.1): a person's drawer a
//! grant call handed it so it can hand the drawer down to a cartridge's actor, and a drawer a
//! cartridge's routine had it create.

use super::*;

impl State {
    /// Grants `drawer` to the service's own actor, durably and on `authorization`'s authority over
    /// it, so the service can hand it down to `cartridge_id`'s actor (ADR 049 section 6.1): an actor
    /// hands down only what it holds, and the service holds none of a person's drawers. The grant
    /// is a standing one, so it survives a restart, and it lasts until the cartridge's config
    /// document no longer names the drawer (`release_idle_drawers`). It needs Admin, which is what
    /// `GrantDrawer` demands of whoever delegates at any level, and fails with the drawers
    /// service's own refusal when the caller has no authority over the drawer.
    pub(super) async fn hold_drawer(
        &mut self,
        context: &RequestContext,
        authorization: Authorization,
        cartridge_id: &CartridgeId,
        drawer: &DrawerId,
    ) -> Res<()> {
        self.stores
            .grant_to_service(context, authorization, drawer)
            .await?;
        store::upsert_service_drawer(
            &self.stores.sql,
            drawer,
            cartridge_id,
            DrawerOrigin::Granted,
        )
        .await?;
        self.service_drawers
            .entry((drawer.clone(), cartridge_id.clone()))
            .or_insert(DrawerOrigin::Granted);
        Ok(())
    }

    /// Gives back the hold a grant that then failed to land made, if no other grant needs it.
    pub(super) async fn release_after_failed_grant(&mut self, context: &RequestContext) -> Res<()> {
        let snapshot = self.config().await?;
        self.release_idle_drawers(context, &snapshot.value).await
    }

    /// Ends each hold whose cartridge's config document is loaded and holds no accepted grant
    /// datum naming the drawer (revoked, or cleared), and withdraws the service actor's standing
    /// grant on a drawer no hold is left on. A cartridge whose document is not loaded keeps its
    /// holds: an uninstalled cartridge's document keeps its grants for a re-install to find, and
    /// one that has not synced here yet says nothing. A drawer the service created is its own and
    /// stays.
    pub(super) async fn release_idle_drawers(
        &mut self,
        context: &RequestContext,
        config: &NoduleConfig,
    ) -> Res<()> {
        let docs = config.cartridge_docs();
        let idle: Vec<(DrawerId, CartridgeId)> = self
            .service_drawers
            .iter()
            .filter(|((drawer, cartridge_id), origin)| {
                **origin == DrawerOrigin::Granted
                    && docs.get(cartridge_id).is_some_and(|doc| {
                        !doc.grants().any(|(key, slot)| {
                            matches!(
                                (key, slot),
                                (GrantKey::Drawer(granted), DatumSlot::Accepted { .. })
                                    if granted == drawer
                            )
                        })
                    })
            })
            .map(|(pair, _)| pair.clone())
            .collect();
        self.end_holds(context, idle).await
    }

    /// Ends the granted holds `pairs` name, and withdraws the service actor's standing grant on each drawer
    /// that no hold, granted or created, remains on.
    pub(super) async fn end_holds(
        &mut self,
        context: &RequestContext,
        pairs: Vec<(DrawerId, CartridgeId)>,
    ) -> Res<()> {
        for (drawer, cartridge_id) in pairs {
            let still_held = self
                .service_drawers
                .keys()
                .any(|(held, holder)| *held == drawer && *holder != cartridge_id);
            if !still_held {
                self.stores.revoke_from_service(context, &drawer).await?;
            }
            store::delete_service_drawer(&self.stores.sql, &drawer, &cartridge_id).await?;
            self.service_drawers.remove(&(drawer, cartridge_id));
        }
        Ok(())
    }

    /// A routine of `cartridge_id` created `drawer` through its own call. The drawers service made
    /// the service's own actor its owner, because the handle the routine called with was minted
    /// from that actor's authority; this records that the owner's hold is its own, not one a grant
    /// gave (so no reconcile gives it back), and writes the cartridge's grant datum for the drawer
    /// at `access`, the access its `#drawers` permbind declares. Written through the same
    /// config edit as `set_grant`, not by the guest, so the grant shows in the person's
    /// per-cartridge consent and audit view like any other, and the reconcile the edit runs
    /// grants the drawer down to the cartridge's actor and re-mints its handles. A routine that
    /// only creates (`access` is `None`) declares no access to what it makes, so it is granted
    /// none.
    pub(super) async fn drawer_created(
        &mut self,
        context: &RequestContext,
        cartridge_id: &CartridgeId,
        drawer: DrawerId,
        access: Option<DrawerAccess>,
    ) -> Res<()> {
        store::upsert_service_drawer(
            &self.stores.sql,
            &drawer,
            cartridge_id,
            DrawerOrigin::Created,
        )
        .await?;
        self.service_drawers.insert(
            (drawer.clone(), cartridge_id.clone()),
            DrawerOrigin::Created,
        );
        let Some(access) = access else {
            return Ok(());
        };
        self.edit_cartridge_config(context, cartridge_id, move |config| {
            config.set_grant(
                GrantKey::Drawer(drawer),
                Some(GrantBody::access(access.into())),
            )
        })
        .await
    }
}
