use super::*;
use big_sync::SubscriptionSource;
use big_sync::SubscriptionUpdate;
use std::collections::HashMap;
use std::collections::HashSet;

impl InventoryMirrors {
    pub async fn drive_subscriptions(
        &self,
        source: SubscriptionSource,
        backend: big_sync::BackendId,
    ) -> Res<()> {
        let mut changes = self.admission.subscribe_changes();
        let mut transport = self.downloads.subscribe();
        let mut previous_parts = HashSet::new();
        let mut previous_peers = HashSet::new();
        loop {
            changes.borrow_and_update();
            let peers: HashSet<_> = transport
                .borrow_and_update()
                .as_ref()
                .map(|peers| peers.addresses.keys().cloned().collect())
                .unwrap_or_default();
            let mut parts = HashSet::new();
            for inventory in self.admission.selected_inventories().await? {
                if self.projection.readable(&inventory).await? {
                    parts.insert(crate::workers::inventory_part(&inventory));
                }
            }
            for peer in previous_peers.difference(&peers) {
                source.remove_peer(peer.clone()).await?;
            }
            let added: HashMap<_, _> = parts
                .difference(&previous_parts)
                .cloned()
                .map(|part| (part, Arc::clone(&backend)))
                .collect();
            let removed: HashSet<_> = previous_parts.difference(&parts).cloned().collect();
            for peer in &peers {
                let update = if previous_peers.contains(peer) {
                    SubscriptionUpdate {
                        parts: added.clone(),
                        removed_parts: removed.clone(),
                        ..Default::default()
                    }
                } else {
                    SubscriptionUpdate {
                        parts: parts
                            .iter()
                            .cloned()
                            .map(|part| (part, Arc::clone(&backend)))
                            .collect(),
                        ..Default::default()
                    }
                };
                if !update.parts.is_empty() || !update.removed_parts.is_empty() {
                    source.update(peer.clone(), update).await?;
                }
            }
            previous_parts = parts;
            previous_peers = peers;
            tokio::select! {
                changed = changes.changed() => if changed.is_err() { return Ok(()); },
                changed = transport.changed() => if changed.is_err() { return Ok(()); },
            }
        }
    }
}
