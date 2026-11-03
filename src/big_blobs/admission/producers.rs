use super::*;

impl State {
    pub async fn refresh_producers(&self, inventory: &DocId) -> Res<()> {
        self.register(inventory).await?;
        let tx = self.sql.begin_write().await?;
        let changed = sqlite::queries::producer_changes(&tx, inventory.to_string()).await?;
        if changed.is_empty() {
            tx.rollback().await?;
            return Ok(());
        }
        sqlite::queries::delete_producer_inventory(&tx, inventory.to_string()).await?;

        sqlite::queries::insert_producer_inventory(&tx, inventory.to_string()).await?;
        for digest in changed {
            Self::touch(&tx, inventory, digest.parse()?).await?;
        }
        tx.commit().await?;
        self.changes.send_modify(|revision| *revision += 1);
        self.changed.notify_one();
        Ok(())
    }
}

impl Admission {
    pub async fn refresh_producers(&self, inventory: &DocId) -> Res<()> {
        let inventory = inventory.clone();
        self.run(move |state| Box::pin(async move { state.refresh_producers(&inventory).await }))
            .await
    }
}
