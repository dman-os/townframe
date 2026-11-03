use super::*;

impl State {
    pub async fn refresh_producers(&self, inventory: &DocId) -> Res<()> {
        self.register(inventory).await?;
        let tx = self.sql.begin_write().await?;
        let changed = tx
            .query(
                "WITH desired AS (
                 SELECT digest
                      , json_array(doc_id, drawer_id, root, name) AS owner
                   FROM big_blobs_writer_claims
                  WHERE inventory_id = ?1
             )
             SELECT digest FROM (
                 SELECT digest, owner FROM desired
                 EXCEPT
                 SELECT digest, owner FROM big_blobs_producer_claims WHERE inventory_id = ?1
             )
             UNION
             SELECT digest FROM (
                 SELECT digest, owner FROM big_blobs_producer_claims WHERE inventory_id = ?1
                 EXCEPT
                 SELECT digest, owner FROM desired
             )",
                (inventory.to_string(),),
            )
            .await?
            .all::<String>()?;
        if changed.is_empty() {
            tx.rollback().await?;
            return Ok(());
        }
        tx.execute(
            "DELETE FROM big_blobs_producer_claims WHERE inventory_id = ?",
            (inventory.to_string(),),
        )
        .await?;
        tx.execute(
            "INSERT INTO big_blobs_producer_claims (inventory_id, digest, owner)
             SELECT inventory_id
                  , digest
                  , json_array(doc_id, drawer_id, root, name)
               FROM big_blobs_writer_claims
              WHERE inventory_id = ?",
            (inventory.to_string(),),
        )
        .await?;
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
