//! SetDrawerSuccessor / ResolveDrawerSuccessor: the fixed successor pointer (design §16.6).
//!
//! One singleton `org.example.daybook.drawer.successor/main` lives on the descriptor-bearing
//! doc — the public metadata root of a grouped drawer, the content doc of a self-doc drawer.
//! Resolution requires caller Read at each hop and stops before reading an inaccessible doc:
//! no grant, no roster or child copy, no blob move, no silent mirror-policy retarget. Access to
//! the successor is established independently of this pointer.

use super::*;
use big_cabinet_core::drawer::protocol::SuccessorResolution;

impl Core {
    /// The successor the descriptor-bearing doc of `drawer` names, if any. A self-pointing
    /// pointer is refused rather than followed.
    async fn successor_of(
        &self,
        context: &RequestContext,
        drawer: &DrawerId,
    ) -> Result<Option<DrawerId>, DrawersError> {
        let key = successor_key();
        let read = self
            .as_signer(
                context,
                self.issuer,
                DocScope {
                    read: [drawer.clone()].into(),
                    ..Default::default()
                },
                |capability| {
                    let key = key.clone();
                    async move {
                        self.docs()
                            .call(
                                context.child(),
                                big_cabinet_core::doc::ReadDatums {
                                    capability,
                                    doc_id: drawer.clone(),
                                    at: big_cabinet_core::doc::At::Current,
                                    datums: Some([key].into_iter().collect()),
                                    optimistic: false,
                                },
                            )
                            .await
                            .map_err(docs_error)
                    }
                },
            )
            .await?;
        let Some(datum) = read.datums.into_iter().next() else {
            return Ok(None);
        };
        let (_key, value) = datum;
        match value {
            big_cabinet_core::doc::DatumRead::Accepted { value, .. } => {
                let entry: DrawerSuccessor = serde_json::from_value(value).map_err(|error| {
                    DrawersError::RosterUnreadable(format!("successor on {drawer}: {error}"))
                })?;
                if entry.drawer_id == *drawer {
                    return Err(DrawersError::SuccessorSelf {
                        drawer_id: drawer.clone(),
                    });
                }
                Ok(Some(entry.drawer_id))
            }
            big_cabinet_core::doc::DatumRead::Absent { .. } => Ok(None),
            other => Err(DrawersError::RosterUnreadable(format!(
                "successor on {drawer}: {other:?}"
            ))),
        }
    }

    /// Follow the chain while the caller has Read on each hop. Every authorized hop is
    /// visited at most once; inaccessible hops are neither read nor classified as terminal.
    pub(crate) async fn resolve_successor(
        &self,
        context: &RequestContext,
        authority: &DomainAuthority<DrawerScope>,
        start: &DrawerId,
    ) -> Result<SuccessorResolution, DrawersError> {
        let mut seen = std::collections::BTreeSet::new();
        let mut current = start.clone();
        loop {
            match self
                .authorize(context, authority, &current, Access::Read)
                .await
            {
                Ok(()) => {}
                Err(DrawersError::Denied) if current != *start => {
                    return Ok(SuccessorResolution::AccessRequired { drawer_id: current });
                }
                Err(error) => return Err(error),
            }
            if !seen.insert(current.clone()) {
                return Err(DrawersError::SuccessorCycle {
                    start: start.clone(),
                    revisited: current,
                });
            }
            let Some(next) = self.successor_of(context, &current).await? else {
                return Ok(if current == *start {
                    SuccessorResolution::NoSuccessor
                } else {
                    SuccessorResolution::Resolved { drawer_id: current }
                });
            };
            current = next;
        }
    }
}

impl DrawerService {
    async fn set_successor(
        &self,
        context: &RequestContext,
        params: SetDrawerSuccessor,
    ) -> Result<(), DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.capability).await?;
        // Write+ on the old drawer's descriptor-bearing doc: the pointer is discovery, not a
        // grant, and the successor is never touched by this write.
        core.authorize(context, &caller.authority, &params.drawer_id, Access::Write)
            .await?;
        if params.drawer_id == params.successor {
            return Err(DrawersError::SuccessorSelf {
                drawer_id: params.drawer_id,
            });
        }
        core.ensure_drawer(context, &params.drawer_id)
            .await?
            .ok_or_else(|| DrawersError::UnknownDrawer(params.drawer_id.clone()))?;
        let entry = DrawerSuccessor {
            drawer_id: params.successor,
            extra: Default::default(),
        };
        core.write_roster(
            context,
            &params.drawer_id,
            None,
            BTreeMap::from([(
                successor_key(),
                serde_json::to_value(entry).expect("a successor serializes"),
            )]),
            Default::default(),
        )
        .await?;
        Ok(())
    }

    async fn resolve(
        &self,
        context: &RequestContext,
        params: ResolveDrawerSuccessor,
    ) -> Result<SuccessorResolution, DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.capability).await?;
        core.resolve_successor(context, &caller.authority, &params.drawer_id)
            .await
    }
}

impl Handler<SetDrawerSuccessor> for DrawerService {
    async fn call(
        &self,
        request: Envelope<SetDrawerSuccessor>,
    ) -> Result<ResponseEnvelope<()>, DrawersError> {
        Ok(self
            .set_successor(&request.context, request.params)
            .await?
            .into())
    }
}

impl Handler<ResolveDrawerSuccessor> for DrawerService {
    async fn call(
        &self,
        request: Envelope<ResolveDrawerSuccessor>,
    ) -> Result<ResponseEnvelope<SuccessorResolution>, DrawersError> {
        Ok(self.resolve(&request.context, request.params).await?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_successor_request_names_both_drawers() {
        let request = SetDrawerSuccessor {
            capability: CapId::from_bytes([1; 16]),
            drawer_id: DocId::new([2; 32]),
            successor: DocId::new([3; 32]),
        };
        let value = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "capability": CapId::from_bytes([1; 16]),
                "drawerId": DocId::new([2; 32]).to_string(),
                "successor": DocId::new([3; 32]).to_string(),
            })
        );
        assert_eq!(
            serde_json::from_value::<SetDrawerSuccessor>(value).expect("deserialize"),
            request
        );
    }

    #[test]
    fn the_successor_datum_round_trips() {
        let entry = DrawerSuccessor {
            drawer_id: DocId::new([7; 32]),
            extra: Default::default(),
        };
        let value = serde_json::to_value(&entry).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "drawerId": DocId::new([7; 32]).to_string(),
            })
        );
        assert_eq!(
            serde_json::from_value::<DrawerSuccessor>(value).expect("deserialize"),
            entry
        );
    }
}
