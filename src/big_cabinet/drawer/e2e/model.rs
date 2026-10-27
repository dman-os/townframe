//! A randomized script of drawer operations against a single node, checked against a reference
//! model of what each actor should be able to read.

use super::*;
use proptest::collection::vec as prop_vec;
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

const DRAWERS: usize = 3;
const DOCS: usize = 2;

#[derive(Clone, Debug)]
enum RecordedCommand {
    AddDoc(AddDoc),
    RemoveDoc(RemoveDoc),
    AddChild(AddChild),
    RemoveChild(RemoveChild),
}

impl RecordedCommand {
    fn command(&self) -> &CommandId {
        match self {
            Self::AddDoc(req) => &req.command,
            Self::RemoveDoc(req) => &req.command,
            Self::AddChild(req) => &req.command,
            Self::RemoveChild(req) => &req.command,
        }
    }

    async fn replay(&self, node: &Node, capability: CapId) -> Result<WriteReceipt, DrawersError> {
        match self.clone() {
            Self::AddDoc(mut req) => {
                req.capability = capability;
                node.drawers()
                    .call(RequestContext::root(), req)
                    .await
                    .map_err(op)
            }
            Self::RemoveDoc(mut req) => {
                req.capability = capability;
                node.drawers()
                    .call(RequestContext::root(), req)
                    .await
                    .map_err(op)
            }
            Self::AddChild(mut req) => {
                req.capability = capability;
                node.drawers()
                    .call(RequestContext::root(), req)
                    .await
                    .map_err(op)
            }
            Self::RemoveChild(mut req) => {
                req.capability = capability;
                node.drawers()
                    .call(RequestContext::root(), req)
                    .await
                    .map_err(op)
            }
        }
    }
}

/// How an interrupted roster operation gets finished.
#[derive(Clone, Copy, Debug)]
enum Recovery {
    /// Nobody retries; the next boot replays the unfinished row.
    Boot,
    /// The caller retries the same command; the row is resumed in place.
    Retry,
}

#[derive(Clone, Copy, Debug)]
enum ModelOp {
    AddDoc {
        drawer: usize,
        doc: usize,
        access: Access,
    },
    RemoveDoc {
        drawer: usize,
        doc: usize,
    },
    /// An add dying at `point`, then finished by `recover`; when the entry already stands where
    /// `point` is never reached the crash is disarmed and this is a plain add.
    CrashedAddDoc {
        drawer: usize,
        doc: usize,
        access: Access,
        point: CrashPoint,
        recover: Recovery,
    },
    /// A removal dying after its roster write, then finished by `recover`; a no-op removal
    /// never reaches the point and refuses as a plain one does.
    CrashedRemoveDoc {
        drawer: usize,
        doc: usize,
        recover: Recovery,
    },
    AddChild {
        parent: usize,
        child: usize,
        access: Access,
    },
    RemoveChild {
        parent: usize,
        child: usize,
    },
    Grant {
        drawer: usize,
        access: Access,
    },
    Revoke {
        drawer: usize,
    },
    Remember {
        drawer: usize,
        mirror: bool,
    },
    SetPolicy {
        drawer: usize,
        mirror: bool,
    },
    Forget {
        drawer: usize,
    },
    Replay(usize),
    Restart,
}

struct DrawerModel {
    drawers: [DrawerId; DRAWERS],
    docs: [DocId; DOCS],
    roster_docs: [BTreeMap<DocId, Access>; DRAWERS],
    roster_children: [BTreeMap<DrawerId, Access>; DRAWERS],
    bob_grants: [BTreeSet<Access>; DRAWERS],
    /// What each doc's note reads back as: the last write that went through.
    notes: [Value; DOCS],
    known: BTreeMap<DrawerId, bool>,
    commands: Vec<(RecordedCommand, WriteReceipt)>,
}

impl DrawerModel {
    fn new(case: &str, drawers: [DrawerId; DRAWERS], docs: [DocId; DOCS]) -> Self {
        Self {
            drawers,
            docs,
            roster_docs: std::array::from_fn(|_| BTreeMap::new()),
            roster_children: std::array::from_fn(|_| BTreeMap::new()),
            bob_grants: std::array::from_fn(|_| BTreeSet::new()),
            notes: std::array::from_fn(|doc| note(&format!("{case}/doc{doc}")).1),
            known: BTreeMap::new(),
            commands: Vec::new(),
        }
    }

    fn drawer_index(&self, id: &DrawerId) -> Option<usize> {
        self.drawers.iter().position(|d| d == id)
    }

    fn held_bob(&self, drawer: usize) -> Option<Access> {
        self.bob_grants[drawer].iter().copied().max()
    }

    /// Strongest effective access Bob holds on `target`, directly or through any parent path
    /// (weakest link along each path, strongest path winning, cycle-safe).
    fn effective_bob(&self, target: usize) -> Option<Access> {
        let mut cap: [Option<Access>; DRAWERS] = [None; DRAWERS];
        cap[target] = Some(Access::Admin);
        for _ in 0..DRAWERS {
            for parent in 0..DRAWERS {
                for (child_id, &edge) in &self.roster_children[parent] {
                    if let Some(child) = self.drawer_index(child_id)
                        && let Some(child_cap) = cap[child]
                    {
                        let via = edge.min(child_cap);
                        if cap[parent].is_none_or(|cur| via > cur) {
                            cap[parent] = Some(via);
                        }
                    }
                }
            }
        }
        (0..DRAWERS)
            .filter_map(|d| self.held_bob(d).zip(cap[d]).map(|(h, c)| h.min(c)))
            .max()
    }

    /// Reference `ListDocs` output for Bob on `root`.
    fn scoped_docs_bob(&self, root: usize) -> Option<Vec<ListedDoc>> {
        let eff = self.effective_bob(root)?;
        let mut bounds: BTreeMap<DrawerId, Access> =
            BTreeMap::from([(self.drawers[root].clone(), eff)]);
        let mut frontier = vec![(root, eff)];
        while let Some((curr, bound)) = frontier.pop() {
            for (child_id, &edge) in &self.roster_children[curr] {
                let via = bound.min(edge);
                if bounds.get(child_id).is_some_and(|known| *known >= via) {
                    continue;
                }
                bounds.insert(child_id.clone(), via);
                if let Some(child_idx) = self.drawer_index(child_id) {
                    frontier.push((child_idx, via));
                }
            }
        }
        let mut entries: BTreeMap<DocId, (Access, Option<DrawerId>)> = BTreeMap::new();
        for (doc_id, &entry) in &self.roster_docs[root] {
            entries.insert(doc_id.clone(), (eff.min(entry), None));
        }
        for (child_id, &via) in &bounds {
            if *child_id == self.drawers[root] {
                continue;
            }
            if let Some(child_idx) = self.drawer_index(child_id) {
                for (doc_id, &entry) in &self.roster_docs[child_idx] {
                    let reached = via.min(entry);
                    match entries.get(doc_id) {
                        Some((known, _)) if *known >= reached => {}
                        _ => {
                            entries.insert(doc_id.clone(), (reached, Some(child_id.clone())));
                        }
                    }
                }
            }
        }
        Some(
            entries
                .into_iter()
                .map(|(doc_id, (access, via))| ListedDoc {
                    doc_id,
                    access,
                    via,
                })
                .collect(),
        )
    }

    /// Strongest access Bob reaches on `doc_idx` across all drawers.
    fn reach_bob(&self, doc_idx: usize) -> Option<Access> {
        let doc = &self.docs[doc_idx];
        (0..DRAWERS)
            .filter_map(|d| {
                self.effective_bob(d)
                    .zip(self.roster_docs[d].get(doc).copied())
                    .map(|(eff, entry)| eff.min(entry))
            })
            .max()
    }

    async fn check(&mut self, node: &Node, alice: &Actor, bob: &Actor, context: &str) {
        let expected_listed: BTreeMap<DrawerId, Access> = (0..DRAWERS)
            .filter_map(|d| {
                self.effective_bob(d)
                    .map(|eff| (self.drawers[d].clone(), eff))
            })
            .collect();
        let actual_listed: BTreeMap<DrawerId, Access> = node
            .drawers()
            .call(
                RequestContext::root(),
                ListDrawers {
                    capability: node
                        .mint_drawer(
                            bob,
                            DrawerScope {
                                drawers: (0..DRAWERS)
                                    .filter_map(|index| {
                                        self.held_bob(index)
                                            .map(|access| (self.drawers[index].clone(), access))
                                    })
                                    .collect(),
                                create: false,
                            },
                        )
                        .await
                        .unwrap(),
                },
            )
            .await
            .unwrap()
            .into_iter()
            .map(|d| (d.drawer_id, d.access))
            .collect();
        assert_eq!(actual_listed, expected_listed, "{context}: ListDrawers");

        for d in 0..DRAWERS {
            let drawer_id = &self.drawers[d];
            match self.effective_bob(d) {
                None => {
                    assert_eq!(
                        node.view(bob, drawer_id).await.unwrap_err(),
                        DrawersError::Denied,
                        "{context}: GetDrawer denied on D{d}"
                    );
                    assert_eq!(
                        node.list_docs(bob, drawer_id, 0, None).await.unwrap_err(),
                        DrawersError::Denied,
                        "{context}: ListDocs denied on D{d}"
                    );
                }
                Some(eff) => {
                    let view = node.view(bob, drawer_id).await.unwrap();
                    assert_eq!(view.access, eff, "{context}: GetDrawer access on D{d}");
                    assert_eq!(
                        view.docs, self.roster_docs[d],
                        "{context}: GetDrawer docs on D{d}"
                    );
                    assert_eq!(
                        view.children, self.roster_children[d],
                        "{context}: GetDrawer children on D{d}"
                    );
                    let listed_docs = node.list_docs(bob, drawer_id, 0, None).await.unwrap();
                    assert_eq!(
                        Some(listed_docs),
                        self.scoped_docs_bob(d),
                        "{context}: ListDocs on D{d}"
                    );
                }
            }
        }

        for doc_idx in 0..DOCS {
            let doc_id = &self.docs[doc_idx];
            let reached = self.reach_bob(doc_idx);
            let read_res = node.read_note(bob, doc_id).await;
            if reached >= Some(Access::Read) {
                assert_eq!(
                    read_res.as_ref().map_err(|error| error.to_string()),
                    Ok(&self.notes[doc_idx]),
                    "{context}: expected Read on Doc{doc_idx} (reached {reached:?})"
                );
            } else {
                assert_eq!(
                    read_res.unwrap_err(),
                    DocsError::Denied,
                    "{context}: expected Read Denied on Doc{doc_idx} (reached {reached:?})"
                );
            }
            let write_res = node.write_note(bob, doc_id, context).await;
            if reached >= Some(Access::Write) {
                assert_eq!(
                    write_res,
                    Ok(()),
                    "{context}: expected Write on Doc{doc_idx} (reached {reached:?})"
                );
                self.notes[doc_idx] = note(context).1;
                assert_eq!(
                    node.read_note(bob, doc_id).await.unwrap(),
                    self.notes[doc_idx],
                    "{context}: Doc{doc_idx} reads back the write"
                );
            } else {
                assert_eq!(
                    write_res.unwrap_err(),
                    DocsError::Denied,
                    "{context}: expected Write Denied on Doc{doc_idx} (reached {reached:?})"
                );
            }
        }

        let actual_known: BTreeMap<DrawerId, bool> = node
            .known_map(alice)
            .await
            .into_iter()
            .filter(|(id, _)| self.drawer_index(id).is_some())
            .collect();
        assert_eq!(actual_known, self.known, "{context}: ListKnownDrawers");

        for d in 0..DRAWERS {
            let (_meta, docs_group) = node.core().groups(&self.drawers[d]).await.unwrap();
            for doc_idx in 0..DOCS {
                let doc_id = &self.docs[doc_idx];
                assert_eq!(
                    node.core()
                        .docs_group_level(&docs_group, doc_id)
                        .await
                        .unwrap(),
                    self.roster_docs[d].get(doc_id).copied(),
                    "{context}: D{d}'s own Keyhive grant on Doc{doc_idx} matches its roster entry"
                );
            }
        }
    }
}

/// Alice's capability for a script step: Admin on every model drawer, the home, and the
/// registry, minted afresh since a boot retires the previous one.
async fn admin_capability(
    node: &Node,
    alice: &Actor,
    model: &DrawerModel,
    home: &DrawerId,
) -> CapId {
    node.mint_drawer(
        alice,
        DrawerScope {
            drawers: model
                .drawers
                .iter()
                .chain([home, &node.local().registry_id])
                .map(|drawer| (drawer.clone(), Access::Admin))
                .collect(),
            create: false,
        },
    )
    .await
    .unwrap()
}

/// Run `recorded` into an armed crash at `point`, check the ledger keeps exactly its row,
/// finish it by `recover`, and answer with the receipt the command then settles on.
#[allow(clippy::too_many_arguments)]
async fn crash_through(
    node: &mut Node,
    alice: &Actor,
    model: &DrawerModel,
    home: &DrawerId,
    point: CrashPoint,
    recover: Recovery,
    recorded: &RecordedCommand,
    capability: CapId,
    context: &str,
) -> WriteReceipt {
    node.arm_crash(point);
    let crashed = recorded.replay(node, capability).await.unwrap_err();
    assert!(
        matches!(&crashed, DrawersError::Storage(message) if message.contains("injected crash")),
        "{context}: {crashed:?}"
    );
    let unfinished = node.core().ledger.unfinished().await.unwrap();
    assert_eq!(
        unfinished
            .iter()
            .map(|row| &row.command)
            .collect::<Vec<_>>(),
        vec![recorded.command()],
        "{context}: the crash left exactly its row unfinished"
    );
    let capability = match recover {
        Recovery::Boot => {
            node.restart().await.unwrap();
            assert!(
                node.core().ledger.unfinished().await.unwrap().is_empty(),
                "{context}: boot finished the row"
            );
            admin_capability(node, alice, model, home).await
        }
        Recovery::Retry => capability,
    };
    let receipt = recorded.replay(node, capability).await.expect(context);
    assert!(
        node.core().ledger.unfinished().await.unwrap().is_empty(),
        "{context}: the retry finished the row"
    );
    receipt
}

async fn run_script(node: &mut Node, case: &str, script: &[ModelOp]) {
    let alice = node.creator(&format!("alice-{case}")).await;
    let bob = node.actor(&format!("bob-{case}")).await;
    node.core()
        .grants
        .grant(
            &RequestContext::root(),
            alice.actor_id,
            drawer_object(Access::Admin, &node.local().registry_id),
            Rights::USE,
            None,
        )
        .await
        .unwrap();
    let home = node.collection(&alice, &format!("{case}/home")).await;
    let drawers = [
        node.collection(&alice, &format!("{case}/d0")).await,
        node.collection(&alice, &format!("{case}/d1")).await,
        node.collection(&alice, &format!("{case}/d2")).await,
    ];
    let docs = [
        node.create_doc(&alice, &home, &format!("{case}/doc0"))
            .await
            .unwrap(),
        node.create_doc(&alice, &home, &format!("{case}/doc1"))
            .await
            .unwrap(),
    ];
    let mut model = DrawerModel::new(case, drawers, docs);
    model
        .check(node, &alice, &bob, &format!("{case}/init"))
        .await;

    for (step, &op_step) in script.iter().enumerate() {
        let ctx = format!("{case}/{step}:{op_step:?}");
        let cmd = command(&format!("{case}/{step}"));
        let capability = admin_capability(node, &alice, &model, &home).await;
        match op_step {
            ModelOp::AddDoc {
                drawer,
                doc,
                access,
            } => {
                let req = AddDoc {
                    capability,
                    command: cmd,
                    drawer_id: model.drawers[drawer].clone(),
                    doc_id: model.docs[doc].clone(),
                    access,
                };
                let receipt = node
                    .drawers()
                    .call(RequestContext::root(), req.clone())
                    .await
                    .unwrap();
                model.roster_docs[drawer].insert(model.docs[doc].clone(), access);
                model.commands.push((RecordedCommand::AddDoc(req), receipt));
            }
            ModelOp::RemoveDoc { drawer, doc } => {
                let doc_id = model.docs[doc].clone();
                let req = RemoveDoc {
                    capability,
                    command: cmd,
                    drawer_id: model.drawers[drawer].clone(),
                    doc_id: doc_id.clone(),
                };
                let res = node
                    .drawers()
                    .call(RequestContext::root(), req.clone())
                    .await
                    .map_err(op);
                if model.roster_docs[drawer].remove(&doc_id).is_some() {
                    let receipt = res.expect(&ctx);
                    model
                        .commands
                        .push((RecordedCommand::RemoveDoc(req), receipt));
                } else {
                    assert_eq!(res.unwrap_err(), DrawersError::NotAnEntry(doc_id), "{ctx}");
                }
            }
            ModelOp::CrashedAddDoc {
                drawer,
                doc,
                access,
                point,
                recover,
            } => {
                let entry = model.roster_docs[drawer].get(&model.docs[doc]).copied();
                let reached = match point {
                    CrashPoint::DocGranted => entry.is_none_or(|held| held < access),
                    CrashPoint::RosterWritten => entry != Some(access),
                    CrashPoint::RosterRemoved | CrashPoint::DocsMembershipAdded => false,
                };
                let req = AddDoc {
                    capability,
                    command: cmd,
                    drawer_id: model.drawers[drawer].clone(),
                    doc_id: model.docs[doc].clone(),
                    access,
                };
                let recorded = RecordedCommand::AddDoc(req);
                let receipt = if reached {
                    crash_through(
                        node, &alice, &model, &home, point, recover, &recorded, capability, &ctx,
                    )
                    .await
                } else {
                    recorded.replay(node, capability).await.expect(&ctx)
                };
                if matches!((reached, recover), (true, Recovery::Boot)) {
                    for grants in &mut model.bob_grants {
                        grants.clear();
                    }
                }
                model.roster_docs[drawer].insert(model.docs[doc].clone(), access);
                model.commands.push((recorded, receipt));
            }
            ModelOp::CrashedRemoveDoc {
                drawer,
                doc,
                recover,
            } => {
                let doc_id = model.docs[doc].clone();
                let req = RemoveDoc {
                    capability,
                    command: cmd,
                    drawer_id: model.drawers[drawer].clone(),
                    doc_id: doc_id.clone(),
                };
                if model.roster_docs[drawer].remove(&doc_id).is_some() {
                    let recorded = RecordedCommand::RemoveDoc(req);
                    let receipt = crash_through(
                        node,
                        &alice,
                        &model,
                        &home,
                        CrashPoint::RosterRemoved,
                        recover,
                        &recorded,
                        capability,
                        &ctx,
                    )
                    .await;
                    if matches!(recover, Recovery::Boot) {
                        for grants in &mut model.bob_grants {
                            grants.clear();
                        }
                    }
                    model.commands.push((recorded, receipt));
                } else {
                    let res = node
                        .drawers()
                        .call(RequestContext::root(), req)
                        .await
                        .map_err(op);
                    assert_eq!(res.unwrap_err(), DrawersError::NotAnEntry(doc_id), "{ctx}");
                }
            }
            ModelOp::AddChild {
                parent,
                child,
                access,
            } => {
                let req = AddChild {
                    capability,
                    command: cmd,
                    parent_id: model.drawers[parent].clone(),
                    child_id: model.drawers[child].clone(),
                    access,
                };
                let res = node
                    .drawers()
                    .call(RequestContext::root(), req.clone())
                    .await
                    .map_err(op);
                if parent == child {
                    assert_eq!(res.unwrap_err(), DrawersError::SelfChild, "{ctx}");
                } else {
                    let receipt = res.expect(&ctx);
                    model.roster_children[parent].insert(model.drawers[child].clone(), access);
                    model
                        .commands
                        .push((RecordedCommand::AddChild(req), receipt));
                }
            }
            ModelOp::RemoveChild { parent, child } => {
                let child_id = model.drawers[child].clone();
                let req = RemoveChild {
                    capability,
                    command: cmd,
                    parent_id: model.drawers[parent].clone(),
                    child_id: child_id.clone(),
                };
                let res = node
                    .drawers()
                    .call(RequestContext::root(), req.clone())
                    .await
                    .map_err(op);
                if model.roster_children[parent].remove(&child_id).is_some() {
                    let receipt = res.expect(&ctx);
                    model
                        .commands
                        .push((RecordedCommand::RemoveChild(req), receipt));
                } else {
                    assert_eq!(
                        res.unwrap_err(),
                        DrawersError::NotAnEntry(child_id),
                        "{ctx}"
                    );
                }
            }
            ModelOp::Grant { drawer, access } => {
                node.grant(&alice, &model.drawers[drawer], &bob, access)
                    .await;
                model.bob_grants[drawer].insert(access);
            }
            ModelOp::Revoke { drawer } => {
                node.drawers()
                    .call(
                        RequestContext::root(),
                        RevokeDrawer {
                            capability,
                            drawer_id: model.drawers[drawer].clone(),
                            subject_id: bob.actor_id,
                        },
                    )
                    .await
                    .unwrap();
                model.bob_grants[drawer].clear();
            }
            ModelOp::Remember { drawer, mirror } => {
                let drawer_id = model.drawers[drawer].clone();
                node.remember(&alice, &drawer_id, mirror).await;
                model.known.insert(drawer_id, mirror);
            }
            ModelOp::SetPolicy { drawer, mirror } => {
                let drawer_id = model.drawers[drawer].clone();
                let res = node
                    .drawers()
                    .call(
                        RequestContext::root(),
                        SetDrawerPolicy {
                            capability,
                            drawer_id: drawer_id.clone(),
                            mirror,
                        },
                    )
                    .await
                    .map_err(op);
                if let Some(entry) = model.known.get_mut(&drawer_id) {
                    res.expect(&ctx);
                    *entry = mirror;
                } else {
                    assert_eq!(
                        res.unwrap_err(),
                        DrawersError::UnknownDrawer(drawer_id),
                        "{ctx}"
                    );
                }
            }
            ModelOp::Forget { drawer } => {
                let drawer_id = model.drawers[drawer].clone();
                node.forget(&alice, &drawer_id).await;
                model.known.remove(&drawer_id);
            }
            ModelOp::Replay(seed) => {
                if !model.commands.is_empty() {
                    let (recorded, expected) = &model.commands[seed % model.commands.len()];
                    assert_eq!(
                        &recorded.replay(node, capability).await.unwrap(),
                        expected,
                        "{ctx}"
                    );
                }
            }
            ModelOp::Restart => {
                node.restart().await.unwrap();
                for grants in &mut model.bob_grants {
                    grants.clear();
                }
            }
        }
        model.check(node, &alice, &bob, &ctx).await;
    }
}

#[tokio::test]
async fn deterministic_drawer_model_scenarios_match_reference() {
    let mut node = Node::boot().await;

    // Scenario 1: Diamond + cycle hierarchy with weakest-link attenuation, strongest-path
    // winning, known-drawer lifecycle, replay, and restart.
    let diamond_and_cycle = [
        ModelOp::AddDoc {
            drawer: 2,
            doc: 0,
            access: Access::Write,
        },
        ModelOp::AddDoc {
            drawer: 1,
            doc: 1,
            access: Access::Read,
        },
        // Link D0 -> D1 (Read) -> D2 (Write), and D2 -> D0 (Relay) forming a cycle.
        ModelOp::AddChild {
            parent: 0,
            child: 1,
            access: Access::Read,
        },
        ModelOp::AddChild {
            parent: 1,
            child: 2,
            access: Access::Write,
        },
        ModelOp::AddChild {
            parent: 2,
            child: 0,
            access: Access::Relay,
        },
        ModelOp::AddChild {
            parent: 1,
            child: 1,
            access: Access::Admin,
        },
        // Grant Bob Write on D0: reaches D1 at Read, D2 at Read (capped by D0->D1 Read).
        ModelOp::Grant {
            drawer: 0,
            access: Access::Write,
        },
        // Add direct link D0 -> D2 at Write: now Bob reaches D2 at Write (strongest path wins)!
        ModelOp::AddChild {
            parent: 0,
            child: 2,
            access: Access::Write,
        },
        ModelOp::Remember {
            drawer: 0,
            mirror: false,
        },
        ModelOp::SetPolicy {
            drawer: 0,
            mirror: true,
        },
        ModelOp::SetPolicy {
            drawer: 2,
            mirror: false,
        },
        ModelOp::Restart,
        ModelOp::Replay(0),
        ModelOp::Replay(2),
        // Remove direct link D0 -> D2: Bob drops back to Read on D2 via D0 -> D1 -> D2.
        ModelOp::RemoveChild {
            parent: 0,
            child: 2,
        },
        ModelOp::Forget { drawer: 0 },
        ModelOp::Forget { drawer: 0 },
        ModelOp::Revoke { drawer: 0 },
        ModelOp::RemoveDoc { drawer: 2, doc: 0 },
        ModelOp::RemoveDoc { drawer: 2, doc: 0 },
    ];
    run_script(&mut node, "diamond-cycle", &diamond_and_cycle).await;

    // Scenario 2: every crash boundary, finished both ways, converges on what a clean operation
    // would have left; Bob's reach and the notes he reads follow the recovered rosters.
    let crashes = [
        ModelOp::Grant {
            drawer: 0,
            access: Access::Write,
        },
        ModelOp::CrashedAddDoc {
            drawer: 0,
            doc: 0,
            access: Access::Write,
            point: CrashPoint::DocGranted,
            recover: Recovery::Retry,
        },
        ModelOp::CrashedAddDoc {
            drawer: 0,
            doc: 1,
            access: Access::Read,
            point: CrashPoint::RosterWritten,
            recover: Recovery::Boot,
        },
        ModelOp::Grant {
            drawer: 0,
            access: Access::Write,
        },
        // Narrowing an entry crashes after the roster write; widening crashes after the grant.
        ModelOp::CrashedAddDoc {
            drawer: 0,
            doc: 0,
            access: Access::Read,
            point: CrashPoint::RosterWritten,
            recover: Recovery::Retry,
        },
        ModelOp::CrashedAddDoc {
            drawer: 0,
            doc: 1,
            access: Access::Write,
            point: CrashPoint::DocGranted,
            recover: Recovery::Boot,
        },
        ModelOp::Grant {
            drawer: 0,
            access: Access::Write,
        },
        // An entry already standing where asked never reaches either point.
        ModelOp::CrashedAddDoc {
            drawer: 0,
            doc: 1,
            access: Access::Write,
            point: CrashPoint::RosterWritten,
            recover: Recovery::Retry,
        },
        ModelOp::Replay(1),
        ModelOp::CrashedRemoveDoc {
            drawer: 0,
            doc: 0,
            recover: Recovery::Retry,
        },
        ModelOp::CrashedRemoveDoc {
            drawer: 0,
            doc: 1,
            recover: Recovery::Boot,
        },
        ModelOp::CrashedRemoveDoc {
            drawer: 0,
            doc: 1,
            recover: Recovery::Retry,
        },
        ModelOp::Replay(3),
    ];
    run_script(&mut node, "crashes", &crashes).await;

    // A parent's docs group reaches its child's docs through the child link, and that reach is
    // not an entry: removing it from the parent is refused, and entering it in the parent still
    // grants the parent's own delegation, which survives the link going away.
    let reach_through_child = [
        ModelOp::AddDoc {
            drawer: 0,
            doc: 0,
            access: Access::Read,
        },
        ModelOp::AddChild {
            parent: 1,
            child: 0,
            access: Access::Read,
        },
        ModelOp::RemoveDoc { drawer: 1, doc: 0 },
        ModelOp::AddDoc {
            drawer: 1,
            doc: 0,
            access: Access::Read,
        },
        ModelOp::AddDoc {
            drawer: 0,
            doc: 1,
            access: Access::Admin,
        },
        ModelOp::AddDoc {
            drawer: 1,
            doc: 1,
            access: Access::Relay,
        },
        ModelOp::RemoveChild {
            parent: 1,
            child: 0,
        },
        ModelOp::Grant {
            drawer: 1,
            access: Access::Read,
        },
        ModelOp::RemoveDoc { drawer: 0, doc: 0 },
    ];
    run_script(&mut node, "reach-through-child", &reach_through_child).await;
    node.stop().await;
}

fn any_access() -> impl Strategy<Value = Access> {
    prop_oneof![
        Just(Access::Relay),
        Just(Access::Read),
        Just(Access::Write),
        Just(Access::Admin),
    ]
}

fn any_recovery() -> impl Strategy<Value = Recovery> {
    prop_oneof![Just(Recovery::Boot), Just(Recovery::Retry)]
}

fn any_model_op() -> impl Strategy<Value = ModelOp> {
    prop_oneof![
        3 => (0..DRAWERS, 0..DOCS, any_access()).prop_map(|(drawer, doc, access)| {
            ModelOp::AddDoc { drawer, doc, access }
        }),
        2 => (0..DRAWERS, 0..DOCS).prop_map(|(drawer, doc)| ModelOp::RemoveDoc { drawer, doc }),
        1 => (
            0..DRAWERS,
            0..DOCS,
            any_access(),
            prop_oneof![Just(CrashPoint::DocGranted), Just(CrashPoint::RosterWritten)],
            any_recovery(),
        )
            .prop_map(|(drawer, doc, access, point, recover)| ModelOp::CrashedAddDoc {
                drawer,
                doc,
                access,
                point,
                recover,
            }),
        1 => (0..DRAWERS, 0..DOCS, any_recovery()).prop_map(|(drawer, doc, recover)| {
            ModelOp::CrashedRemoveDoc { drawer, doc, recover }
        }),
        3 => (0..DRAWERS, 0..DRAWERS, any_access()).prop_map(|(parent, child, access)| {
            ModelOp::AddChild { parent, child, access }
        }),
        2 => (0..DRAWERS, 0..DRAWERS).prop_map(|(parent, child)| {
            ModelOp::RemoveChild { parent, child }
        }),
        3 => (0..DRAWERS, any_access()).prop_map(|(drawer, access)| {
            ModelOp::Grant { drawer, access }
        }),
        2 => (0..DRAWERS).prop_map(|drawer| ModelOp::Revoke { drawer }),
        2 => (0..DRAWERS, any::<bool>()).prop_map(|(drawer, mirror)| {
            ModelOp::Remember { drawer, mirror }
        }),
        1 => (0..DRAWERS, any::<bool>()).prop_map(|(drawer, mirror)| {
            ModelOp::SetPolicy { drawer, mirror }
        }),
        1 => (0..DRAWERS).prop_map(|drawer| ModelOp::Forget { drawer }),
        1 => any::<usize>().prop_map(ModelOp::Replay),
        1 => Just(ModelOp::Restart),
    ]
}

static CASES: AtomicUsize = AtomicUsize::new(0);

proptest! {
    #![proptest_config(ProptestConfig { cases: 10, ..ProptestConfig::default() })]

    #[test]
    fn generated_drawer_scripts_match_reference_model_long_test(
        script in prop_vec(any_model_op(), 3..=10),
    ) {
        let case = CASES.fetch_add(1, Ordering::Relaxed);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("a tokio runtime");
        runtime.block_on(async {
            let mut node = Node::boot().await;
            run_script(&mut node, &format!("gen-{case}"), &script).await;
            node.stop().await;
        });
    }
}
