use super::*;

impl<C: Clone> SurfaceTree<C> {
    fn charge(
        total: &mut usize,
        amount: usize,
        limit: usize,
        reason: &'static str,
    ) -> Result<(), PresentationError> {
        *total = total
            .checked_add(amount)
            .filter(|&next| next <= limit)
            .ok_or_else(|| PresentationError::Unsupported(reason.into()))?;
        Ok(())
    }

    fn charge_string(bytes: &mut usize, value: &str) -> Result<(), PresentationError> {
        if value.len() > Self::MAX_STRING_BYTES {
            return Err(PresentationError::Unsupported("string byte limit".into()));
        }
        Self::charge(
            bytes,
            value.len(),
            Self::MAX_BATCH_BYTES,
            "batch byte limit",
        )
    }

    fn charge_work(work: &mut usize, amount: usize) -> Result<(), PresentationError> {
        Self::charge(work, amount, Self::MAX_BATCH_WORK, "batch work limit")
    }

    fn check_capacity(
        &self,
        nodes: usize,
        removed_nodes: usize,
        bytes: usize,
    ) -> Result<(), PresentationError> {
        let mut slots = self.nodes.len() - self.vacant_slots.len() - removed_nodes;
        Self::charge(&mut slots, nodes, Self::MAX_NODE_SLOTS, "node slot limit")?;
        self.next_generation
            .checked_add(nodes as u64)
            .ok_or_else(|| PresentationError::Unsupported("node generation limit".into()))?;
        let mut retained = self.retained_bytes;
        Self::charge(
            &mut retained,
            bytes,
            Self::MAX_RETAINED_BYTES,
            "retained byte limit",
        )
    }

    pub(super) fn attribute_bytes(
        name: &str,
        namespace: Option<&str>,
        value: &AttributeValue,
    ) -> usize {
        name.len()
            + namespace.map_or(0, str::len)
            + match value {
                AttributeValue::Text(text) => text.len(),
                _ => 0,
            }
    }

    pub(super) fn kind_bytes(kind: &SurfaceNodeKind<C>) -> usize {
        match kind {
            SurfaceNodeKind::Root | SurfaceNodeKind::Component(_) => 0,
            SurfaceNodeKind::Text(text) => text.len(),
            SurfaceNodeKind::Element {
                tag,
                namespace,
                attributes,
                listeners,
                ..
            } => {
                tag.len()
                    + namespace.as_ref().map_or(0, String::len)
                    + attributes
                        .iter()
                        .map(|((name, ns), value)| {
                            Self::attribute_bytes(name, ns.as_deref(), value)
                        })
                        .sum::<usize>()
                    + listeners.iter().map(String::len).sum::<usize>()
            }
        }
    }

    fn subtree_stats(
        &self,
        id: SurfaceNodeId,
        work: &mut usize,
    ) -> Result<(usize, usize, usize), PresentationError> {
        let node = self.node(id);
        let mut count = 1;
        let mut bytes = Self::kind_bytes(&node.kind);
        Self::charge_work(work, 1 + bytes.div_ceil(64))?;
        let mut height = 1;
        for &child in &node.children {
            let (child_count, child_bytes, child_height) = self.subtree_stats(child, work)?;
            count += child_count;
            bytes += child_bytes;
            height = height.max(child_height + 1);
        }
        Ok((count, bytes, height))
    }

    pub(super) fn depth(
        &self,
        mut id: SurfaceNodeId,
        work: &mut usize,
    ) -> Result<usize, PresentationError> {
        let mut depth = 0;
        while let Some(parent) = self.node(id).parent {
            Self::charge_work(work, 1)?;
            depth += 1;
            id = parent;
        }
        Ok(depth)
    }

    fn validate_attachment(
        &self,
        mutation: &Mutation<C>,
        count: usize,
        work: &mut usize,
    ) -> Result<(), PresentationError> {
        let invalid = || PresentationError::InvalidBatch("invalid attachment or arity".into());
        if count >= self.stack.len() {
            return Err(invalid());
        }
        let split = self.stack.len() - count;
        let anchor = self.stack[split - 1].node;
        let replacing = matches!(mutation, Mutation::ReplaceWith(_));
        let parent = if matches!(mutation, Mutation::AppendChildren(_)) {
            if !matches!(
                self.node(anchor).kind,
                SurfaceNodeKind::Root | SurfaceNodeKind::Element { .. }
            ) {
                return Err(invalid());
            }
            anchor
        } else {
            if anchor == SurfaceNodeId::ROOT || (replacing && split <= 1) {
                return Err(invalid());
            }
            let parent = self.node(anchor).parent.ok_or_else(invalid)?;
            Self::charge_work(work, self.node(parent).children.len())?;
            if !self.node(parent).children.contains(&anchor) {
                return Err(invalid());
            }
            parent
        };
        let selected: BTreeSet<_> = self.stack[split..].iter().map(|entry| entry.node).collect();
        if selected.len() != count || selected.contains(&SurfaceNodeId::ROOT) {
            return Err(invalid());
        }
        let mut ancestor = Some(anchor);
        while let Some(id) = ancestor {
            Self::charge_work(work, 1)?;
            if selected.contains(&id) {
                return Err(invalid());
            }
            ancestor = self.node(id).parent;
        }
        let parent_depth = self.depth(parent, work)?;
        Self::charge_work(work, count)?;
        for &child in &selected {
            let mut ancestor = self.node(child).parent;
            while let Some(id) = ancestor {
                Self::charge_work(work, 1)?;
                if selected.contains(&id) {
                    return Err(invalid());
                }
                ancestor = self.node(id).parent;
            }
            if let Some(old_parent) = self.node(child).parent {
                Self::charge_work(work, self.node(old_parent).children.len())?;
            }
            let (_, _, height) = self.subtree_stats(child, work)?;
            if parent_depth + height > Self::MAX_TREE_DEPTH {
                return Err(PresentationError::Unsupported("tree depth limit".into()));
            }
        }
        if replacing {
            self.subtree_stats(anchor, work)?;
        }
        Ok(())
    }

    pub(super) fn validate_mutation(
        &self,
        mutation: &Mutation<C>,
        bytes: &mut usize,
        work: &mut usize,
    ) -> Result<(), PresentationError> {
        Self::charge_work(work, self.stack.len() + 1)?;
        Self::charge(
            bytes,
            std::mem::size_of::<Mutation<C>>(),
            Self::MAX_BATCH_BYTES,
            "batch byte limit",
        )?;
        let top = self.top().node;
        if matches!(
            mutation,
            Mutation::PushId(_)
                | Mutation::CreateElement(_)
                | Mutation::CreateText(_)
                | Mutation::MountComponent(_)
        ) && self.stack.len() >= Self::MAX_STACK_DEPTH
        {
            return Err(PresentationError::Unsupported("stack depth limit".into()));
        }
        match mutation {
            Mutation::PushId(id) | Mutation::SetId(id) => {
                self.validate_element_id(mutation, top, *id)
            }
            Mutation::Child(index) => {
                if self.stack.len() <= 1 || *index as usize >= self.node(top).children.len() {
                    return Err(invalid_target());
                }
                Ok(())
            }
            Mutation::Pop => {
                if self.stack.len() <= 1 {
                    return Err(invalid_target());
                }
                Ok(())
            }
            Mutation::CreateElement((tag, ns)) => {
                self.validate_create_element(tag, ns.as_deref(), bytes)
            }
            Mutation::CreateText(text) => self.validate_create_text(text, bytes),
            Mutation::MountComponent(_) => self.check_capacity(1, 0, 0),
            Mutation::CloneNode => self.validate_clone(top, work),
            Mutation::AppendChildren(count)
            | Mutation::ReplaceWith(count)
            | Mutation::InsertAfter(count)
            | Mutation::InsertBefore(count) => {
                self.validate_attachment(mutation, *count as usize, work)
            }
            Mutation::SetAttribute((name, ns, value)) => {
                self.validate_attribute(top, name, ns.as_deref(), value, bytes, work)
            }
            Mutation::SetText(text) => self.validate_text(top, text, bytes, work),
            Mutation::AddEventListener(name) | Mutation::RemoveEventListener(name) => {
                self.validate_listener(top, mutation, name, bytes)
            }
            Mutation::Remove => self.validate_remove(top, work),
        }
    }

    fn validate_element_id(
        &self,
        mutation: &Mutation<C>,
        top: SurfaceNodeId,
        id: NodeId,
    ) -> Result<(), PresentationError> {
        if id > Self::MAX_ELEMENT_ID {
            return Err(PresentationError::Unsupported("element id limit".into()));
        }
        if matches!(mutation, Mutation::PushId(_)) {
            if self
                .element_to_node
                .get(id as usize)
                .copied()
                .flatten()
                .and_then(|node| self.live_node(node))
                .is_none()
            {
                return Err(invalid_target());
            }
        } else if (id == 0) != (top == SurfaceNodeId::ROOT) {
            return Err(invalid_target());
        }
        Ok(())
    }

    fn validate_create_element(
        &self,
        tag: &str,
        ns: Option<&str>,
        bytes: &mut usize,
    ) -> Result<(), PresentationError> {
        Self::charge_string(bytes, tag)?;
        if let Some(ns) = ns {
            Self::charge_string(bytes, ns)?;
        }
        if !supported_tag(tag, ns) {
            return Err(PresentationError::Unsupported(
                "element tag or namespace".into(),
            ));
        }
        self.check_capacity(1, 0, tag.len() + ns.map_or(0, str::len))
    }

    fn validate_create_text(&self, text: &str, bytes: &mut usize) -> Result<(), PresentationError> {
        Self::charge_string(bytes, text)?;
        self.check_capacity(1, 0, text.len())
    }

    fn validate_clone(
        &self,
        top: SurfaceNodeId,
        work: &mut usize,
    ) -> Result<(), PresentationError> {
        if top == SurfaceNodeId::ROOT || self.stack.len() <= 1 {
            return Err(invalid_target());
        }
        let (count, bytes, _) = self.subtree_stats(top, work)?;
        self.check_capacity(count, 0, bytes)
    }

    fn validate_attribute(
        &self,
        top: SurfaceNodeId,
        name: &str,
        ns: Option<&str>,
        value: &AttributeValue,
        bytes: &mut usize,
        work: &mut usize,
    ) -> Result<(), PresentationError> {
        Self::charge_string(bytes, name)?;
        if let Some(ns) = ns {
            Self::charge_string(bytes, ns)?;
        }
        if let AttributeValue::Text(text) = value {
            Self::charge_string(bytes, text)?;
        }
        if static_attribute(name, ns).is_none() {
            return Err(PresentationError::Unsupported(
                "attribute name or namespace".into(),
            ));
        }
        if matches!(value, AttributeValue::Float(float) if !float.is_finite()) {
            return Err(PresentationError::Unsupported(
                "non-finite attribute".into(),
            ));
        }
        let SurfaceNodeKind::Element { attributes, .. } = &self.node(top).kind else {
            return Err(invalid_target());
        };
        Self::charge_work(work, attributes.len())?;
        let existing = attributes
            .iter()
            .find(|((old_name, old_ns), _)| old_name == name && old_ns.as_deref() == ns);
        if !matches!(value, AttributeValue::None | AttributeValue::Boolean(false)) {
            if existing.is_none() && attributes.len() >= Self::MAX_ATTRIBUTES_PER_NODE {
                return Err(PresentationError::Unsupported(
                    "attribute count limit".into(),
                ));
            }
            let previous = existing.map_or(0, |(_, value)| Self::attribute_bytes(name, ns, value));
            self.check_capacity(
                0,
                0,
                Self::attribute_bytes(name, ns, value).saturating_sub(previous),
            )?;
        }
        Ok(())
    }

    fn validate_text(
        &self,
        top: SurfaceNodeId,
        text: &str,
        bytes: &mut usize,
        work: &mut usize,
    ) -> Result<(), PresentationError> {
        Self::charge_string(bytes, text)?;
        let node = self.node(top);
        match &node.kind {
            SurfaceNodeKind::Root | SurfaceNodeKind::Component(_) => Err(invalid_target()),
            SurfaceNodeKind::Text(previous) => {
                self.check_capacity(0, 0, text.len().saturating_sub(previous.len()))
            }
            SurfaceNodeKind::Element { .. } => {
                let mut previous_nodes = 0;
                let mut previous_bytes = 0;
                for &child in &node.children {
                    let (count, bytes, _) = self.subtree_stats(child, work)?;
                    previous_nodes += count;
                    previous_bytes += bytes;
                }
                self.check_capacity(1, previous_nodes, text.len().saturating_sub(previous_bytes))?;
                if self.depth(top, work)? + 1 > Self::MAX_TREE_DEPTH {
                    return Err(PresentationError::Unsupported("tree depth limit".into()));
                }
                Ok(())
            }
        }
    }

    fn validate_listener(
        &self,
        top: SurfaceNodeId,
        mutation: &Mutation<C>,
        name: &str,
        bytes: &mut usize,
    ) -> Result<(), PresentationError> {
        Self::charge_string(bytes, name)?;
        if !supported_listener(name) {
            return Err(PresentationError::Unsupported("event listener".into()));
        }
        let node = self.node(top);
        let SurfaceNodeKind::Element {
            listeners,
            element_id,
            ..
        } = &node.kind
        else {
            return Err(invalid_target());
        };
        if matches!(mutation, Mutation::AddEventListener(_)) {
            let id = element_id.ok_or_else(invalid_target)?;
            if self.element_to_node.get(id.raw()).copied().flatten() != Some(top) {
                return Err(invalid_target());
            }
            if !listeners.contains(name) {
                if listeners.len() >= Self::MAX_LISTENERS_PER_NODE {
                    return Err(PresentationError::Unsupported(
                        "listener count limit".into(),
                    ));
                }
                self.check_capacity(0, 0, name.len())?;
            }
        }
        Ok(())
    }

    fn validate_remove(
        &self,
        top: SurfaceNodeId,
        work: &mut usize,
    ) -> Result<(), PresentationError> {
        if top == SurfaceNodeId::ROOT || self.stack.len() <= 1 {
            return Err(invalid_target());
        }
        self.subtree_stats(top, work)?;
        if let Some(parent) = self.node(top).parent {
            Self::charge_work(work, self.node(parent).children.len())?;
        }
        Ok(())
    }
}

fn invalid_target() -> PresentationError {
    PresentationError::InvalidBatch("invalid mutation target or stack".into())
}
