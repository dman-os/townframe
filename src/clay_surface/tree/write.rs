use super::*;

impl<C: Clone> WriteMutations for SurfaceTree<C> {
    fn push_id(&mut self, id: ElementId) {
        let node = self.lookup(id);
        self.stack.push(StackEntry {
            node,
            element_id: Some(id),
        });
    }

    fn set_id(&mut self, id: ElementId) {
        let node = self.top().node;
        self.set_mapping(id, node);
        if let Some(top) = self.stack.last_mut() {
            top.element_id = Some(id);
        }
    }

    fn child(&mut self, index: usize) {
        let parent = self.top().node;
        let child = self.node(parent).children[index];
        let element_id = match &self.node(child).kind {
            SurfaceNodeKind::Element { element_id, .. } => *element_id,
            _ => None,
        };
        *self
            .stack
            .last_mut()
            .expect("surface stack unexpectedly empty") = StackEntry {
            node: child,
            element_id,
        };
    }

    fn pop(&mut self) {
        self.pop_entry();
    }

    fn create_element(&mut self, tag: &str, ns: Option<&str>) {
        let node = self.alloc_node(SurfaceNodeKind::Element {
            tag: tag.to_owned(),
            namespace: ns.map(str::to_owned),
            attributes: BTreeMap::new(),
            listeners: BTreeSet::new(),
            element_id: None,
        });
        self.stack.push(StackEntry {
            node,
            element_id: None,
        });
    }

    fn create_text(&mut self, value: &str) {
        let node = self.alloc_node(SurfaceNodeKind::Text(value.to_owned()));
        self.stack.push(StackEntry {
            node,
            element_id: None,
        });
    }

    fn clone(&mut self) {
        let source = self.top().node;
        let cloned = self.deep_clone(source);
        *self
            .stack
            .last_mut()
            .expect("surface stack unexpectedly empty") = StackEntry {
            node: cloned,
            element_id: None,
        };
    }

    fn append_children(&mut self, m: usize) {
        let children = self.pop_nodes(m);
        let parent = self.top().node;
        for &child in &children {
            self.detach_from_parent(child);
            self.node_mut(child).parent = Some(parent);
        }
        self.node_mut(parent).children.extend(children);
    }

    fn replace_with(&mut self, m: usize) {
        let replacements = self.pop_nodes(m);
        let target = self.pop_entry();
        for &node in &replacements {
            self.detach_from_parent(node);
        }
        let parent = self
            .node(target.node)
            .parent
            .expect("replaced surface node must have a parent");
        for &node in &replacements {
            self.node_mut(node).parent = Some(parent);
        }
        let siblings = &mut self.node_mut(parent).children;
        let pos = siblings
            .iter()
            .position(|&existing| existing == target.node)
            .expect("target node must be in parent's children");
        siblings.splice(pos..=pos, replacements);
        self.drop_subtree(target.node);
    }

    fn insert_after(&mut self, m: usize) {
        let nodes = self.pop_nodes(m);
        let anchor = self.top().node;
        for &node in &nodes {
            self.detach_from_parent(node);
        }
        let parent = self
            .node(anchor)
            .parent
            .expect("anchor surface node must have a parent");
        for &node in &nodes {
            self.node_mut(node).parent = Some(parent);
        }
        let siblings = &mut self.node_mut(parent).children;
        let pos = siblings
            .iter()
            .position(|&existing| existing == anchor)
            .expect("anchor node must be in parent's children");
        siblings.splice(pos + 1..pos + 1, nodes);
    }

    fn insert_before(&mut self, m: usize) {
        let nodes = self.pop_nodes(m);
        let anchor = self.top().node;
        for &node in &nodes {
            self.detach_from_parent(node);
        }
        let parent = self
            .node(anchor)
            .parent
            .expect("anchor surface node must have a parent");
        for &node in &nodes {
            self.node_mut(node).parent = Some(parent);
        }
        let siblings = &mut self.node_mut(parent).children;
        let pos = siblings
            .iter()
            .position(|&existing| existing == anchor)
            .expect("anchor node must be in parent's children");
        siblings.splice(pos..pos, nodes);
    }

    fn set_attribute(&mut self, name: &str, ns: Option<&str>, value: BorrowedAttributeValue<'_>) {
        let node = self.top().node;
        let SurfaceNodeKind::Element { attributes, .. } = &mut self.node_mut(node).kind else {
            panic!("set_attribute called on non-element surface node");
        };
        let key = (name.to_owned(), ns.map(str::to_owned));
        let previous_bytes = attributes
            .get(&key)
            .map_or(0, |value| Self::attribute_bytes(name, ns, value));
        match value {
            BorrowedAttributeValue::None | BorrowedAttributeValue::Bool(false) => {
                attributes.remove(&key);
            }
            BorrowedAttributeValue::Text(text) => {
                attributes.insert(key, AttributeValue::Text(text.to_owned()));
            }
            BorrowedAttributeValue::Float(float) => {
                attributes.insert(key, AttributeValue::Float(float));
            }
            BorrowedAttributeValue::Int(int) => {
                attributes.insert(key, AttributeValue::Int(int));
            }
            BorrowedAttributeValue::Bool(true) => {
                attributes.insert(key, AttributeValue::Boolean(true));
            }
            BorrowedAttributeValue::Any(_) => {
                panic!("`Any` attribute `{name}` cannot cross the presentation boundary")
            }
        }
        let SurfaceNodeKind::Element { attributes, .. } = &self.node(node).kind else {
            unreachable!()
        };
        let next_bytes = attributes
            .iter()
            .find(|((key_name, key_ns), _)| key_name == name && key_ns.as_deref() == ns)
            .map_or(0, |(_, value)| Self::attribute_bytes(name, ns, value));
        self.retained_bytes = self.retained_bytes - previous_bytes + next_bytes;
    }

    fn set_text(&mut self, value: &str) {
        let node = self.top().node;
        match &mut self.node_mut(node).kind {
            SurfaceNodeKind::Text(text) => {
                let previous_bytes = text.len();
                *text = value.to_owned();
                self.retained_bytes = self.retained_bytes - previous_bytes + value.len();
            }
            SurfaceNodeKind::Element { .. } => {
                let children = self.node(node).children.clone();
                for child in children {
                    self.drop_subtree(child);
                }
                let text_node = self.alloc_node(SurfaceNodeKind::Text(value.to_owned()));
                self.node_mut(text_node).parent = Some(node);
                self.node_mut(node).children = vec![text_node];
            }
            SurfaceNodeKind::Root | SurfaceNodeKind::Component(_) => {
                panic!("set_text called on a non-DOM surface node")
            }
        }
    }

    fn add_event_listener(&mut self, name: &str) {
        let entry = self.top();
        let element_id = entry
            .element_id
            .filter(|id| self.element_to_node.get(id.raw()).copied().flatten() == Some(entry.node))
            .or_else(|| match &self.node(entry.node).kind {
                SurfaceNodeKind::Element { element_id, .. } => *element_id,
                _ => None,
            })
            .expect("event listener target must be mapped to an ElementId");
        self.set_mapping(element_id, entry.node);
        let SurfaceNodeKind::Element { listeners, .. } = &mut self.node_mut(entry.node).kind else {
            panic!("add_event_listener called on non-element surface node");
        };
        if listeners.insert(name.to_owned()) {
            self.retained_bytes += name.len();
        }
    }

    fn remove_event_listener(&mut self, name: &str) {
        let node = self.top().node;
        if let SurfaceNodeKind::Element { listeners, .. } = &mut self.node_mut(node).kind
            && listeners.remove(name)
        {
            self.retained_bytes -= name.len();
        }
    }

    fn remove(&mut self) {
        let entry = self.pop_entry();
        self.detach_from_parent(entry.node);
        self.drop_subtree(entry.node);
    }
}
