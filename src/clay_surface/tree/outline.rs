use super::*;

impl<C: Clone> SurfaceTree<C> {
    /// A compact structural summary of the tree: `tag[attribute names](children)` (the root, which has no tag, is just its parenthesized
    /// children), with `#` for
    /// a text node and `<slot>` for a component slot. For tests and diagnostics that assert the
    /// shape of what a host renders without spelling out its contents.
    pub fn outline(&self) -> String {
        fn walk<C: Clone>(tree: &SurfaceTree<C>, id: SurfaceNodeId, out: &mut String) {
            let node = tree.node(id);
            match &node.kind {
                SurfaceNodeKind::Root => {}
                SurfaceNodeKind::Text(_) => out.push('#'),
                SurfaceNodeKind::Component(_) => out.push_str("<slot>"),
                SurfaceNodeKind::Element {
                    tag, attributes, ..
                } => {
                    out.push_str(tag);
                    let names: Vec<&str> =
                        attributes.keys().map(|(name, _)| name.as_str()).collect();
                    if !names.is_empty() {
                        out.push('[');
                        out.push_str(&names.join(","));
                        out.push(']');
                    }
                }
            }
            if !node.children.is_empty() {
                out.push('(');
                for (index, &child) in node.children.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    walk(tree, child, out);
                }
                out.push(')');
            }
        }
        let mut out = String::new();
        walk(self, SurfaceNodeId::ROOT, &mut out);
        out
    }
}
