//! The guide: an intxt's `structure` datums (ADR 041 §4) as the author's ordered outline.

use crate::view::TargetView;
use crate::view::slot_time;
use crate::view::spelling;
use crate::view::target_view;
use big_cabinet_core::DatumReference;
use intxt_core::annotation::Structure;
use jiff::Timestamp;

/// A `structure` datum and the reference that names it, which its children's `parent` point at.
#[derive(Debug, Clone, Copy)]
pub struct StructureDatum<'a> {
    pub id: &'a DatumReference,
    pub structure: &'a Structure,
}

/// One entry of the guide, listed in reading order (a parent before its children).
#[derive(Debug, Clone, PartialEq)]
pub struct GuideSection {
    /// Nesting below the guide's top level, 0 for a top-level section.
    pub depth: usize,
    /// `section`, `task`, `decision` or whatever the author's vocabulary says.
    pub kind: String,
    pub title: String,
    /// Markdown source.
    pub body: Option<String>,
    /// The hunks the section points at.
    pub targets: Vec<TargetView>,
    /// From the datum's UUIDv7 slot.
    pub at: Option<Timestamp>,
}

/// The guide for `structures`: siblings sorted by their `order` (then by key, so the same data
/// always lists the same way), children under their parent. A structure whose parent is not among
/// them stands at the top level rather than disappearing, and so does one on a `parent` cycle in
/// unvalidated data, which is listed once.
pub fn build(structures: &[StructureDatum<'_>]) -> Vec<GuideSection> {
    let mut guide = Vec::with_capacity(structures.len());
    let mut visited = vec![false; structures.len()];
    let known = |parent: &DatumReference| structures.iter().any(|other| other.id == parent);
    let roots = (0..structures.len())
        .filter(|&at| {
            structures[at]
                .structure
                .parent
                .as_ref()
                .is_none_or(|parent| !known(parent))
        })
        .collect();
    walk(structures, roots, 0, &mut visited, &mut guide);
    // Only a `parent` cycle leaves structures unreached; they stand at the top level.
    let cycles = (0..structures.len()).filter(|&at| !visited[at]).collect();
    walk(structures, cycles, 0, &mut visited, &mut guide);
    guide
}

/// Lists `siblings` (indices into `structures`) in order, each followed by its children.
fn walk(
    structures: &[StructureDatum<'_>],
    mut siblings: Vec<usize>,
    depth: usize,
    visited: &mut [bool],
    guide: &mut Vec<GuideSection>,
) {
    siblings.sort_by(|&left, &right| {
        let (left, right) = (&structures[left], &structures[right]);
        (&left.structure.order, left.id.key()).cmp(&(&right.structure.order, right.id.key()))
    });
    for at in siblings {
        if std::mem::replace(&mut visited[at], true) {
            continue;
        }
        let datum = &structures[at];
        let structure = datum.structure;
        guide.push(GuideSection {
            depth,
            kind: spelling(&structure.kind),
            title: structure.title.clone(),
            body: structure.body.as_ref().map(|body| body.content.clone()),
            targets: structure.targets.iter().map(target_view).collect(),
            at: slot_time(datum.id.key()),
        });
        let children = (0..structures.len())
            .filter(|&child| structures[child].structure.parent.as_ref() == Some(datum.id))
            .collect();
        walk(structures, children, depth + 1, visited, guide);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn id(slot: &str) -> DatumReference {
        format!("dtxt:datum:z11111111111111111111111111111111/org.example.intxt.structure/{slot}")
            .parse()
            .unwrap()
    }

    fn structure(title: &str, order: Option<&str>, parent: Option<&str>) -> Structure {
        serde_json::from_value(json!({
            "kind": "section", "title": title, "order": order,
            "parent": parent.map(|slot| id(slot).to_string()),
        }))
        .unwrap()
    }

    #[test]
    fn sections_list_by_order_with_children_under_their_parent() {
        let all = [
            (id("c"), structure("second", Some("2"), None)),
            (id("a"), structure("first", Some("1"), None)),
            (id("b"), structure("first, child", Some("1"), Some("a"))),
            (id("d"), structure("orphan", Some("0"), Some("gone"))),
            (id("e"), structure("looped", None, Some("e"))),
        ];
        let datums: Vec<_> = all
            .iter()
            .map(|(id, structure)| StructureDatum { id, structure })
            .collect();
        let guide: Vec<_> = build(&datums)
            .into_iter()
            .map(|section| (section.depth, section.title))
            .collect();
        // The looped one is its own parent, so no root reaches it: it stands last, once.
        assert_eq!(
            guide,
            [
                (0, "orphan".to_owned()),
                (0, "first".to_owned()),
                (1, "first, child".to_owned()),
                (0, "second".to_owned()),
                (0, "looped".to_owned()),
            ]
        );
    }
}
