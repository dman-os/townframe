//! intxt's datum vocabularies (ADR 036 core, ADR 040 work capture, ADR 041 annotations).
//!
//! References between datums are `dtxt:` URLs (ADR 035) carried as plain strings: the reference
//! types in `big_cabinet_core` still parse the `db+datum:` grammar ADR 035 replaces, so these
//! fields are not yet marked `x-automerge: reference`, and the reference index does not see
//! them until that migration lands.

pub mod annotation;
pub mod generic;
pub mod work;

use big_cabinet_core::datum::Declaration;
use big_cabinet_core::datum::Universe;
use std::sync::LazyLock;

pub fn declarations() -> Universe {
    Universe::collect([
        Declaration::of::<generic::Intxt>().expect("Intxt declares a valid datum"),
        Declaration::of::<generic::Title>().expect("Title declares a valid datum"),
        Declaration::of::<generic::PathName>().expect("PathName declares a valid datum"),
        Declaration::of::<generic::Note>().expect("Note declares a valid datum"),
        Declaration::of::<generic::Body>().expect("Body declares a valid datum"),
        Declaration::of::<annotation::Comment>().expect("Comment declares a valid datum"),
        Declaration::of::<annotation::ThreadState>().expect("ThreadState declares a valid datum"),
        Declaration::of::<annotation::Structure>().expect("Structure declares a valid datum"),
        Declaration::of::<annotation::Seen>().expect("Seen declares a valid datum"),
        Declaration::of::<annotation::AnnotationsEntry>()
            .expect("AnnotationsEntry declares a valid datum"),
        Declaration::of::<work::GitRepo>().expect("GitRepo declares a valid datum"),
        Declaration::of::<work::GitClone>().expect("GitClone declares a valid datum"),
        Declaration::of::<work::Worktree>().expect("Worktree declares a valid datum"),
        Declaration::of::<work::WorktreeOp>().expect("WorktreeOp declares a valid datum"),
        Declaration::of::<work::File>().expect("File declares a valid datum"),
        Declaration::of::<work::GitCommit>().expect("GitCommit declares a valid datum"),
        Declaration::of::<work::GitRef>().expect("GitRef declares a valid datum"),
        Declaration::of::<work::JjOperation>().expect("JjOperation declares a valid datum"),
        Declaration::of::<work::JjChange>().expect("JjChange declares a valid datum"),
        Declaration::of::<work::AgentSession>().expect("AgentSession declares a valid datum"),
        Declaration::of::<work::PromptTurn>().expect("PromptTurn declares a valid datum"),
        Declaration::of::<work::Plan>().expect("Plan declares a valid datum"),
        Declaration::of::<work::RepoEntry>().expect("RepoEntry declares a valid datum"),
        Declaration::of::<work::WorktreeEntry>().expect("WorktreeEntry declares a valid datum"),
        Declaration::of::<work::SessionEntry>().expect("SessionEntry declares a valid datum"),
    ])
}

pub static UNIVERSE: LazyLock<Universe> = LazyLock::new(declarations);

#[cfg(test)]
mod tests;
