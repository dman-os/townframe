//! The header's facts: who took part and whether they are at it, where the work lives, and which
//! other intxts this one is tied to.

use crate::view::EntryKind;
use crate::view::IntxtView;
use crate::view::LinkedIntxt;
use jiff::SignedDuration;
use jiff::Timestamp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// An actor with activity this recently (before the view's `as_of`) is active now.
pub const ACTIVE_WINDOW: SignedDuration = SignedDuration::from_mins(10);

#[derive(Debug, Clone, PartialEq)]
pub struct Actor {
    pub name: String,
    /// The newest time any of the actor's items carries.
    pub last_active: Timestamp,
    pub active_now: bool,
}

/// A repository, branch or pull request the intxt's work lives in.
#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    pub label: String,
    pub href: Option<String>,
}

impl IntxtView {
    /// The people and agents who took part, by name, for what their items say: commit authors,
    /// a session's harness, a comment's creator. An actor none of whose items carries a time
    /// cannot be placed in time and is not listed.
    pub fn actors(&self) -> Vec<Actor> {
        let commits = self
            .entries
            .iter()
            .flat_map(|entry| &entry.commits)
            .map(|commit| (commit.author.as_str(), Some(commit.at)));
        let sessions = self
            .entries
            .iter()
            .filter_map(|entry| Some((entry.actor.as_deref()?, entry.at)));
        let comments = self
            .threads
            .iter()
            .flat_map(|thread| std::iter::once(&thread.root).chain(&thread.replies))
            .map(|comment| (comment.author.as_str(), comment.at));
        let mut last: BTreeMap<&str, Timestamp> = BTreeMap::new();
        for (name, at) in commits.chain(sessions).chain(comments) {
            if let Some(at) = at {
                let held = last.entry(name).or_insert(at);
                *held = (*held).max(at);
            }
        }
        last.into_iter()
            .map(|(name, last_active)| Actor {
                name: name.to_owned(),
                last_active,
                active_now: self.as_of.duration_since(last_active) <= ACTIVE_WINDOW,
            })
            .collect()
    }

    /// The newest time anything in the intxt carries.
    pub fn last_activity(&self) -> Option<Timestamp> {
        let entries = self.entries.iter().filter_map(|entry| entry.at);
        self.actors()
            .iter()
            .map(|actor| actor.last_active)
            .chain(entries)
            .max()
    }

    /// Repositories, branches and pull requests, taken from the entries: a pull request's URL
    /// names its repository, its `head` and a branch entry name branches. Each once, in the order
    /// the entries give them.
    pub fn sources(&self) -> Vec<Source> {
        let mut seen = BTreeSet::new();
        let mut sources = Vec::new();
        let mut add = |label: String, href: Option<String>| {
            if seen.insert(label.clone()) {
                sources.push(Source { label, href });
            }
        };
        for entry in &self.entries {
            match entry.kind {
                EntryKind::GithubPull => {
                    let Some(url) = &entry.href else { continue };
                    let head = entry.refs.iter().find(|held| held.name == "head");
                    let repo = url
                        .strip_prefix("https://github.com/")
                        .and_then(|rest| rest.split_once("/pull/"));
                    if let Some((repo, number)) = repo {
                        add(repo.to_owned(), Some(format!("https://github.com/{repo}")));
                        if let Some(head) = head {
                            add(head.value.clone(), None);
                        }
                        add(format!("{repo}#{number}"), Some(url.clone()));
                    } else {
                        if let Some(head) = head {
                            add(head.value.clone(), None);
                        }
                        add(entry.label.clone(), Some(url.clone()));
                    }
                }
                EntryKind::GitRef => add(entry.label.clone(), None),
                _ => {}
            }
        }
        sources
    }

    /// The other intxts this one is tied to. Today the only tie the data holds is a pull
    /// request's intxt block, so these are the intxts the block names besides this one.
    pub fn related(&self) -> Vec<&LinkedIntxt> {
        let mut seen = BTreeSet::new();
        self.entries
            .iter()
            .flat_map(|entry| &entry.pr_links.intxts)
            .filter(|link| link.drawer != self.drawer && seen.insert(link.drawer.as_str()))
            .collect()
    }
}
