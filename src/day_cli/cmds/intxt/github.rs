//! `day x intxt github pull`: a GitHub pull request onto the node, so a micro (which has no
//! egress) can list and read it. The CLI fetches it with the user's own `gh` credentials, shelling
//! out to `gh api`, and writes a `githubPull/<owner>/<repo>/<number>` datum into the pull
//! request's own document. The node holds no token.
//!
//! The documents live in one node-level "github" drawer, made on first use. Its id and each pull
//! request's document id are remembered in `<node dir>/intxt/github.json`, as the capture
//! drawer's are in `capture.json`. A pull request has a document of its own so that an intxt
//! which names it can share exactly that document and not the other pull requests of the
//! repository.

use super::capture::write_atomically;
use super::package::Packaging;
use super::package::shown;
use crate::interlude::*;

use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::drawer::protocol::drawer_placement;
use big_cabinet_core::reference::DatumReference;
use big_cabinet_core::reference::ReferenceDocId;
use intxt_core::client::key;
use intxt_core::client::to_value;
use intxt_core::generic;
use intxt_core::work;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::Write;

/// The pull request a command line names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PullRef {
    pub owner: String,
    pub repo: String,
    pub number: u64,
}

impl PullRef {
    /// `owner/repo#12`, or a pull request's URL (`https://github.com/owner/repo/pull/12`, with
    /// any trailing path, query or fragment such as `/files`).
    pub(crate) fn parse(text: &str) -> Res<Self> {
        let usage = || {
            eyre::eyre!(
                "`{text}` names no pull request: write `<owner>/<repo>#<number>` or its GitHub URL"
            )
        };
        let (path, number) = if let Some(url) = text
            .strip_prefix("https://github.com/")
            .or_else(|| text.strip_prefix("http://github.com/"))
        {
            let url = url.split(['?', '#']).next().unwrap_or_default();
            let mut parts = url.split('/');
            let (Some(owner), Some(repo), Some("pull"), Some(number)) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else {
                return Err(usage());
            };
            (format!("{owner}/{repo}"), number)
        } else {
            let (path, number) = text.split_once('#').ok_or_else(usage)?;
            (path.to_owned(), number)
        };
        let (owner, repo) = path.split_once('/').ok_or_else(usage)?;
        // GitHub's own charset, which also keeps the slot and the `gh api` path unambiguous.
        let name = |part: &str| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        };
        eyre::ensure!(name(owner) && name(repo), "{}", usage());
        let number: u64 = number.parse().map_err(|_| usage())?;
        eyre::ensure!(number > 0, "{}", usage());
        Ok(Self {
            owner: owner.to_owned(),
            repo: repo.to_owned(),
            number,
        })
    }

    /// `<owner>/<repo>#<number>`, how the state file and the printout name it.
    fn name(&self) -> String {
        format!("{}/{}#{}", self.owner, self.repo, self.number)
    }

    fn slot(&self) -> String {
        format!("{}/{}/{}", self.owner, self.repo, self.number)
    }
}

/// `gh api repos/<owner>/<repo>/pulls/<number>`: the user's own GitHub credentials, through the
/// GitHub CLI. A missing or logged-out `gh` is a user error that says what to do.
pub(crate) async fn fetch_with_gh(pull: &PullRef) -> Res<Value> {
    let endpoint = format!("repos/{}/{}/pulls/{}", pull.owner, pull.repo, pull.number);
    let output = tokio::process::Command::new("gh")
        .args(["api", &endpoint])
        .output()
        .await
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => eyre::eyre!(
                "the GitHub CLI `gh` is not installed or not on PATH; install it from https://cli.github.com and run `gh auth login`"
            ),
            _ => eyre::Report::new(error).wrap_err("running `gh`"),
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();
        if stderr.contains("gh auth login") || stderr.contains("authentication") {
            eyre::bail!("`gh` is not logged in to GitHub: run `gh auth login` ({stderr})");
        }
        eyre::bail!("`gh api {endpoint}` failed: {stderr}");
    }
    serde_json::from_slice(&output.stdout).wrap_err("`gh api` did not answer JSON")
}

/// The datum `gh api`'s answer says: a merged PR is a closed one with a merge time.
pub(crate) fn pull_datum(answer: &Value) -> Res<work::GithubPull> {
    let text = |path: &[&str]| -> Res<String> {
        let mut at = answer;
        for step in path {
            at = &at[step];
        }
        at.as_str()
            .map(str::to_owned)
            .ok_or_else(|| eyre::eyre!("the pull request has no `{}`", path.join(".")))
    };
    let state = match (text(&["state"])?.as_str(), answer["merged_at"].is_null()) {
        ("closed", false) => work::PullState::Merged,
        ("open", _) => work::PullState::Open,
        ("closed", true) => work::PullState::Closed,
        (other, _) => work::PullState::Unknown(other.to_owned()),
    };
    Ok(work::GithubPull {
        title: text(&["title"])?,
        // A PR with no description has a null body.
        body: answer["body"].as_str().unwrap_or_default().to_owned(),
        state,
        head: text(&["head", "ref"])?,
        base: text(&["base", "ref"])?,
        head_sha: text(&["head", "sha"])?,
        base_sha: text(&["base", "sha"])?,
        updated_at: text(&["updated_at"])?
            .parse()
            .wrap_err("the pull request's `updated_at`")?,
        url: text(&["html_url"])?,
        extra: Default::default(),
    })
}

/// The node's GitHub drawer and its documents, in `<node dir>/intxt/github.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GithubNode {
    drawer: DrawerId,
    /// `<owner>/<repo>#<number>` → the pull request's document. A missing entry is recovered by
    /// the idempotent creation, which answers the first one.
    pulls: BTreeMap<String, DocId>,
}

impl Packaging<'_> {
    fn github_path(&self) -> PathBuf {
        self.node_dir.join("intxt/github.json")
    }

    fn load_github(&self) -> Res<Option<GithubNode>> {
        let path = self.github_path();
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(
                serde_json::from_slice(&bytes)
                    .wrap_err_with(|| format!("parsing {}", path.display()))?,
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).wrap_err_with(|| format!("reading {}", path.display())),
        }
    }

    fn save_github(&self, node: &GithubNode) -> Res<()> {
        write_atomically(&self.github_path(), &serde_json::to_vec_pretty(node)?)
    }

    /// The GitHub drawer, made if this node has none yet: a single-group drawer owned by the
    /// operator, like the capture drawer.
    async fn github_node(&self) -> Res<GithubNode> {
        if let Some(node) = self.load_github()? {
            return Ok(node);
        }
        let creating = self
            .caller
            .call(MintDrawerCapability {
                scope: DrawerScope {
                    drawers: Default::default(),
                    create: true,
                },
                rights: Rights::USE,
                subject: None,
            })
            .await?
            .capability_id;
        let layout = CreateLayout::SingleGroup;
        let drawer = self
            .caller
            .call(CreateDrawer {
                capability: creating,
                command: self.caller.command("github-drawer", &layout)?,
                layout,
            })
            .await?
            .drawer_id;
        let node = GithubNode {
            drawer,
            pulls: BTreeMap::new(),
        };
        self.save_github(&node)?;
        Ok(node)
    }

    /// The pull request's document in the GitHub drawer, empty when new. The command id is fixed
    /// per (drawer, owner, repo, number), so a run that died between creating it and saving
    /// `github.json` finds the same document the next time.
    async fn github_pull_doc(&self, node: &mut GithubNode, pull: &PullRef) -> Res<DocId> {
        if let Some(doc) = node.pulls.get(&pull.name()) {
            return Ok(doc.clone());
        }
        let placement = drawer_placement(&node.drawer);
        let capability = self
            .caller
            .doc_capability(DocScope {
                create: [placement.clone()].into(),
                ..DocScope::default()
            })
            .await?;
        let doc = self
            .caller
            .call(CreateDoc {
                capability,
                command: CommandId::try_from(format!(
                    "intxt-github-pull-{}-{}/{}/{}",
                    node.drawer, pull.owner, pull.repo, pull.number
                ))?,
                placement,
                datums: BTreeMap::new(),
            })
            .await?
            .doc_id;
        node.pulls.insert(pull.name(), doc.clone());
        self.save_github(node)?;
        Ok(doc)
    }

    /// `day x intxt github pull`: fetches the pull request with `fetch` (in the CLI, `gh`) and
    /// writes it into its document, which is made when it has none. An existing datum is
    /// overwritten only when GitHub's `updatedAt` moved. Prints the title and the intxts the
    /// description names, with their links, and includes the pull request in each of those the
    /// caller can write: an entry in the intxt, and the pull request's document shared into its
    /// drawer. An intxt that is not readable or writable here is named with the reason and
    /// skipped.
    pub(crate) async fn github_pull(
        &self,
        out: &mut impl Write,
        target: &str,
        fetch: impl AsyncFnOnce(&PullRef) -> Res<Value>,
    ) -> Res<()> {
        let pull = PullRef::parse(target)?;
        let datum = pull_datum(&fetch(&pull).await?)?;
        let slot_key: DatumKey = key(work::GithubPull::ID, &pull.slot())?;
        let mut node = self.github_node().await?;
        let doc = self.github_pull_doc(&mut node, &pull).await?;
        let held = match self.caller.try_read(&doc, &slot_key).await?.1 {
            Some(value) => Some(serde_json::from_value::<work::GithubPull>(value)?),
            None => None,
        };
        let written = !held
            .as_ref()
            .is_some_and(|held| held.updated_at == datum.updated_at);
        if written {
            self.caller
                .write_datums(
                    &doc,
                    [(slot_key.clone(), to_value(&datum))].into(),
                    Default::default(),
                )
                .await?;
        }
        writeln!(
            out,
            "{} \"{}\" ({:?}) {}",
            pull.name(),
            datum.title,
            datum.state,
            if written { "imported" } else { "unchanged" }
        )?;
        // A malformed block fails the pull, after the pull request itself is imported above, so
        // the description is fixed at its source rather than guessed at.
        let named = generic::PrLinks::read(&datum.body)
            .wrap_err("reading the intxt block of the pull request description")?
            .intxts;
        if named.is_empty() {
            writeln!(out, "  names no intxt")?;
        }
        let part = DatumReference::new(ReferenceDocId::Concrete(doc), slot_key);
        let live = self.live_intxts().await?;
        let ids =
            short_ids::ShortIds::new(live.iter().map(ToString::to_string).collect::<Vec<_>>());
        for linked in named {
            let drawer = linked.reference.document().clone();
            let name = if live.contains(&drawer) {
                shown(&ids, &drawer.to_string())
            } else {
                drawer.to_string()
            };
            let title_key = key(generic::Title::ID, "main")?;
            let title = match self.caller.try_read(&drawer, &title_key).await {
                Ok((_, Some(title))) => serde_json::from_value::<generic::Title>(title)?.title,
                Ok((_, None)) => {
                    writeln!(out, "  intxt {name}: not an intxt on this node, skipped")?;
                    continue;
                }
                Err(error) => {
                    writeln!(out, "  intxt {name}: not readable here, skipped ({error})")?;
                    continue;
                }
            };
            writeln!(
                out,
                "  intxt {name} \"{title}\"\n    link: {}",
                linked.reference
            )?;
            match self.caller.add_part(&drawer, &part).await {
                Ok(included) => writeln!(
                    out,
                    "    {}",
                    if included.added {
                        "pull request added to it"
                    } else {
                        "pull request already in it"
                    }
                )?,
                Err(error) => writeln!(out, "    not added: the caller cannot write it ({error})")?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_pull_request_is_named_by_slug_or_url() {
        let want = PullRef {
            owner: "dman-os".to_owned(),
            repo: "townframe".to_owned(),
            number: 140,
        };
        for text in [
            "dman-os/townframe#140",
            "https://github.com/dman-os/townframe/pull/140",
            "https://github.com/dman-os/townframe/pull/140/files",
            "https://github.com/dman-os/townframe/pull/140#issuecomment-1",
            "https://github.com/dman-os/townframe/pull/140?diff=split",
        ] {
            assert_eq!(PullRef::parse(text).unwrap(), want, "{text}");
        }
        for text in [
            "",
            "townframe#140",
            "dman-os/townframe",
            "dman-os/townframe#",
            "dman-os/townframe#0",
            "dman-os/townframe#x",
            "dman-os/town frame#1",
            "dman-os//x#1",
            "https://github.com/dman-os/townframe/issues/140",
            "https://github.com/dman-os/townframe",
            "https://example.org/dman-os/townframe/pull/140",
        ] {
            assert!(PullRef::parse(text).is_err(), "{text}");
        }
    }

    fn answer(state: &str, merged_at: Value, body: Value) -> Value {
        json!({
            "title": "Splits the reference parser", "body": body, "state": state,
            "merged_at": merged_at,
            "head": {"ref": "feat/refs", "sha": "4c1f0a"},
            "base": {"ref": "main", "sha": "9a0e11"},
            "updated_at": "2026-11-17T10:00:00Z",
            "html_url": "https://github.com/dman-os/townframe/pull/140",
            "number": 140, "user": {"login": "x"}
        })
    }

    #[test]
    fn gh_json_becomes_the_pull_datum() {
        let open = pull_datum(&answer("open", Value::Null, json!("Body"))).unwrap();
        assert_eq!(open.state, work::PullState::Open);
        assert_eq!(
            (open.head.as_str(), open.base.as_str()),
            ("feat/refs", "main")
        );
        assert_eq!(
            (open.head_sha.as_str(), open.base_sha.as_str()),
            ("4c1f0a", "9a0e11")
        );
        assert_eq!(open.updated_at.to_string(), "2026-11-17T10:00:00Z");
        assert_eq!(open.url, "https://github.com/dman-os/townframe/pull/140");
        let merged = pull_datum(&answer(
            "closed",
            json!("2026-11-17T11:00:00Z"),
            Value::Null,
        ))
        .unwrap();
        assert_eq!(merged.state, work::PullState::Merged);
        assert_eq!(merged.body, "", "a null description is empty");
        assert_eq!(
            pull_datum(&answer("closed", Value::Null, json!("")))
                .unwrap()
                .state,
            work::PullState::Closed
        );
        let mut broken = answer("open", Value::Null, json!(""));
        broken["head"] = json!({});
        assert!(
            pull_datum(&broken)
                .unwrap_err()
                .to_string()
                .contains("head.ref")
        );
    }
}
