//! intxt's commands, the subcommands of `day x` (`day x intxt install`), hard-coded until command
//! bindings are generated from installed cartridges. `day x <nsid>` reaches a cartridge's
//! operations directly (`day x org.example.intxt.createIntxt '{"title": ".."}'`), so a command is
//! only what the operations cannot do yet: `install` stores the handler component's bytes, which
//! no operation takes from a file, and `capture` observes the host (transcript, worktrees, blob
//! files) and records what it saw.
//!
//! # Claude Code hooks
//!
//! `day x intxt record install claude-code` writes the hooks that record Claude Code sessions
//! (ADR 039 §4) into `./.claude/settings.json`, or `~/.claude/settings.json` with `--user`;
//! `--dry-run` prints the resulting JSON and writes nothing. Each recorded event gets a
//! `{ matcher?, hooks: [{ type: "command", command: "intxt record hook" }] }` entry whose command
//! reads the hook's JSON on stdin. The install merges into what is there: every other key and
//! hook is kept, an identical entry is not added twice, and an older `day x intxt capture` entry
//! is updated in place. Malformed JSON is an error that names the file, and the file is
//! never overwritten. The hooks call `intxt`, the name `day` answers to when linked as `intxt`
//! (`ln -s "$(command -v day)" ~/.local/bin/intxt`; `day-intxt` likewise makes `day intxt` work); the install says so when either is not on
//! the `PATH`. Add `--node <dir>` (or set `DAY_NODE`) to a hook's command to name the node, and
//! `"timeout"` to a hook if a recording can take long. The command needs the node's server (`day
//! node start --bg`) and fails fast without one: booting the node in each hook's process would take
//! seconds, and parallel tool calls run parallel hooks. The configuration's shape was checked
//! against Claude Code's hooks documentation (code.claude.com/docs/en/hooks).
//!
//! # git's `post-rewrite` hook
//!
//! The first time capture records a clone it installs git's `post-rewrite` hook there (in
//! `git rev-parse --git-path hooks`, which follows `core.hooksPath`): a two-line `sh` script, marked
//! as capture's, that runs `day x intxt capture git-rewrite "$1"` (a spelling clones that have the hook keep calling, so `capture` stays as a hidden command). git then reports the exact old
//! and new commit of every amend and rebase, which capture records beside the worktree's commits
//! and `day x intxt add` copies into an intxt that holds the old one. The hook needs `day` on
//! the `PATH` git runs with, and the node's server (set `DAY_NODE` in the environment if the node
//! is not the default). A `post-rewrite` hook that is not capture's is never overwritten or
//! chained: `day x intxt status` says "hook not installed: foreign post-rewrite", and rewrites are
//! then matched by patch id only. `day x intxt capture install-git-hooks` installs it on request
//! and reports what it did.
//!
//! # ACP agents
//!
//! `day x intxt record acp -- <agent command>` records an ACP agent for the client that launches it:
//! point the editor's agent command at it (`day x intxt record acp -- claude-code-acp`). It forwards
//! the connection unchanged and records session starts and prompt turns (see `record.rs`).
//!
//! `claude-code-acp`, `gemini --experimental-acp` and `codex-acp` are the agents it is meant for;
//! none was run against it here (the tests use a scripted agent), so check the first recording
//! of each.

pub(crate) mod capture;
mod edit;
mod github;
mod hooks;
pub(crate) mod package;
pub(crate) mod participant;
mod record;
pub(crate) mod review;

use crate::client::NodeClient;
use crate::interlude::*;

use big_dispatch_core::Readiness;
use intxt_core::client::Caller;
use intxt_core::edit::Named;
use service_utils_rs::RequestContext;

#[derive(Debug, clap::Subcommand)]
pub enum IntxtCommands {
    /// Admit intxt's cartridge to this node, host its handler, and enable it
    ///
    /// Stores the built handler component in the node's blob store, admits the cartridge that
    /// names it, grants the nodule the methods it calls, and enables it, which registers intxt's records and
    /// serves `createIntxt` and `addToIntxt`.
    Install {
        /// The handler component: `src/wasm_utils_rs/target/wasm32-wasip3/release/intxt_nodule_guest.wasm`,
        /// built with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_nodule_guest
        /// --target wasm32-wasip3 --release`
        #[arg(long)]
        handler: PathBuf,
    },
    /// Add commits, a branch or a jj change to an intxt, with the sessions that produced them
    ///
    /// Run it in the worktree that holds the work. What is named is resolved now, to commits:
    /// a range is stored as its tip and merge base, never as the text you typed. The objects
    /// the commits need are carried as a git pack (thin against the base unless `--full`), in
    /// the node's blob store, and the sessions whose recorded edits produced content in those
    /// commits come with them. Adding something already in the intxt changes nothing and says
    /// so. Names the intxt by the short id `show` prints, or by its full drawer id, and with
    /// neither, adds to the intxt this worktree is on.
    #[command(
        group = clap::ArgGroup::new("what").required(true),
        after_long_help = "Examples:\n  day x intxt add qx --branch feat/refs\n  day x intxt add qx --commits main..feat/refs\n  day x intxt add qx --change kxqoltvm"
    )]
    Add {
        /// The intxt: its short id, or its full drawer id. Default: the one this worktree is on
        /// (`day x intxt use`)
        intxt: Option<String>,
        /// Commits: a range `main..feat/x` (its tip and its merge base with `main`), or one
        /// commit (against its first parent). Brings those commits and the files they touch
        #[arg(long, group = "what", value_name = "RANGE")]
        commits: Option<String>,
        /// A local branch: its commits since its merge base with its upstream, or with `main`
        /// (`master`) when it has none, and the branch itself, tracked as this clone sees it, so
        /// commits it moves onto later come in as new revisions. Brings what `--commits` brings
        #[arg(long, group = "what", value_name = "NAME")]
        branch: Option<String>,
        /// A jj change, by its change id, followed through its rewrites: every commit the change
        /// has had comes in. Needs `jj`
        #[arg(long, group = "what", value_name = "CHANGE")]
        change: Option<String>,
        /// Pack every object, not only what the base does not have
        ///
        /// The default pack is thin: it assumes whoever receives the intxt already has the base
        /// commit. A full pack stands alone and is several times larger.
        #[arg(long)]
        full: bool,
    },
    /// Change an intxt through one document: its title, description and entries
    ///
    /// The document is JSON (`org.example.intxt.edit`, with a JSON Schema: `--schema`). With no
    /// flag, `edit` writes it to a temporary file, runs `$VISUAL`, `$EDITOR` or `vi` on it (as git
    /// does, so `EDITOR="code --wait"` works) and applies the difference when the editor exits:
    /// an entry that is gone is removed (as `rm` does), an `{"add": {...}}` entry is resolved in
    /// this worktree and added (as `add` does), and a changed title or description is written.
    /// If it cannot be applied the file reopens with an `errors` member until it applies, or is
    /// saved unchanged or emptied. Without a terminal this form refuses at once.
    ///
    /// The non-interactive form never opens an editor. `--print` writes the document; `--apply`
    /// reads one from a file or `-` for stdin. An applied document may be partial: `base` (the
    /// heads it was printed at, required) and only the members to change. An omitted member is
    /// unchanged and `"description": null` clears the description. `entries` is the whole list,
    /// as printed: the existing entries it leaves out are removed. `entryChanges` is a delta that
    /// leaves the other entries alone: `{"remove": "<id>"}` and `{"add": {...}}`. A document has
    /// one of the two, never both. Entries are named by their full id (32 characters); any prefix that is unambiguous when the
    /// document is applied (every short id `show` prints) is accepted. Entries are added or
    /// removed, never edited in place. Applying prints each change and the commands that undo
    /// them, in the order to run them. `--dry-run` prints the plan and writes nothing.
    ///
    /// `--json` prints the result as `{"changes": [{"op", "text"}], "undo": [...]}`, or on
    /// failure `{"errors": [{"path": "<JSON pointer into the document>", "message"}]}`.
    ///
    /// Exit status: 0 applied (or nothing to change); 1 the document is wrong; 2 bad command
    /// line; 3 the intxt changed since the document was printed (stale `base`) and the document
    /// still has something to change: the current document is printed (with `--json`, as
    /// `"current"` beside the errors) to edit again. A stale document that asks for nothing the
    /// intxt does not already have (a retry of one that applied) is "nothing to change", exit 0.
    #[command(
        group = clap::ArgGroup::new("mode").args(["print", "apply", "schema"]),
        after_long_help = "Example, for an agent or a script:\n  day x intxt edit qx --print > qx.json    # the document, with its base heads\n  # edit qx.json: remove an entry by deleting its line, or add one with\n  #   { \"add\": { \"branch\": \"feat/x\" } }\n  day x intxt edit qx --apply qx.json --dry-run\n  day x intxt edit qx --apply qx.json      # prints the changes and their undo commands\n\nA partial document works as well:\n  echo '{\"base\": [\"z..\"], \"title\": \"New title\", \"entryChanges\": [{\"remove\": \"zkq\"}]}' | day x intxt edit qx --apply -\n\nInteractively:\n  day x intxt edit qx\n"
    )]
    Edit {
        /// The intxt: its short id, or its full drawer id. Default: the one this worktree is on
        /// (`day x intxt use`), or with `--apply` the one the document names
        intxt: Option<String>,
        /// Write the document to stdout
        #[arg(long)]
        print: bool,
        /// Apply the document in FILE (`-` for stdin)
        #[arg(long, value_name = "FILE")]
        apply: Option<String>,
        /// Write the document's JSON Schema to stdout
        #[arg(long)]
        schema: bool,
        /// Print what would change and write nothing
        #[arg(long, conflicts_with_all = ["print", "schema"])]
        dry_run: bool,
        /// Print the result of `--apply` as JSON
        #[arg(long, requires = "apply")]
        json: bool,
    },
    /// Create an intxt and remember it, so it has a short id
    ///
    /// Prints the new intxt's short id, which names it in `add`, `show` and `rm`.
    ///
    /// With `--with`, the new intxt is lent the named intxt's commits documents, for a stack or a
    /// follow-up: commits they have in common are stored once, and each holds the commits the
    /// other added.
    #[command(
        after_long_help = "Examples:\n  day x intxt new \"Datum refs move to dtxt\"\n  day x intxt new \"Follow-up\" --with qx"
    )]
    New {
        /// The intxt's title
        title: String,
        /// The intxt's description, in Markdown
        #[arg(long)]
        description: Option<String>,
        /// A related intxt (its short id, or its full drawer id) whose commits documents the new
        /// intxt is lent
        #[arg(long)]
        with: Option<String>,
    },
    /// Remove entries and comments from an intxt, by the short ids `show` and `comment` print
    ///
    /// Deletes the entries (commits, jj changes, tracked refs, sessions) and the packs only they
    /// carried, and comments (a thread's first comment takes its replies with it). The documents already shared with the intxt stay shared, and copies synced to
    /// others stay with them.
    #[command(after_long_help = "Examples:\n  day x intxt rm qx kq vx")]
    Rm {
        /// The intxt: its short id, or its full drawer id
        intxt: String,
        /// The entries to remove, by short id
        #[arg(required = true)]
        items: Vec<String>,
    },
    /// List every intxt remembered by this node, with counts of its commits, refs and sessions
    ///
    #[command(
        after_long_help = "Examples:\n  day x intxt list\n  day x intxt list --node ~/.local/share/day/node"
    )]
    List,
    /// Forget an intxt on this node
    ///
    /// The drawer and anything already shared stay; it only stops being listed here, and naming it by its full id brings it back.
    #[command(after_long_help = "Examples:\n  day x intxt drop qx\n  day x intxt drop qy")]
    Drop {
        /// The intxt: its short id, or its full drawer id
        intxt: String,
    },
    /// Say which intxts on this node hold a commit
    ///
    /// Names the commit by a full sha, a unique prefix or any name git resolves in this worktree.
    /// Prints one line per intxt that holds it: its short id, its title and its `link:`. Prints
    /// nothing when no intxt does.
    #[command(after_long_help = "Examples:\n  day x intxt which 4c1f9a0")]
    Which {
        /// The commit, as this worktree's git resolves it
        commit: String,
    },
    /// Show what an intxt holds: its commits, jj changes, tracked refs and sessions
    ///
    /// Each entry has a short id, the shortest prefix that is unique among the intxt's entries.
    #[command(after_long_help = "Examples:\n  day x intxt show qx")]
    Show {
        /// The intxt: its short id, or its full drawer id. Default: the one this worktree is on
        /// (`day x intxt use`)
        intxt: Option<String>,
    },
    /// Comment on lines of a commit or an op the intxt holds
    ///
    /// The target is `<commit>:<path>:<lines>` (a commit the intxt holds, `4c1f9a0` or any name
    /// git resolves) or `op<seq>:<path>:<lines>` (an uncommitted state of this worktree); lines
    /// are `a-b` or `a`, 1-based in the path's content at that commit or op. Run it in the
    /// worktree whose repository has the commit. The comment records the quoted text, so it
    /// can be found again after a rebase.
    #[command(
        after_long_help = "Examples:\n  day x intxt comment qx 4c1f9a0:src/lib.rs:120-124 -m \"Why not reuse the fragment here?\"\n  day x intxt comment qx 4c1f9a0:src/lib.rs:12 --side old -m \"This line was fine.\""
    )]
    Comment {
        /// The intxt: its short id, or its full drawer id
        intxt: String,
        /// What to comment on: `<commit>:<path>:<lines>` or `op<seq>:<path>:<lines>`
        target: String,
        /// The comment, in Markdown
        #[arg(short, long)]
        message: String,
        /// Count the lines in the commit's parent, as the old side of its diff
        #[arg(long, value_enum, default_value = "new")]
        side: review::Side,
        /// Why: one of W3C Web Annotation's motivations
        #[arg(long, default_value = "commenting", value_parser = clap::builder::PossibleValuesParser::new([
            "assessing", "bookmarking", "classifying", "commenting", "describing", "editing",
            "highlighting", "identifying", "linking", "moderating", "questioning", "tagging",
        ]))]
        motivation: String,
    },
    /// Reply to a comment, and resolve or reopen its thread
    ///
    /// Names the comment by the short id `comment` and `inbox` print, which is unique among one
    /// intxt's entries and comments: name that intxt with `--intxt` or run in the worktree that
    /// is on it. With `--resolve` or `--reopen` and no message, only the thread's state changes.
    #[command(
        after_long_help = "Examples:\n  day x intxt reply kz -m \"Done in 77b2\" --resolve\n  day x intxt reply kz --intxt qx --reopen"
    )]
    Reply {
        /// The comment to answer, by the short id printed for it
        comment: String,
        /// The intxt the comment is in: its short id, or its full drawer id. Default: the one this
        /// worktree is on (`day x intxt use`)
        #[arg(long)]
        intxt: Option<String>,
        /// The reply, in Markdown
        #[arg(short, long, required_unless_present_any = ["resolve", "reopen"])]
        message: Option<String>,
        /// Mark the thread resolved
        #[arg(long, conflicts_with = "reopen")]
        resolve: bool,
        /// Mark the thread unresolved
        #[arg(long)]
        reopen: bool,
    },
    /// List the threads waiting for you and where their comments are on the current revision
    ///
    /// A thread is listed when it is unresolved and you have not seen its latest comments, or
    /// someone else wrote last. Each comment is carried to the newest revision the intxt holds
    /// (same path only): its placed lines, `(context changed)` when the text is there but its
    /// surroundings changed, or `outdated`. Run it in the worktree whose repository has the
    /// commits. Listing marks the threads seen.
    #[command(after_long_help = "Examples:\n  day x intxt inbox qx")]
    Inbox {
        /// The intxt: its short id, or its full drawer id. Default: the one this worktree is on
        /// (`day x intxt use`)
        intxt: Option<String>,
    },
    /// Say which intxt this worktree is on, or put it on another
    ///
    /// `add`, `show` and `inbox` act on that intxt when none is named, and `new` puts the
    /// worktree it runs in on the intxt it creates. Kept in the worktree's local state
    /// (`<git dir>/intxt/state.json`), never synced: each checkout is on its own intxt.
    #[command(
        after_long_help = "Examples:\n  day x intxt use qx\n  day x intxt use\n  day x intxt use --none"
    )]
    Use {
        /// The intxt: its short id, or its full drawer id
        intxt: Option<String>,
        /// Put the worktree on no intxt
        #[arg(long, conflicts_with = "intxt")]
        none: bool,
    },
    /// Print the block that names intxts in a PR description
    ///
    /// Prints a collapsed `<details>` holding a JSON code block (`org.example.intxt.prLinks`) with
    /// each intxt's `dtxt:doc:` reference (hinting at this node and the peers it syncs with), title
    /// and tracked refs, to paste at the end of the description. Default: the intxt this worktree
    /// is on. Reading a PR with `day x intxt github pull` finds the intxts it names there.
    #[command(after_long_help = "Examples:\n  day x intxt pr-block\n  day x intxt pr-block qx qy")]
    PrBlock {
        /// The intxts: short ids, or full drawer ids. Default: the one this worktree is on
        intxts: Vec<String>,
    },
    /// Say what is recorded in this worktree and which intxts include it
    ///
    /// Run it in a worktree: it lists the ops and sessions capture recorded here, and the
    /// intxts that include this worktree's commits, its clone's tracked refs or its sessions.
    #[command(after_long_help = "Examples:\n  day x intxt status")]
    Status,
    /// Record one Claude Code hook invocation: the hook's JSON on stdin (the old spelling of
    /// `record hook` and `record git-rewrite`, kept for hooks already installed)
    ///
    /// Observes the session's transcript and worktrees, puts changed binary files in the node's
    /// blob store, and records everything in the node's capture drawer. Install it as the command
    /// of Claude Code's hooks (see this module's documentation). It prints nothing unless it has
    /// something to report or `--verbose` is given, and then only to stderr; it never exits with
    /// status 2, which Claude Code feeds back to the model.
    ///
    /// Its subcommands are git's side of capture: the `post-rewrite` hook capture installs in the
    /// clones it records.
    #[command(hide = true)]
    Capture {
        /// Say on stderr what was recorded
        #[arg(long, global = true)]
        verbose: bool,
        #[command(subcommand)]
        command: Option<capture::CaptureCommands>,
    },
    /// Work with GitHub pull requests
    Github {
        #[command(subcommand)]
        command: GithubCommands,
    },
    /// Recording: the hooks that record sessions, the ACP proxy, git's side of rewrites
    ///
    /// Recording moves raw data into the node's capture drawer. Nothing in it calls for judgement.
    Record {
        /// Say on stderr what was recorded (`hook`, `git-rewrite`)
        #[arg(long, global = true)]
        verbose: bool,
        #[command(subcommand)]
        command: RecordCommands,
    },
}

/// What `day x intxt record` does.
#[derive(Debug, clap::Subcommand)]
pub enum RecordCommands {
    /// Write a harness's hook configuration
    Install {
        #[command(subcommand)]
        harness: Harness,
    },
    /// Record one Claude Code hook invocation: the hook's JSON on stdin
    ///
    /// The command of the hooks `install claude-code` writes. Observes the session's transcript
    /// and worktrees, puts changed binary files in the node's blob store, and records everything
    /// in the node's capture drawer. It prints nothing unless it has something to report or
    /// `--verbose` is given, and then only to stderr; it never exits with status 2, which
    /// Claude Code feeds back to the model.
    Hook,
    /// Record an ACP session: run the agent command, passing everything through
    ///
    /// An ACP client (an editor) launches this in place of the agent, e.g. `intxt record acp --
    /// claude-code-acp`. It spawns the agent, forwards the client's stdin to it and its stdout to
    /// the client unchanged, and records the session's messages, tool calls and plan, and the edits
    /// each prompt turn made in the session's working directory, in the node's capture drawer.
    /// Needs the node's server; without one the session runs unrecorded. A recording that fails
    /// says why on stderr and stops, and the session goes on. Exits with the agent's status.
    Acp {
        /// Record the agent's thinking (`agent_thought_chunk`) as well
        #[arg(long)]
        thinking: bool,
        /// The agent's command and arguments
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        agent: Vec<String>,
    },
    /// Record the commits git's `post-rewrite` hook reports as replaced (stdin: `<old> <new>`
    /// per line) in this worktree's capture
    ///
    /// What the `post-rewrite` hook capture installs in a clone runs; git passes `amend` or
    /// `rebase`. A failure is one line on stderr and status 1; git ignores it, since
    /// `post-rewrite` cannot block.
    GitRewrite {
        /// What was rewritten, git's argument to the hook
        #[arg(value_enum)]
        kind: capture::RewriteKind,
    },
}

impl RecordCommands {
    async fn run(self, verbose: bool) -> Res<ExitCode> {
        match self {
            Self::Install {
                harness: Harness::ClaudeCode { user, dry_run },
            } => hooks::run(user, dry_run),
            Self::Hook => capture::CaptureCommands::run(None, verbose).await,
            Self::Acp { thinking, agent } => record::run(agent, thinking).await,
            Self::GitRewrite { kind } => {
                capture::CaptureCommands::run(
                    Some(capture::CaptureCommands::GitRewrite { kind }),
                    verbose,
                )
                .await
            }
        }
    }
}

/// The harnesses `record install` writes a configuration for.
#[derive(Debug, clap::Subcommand)]
pub enum Harness {
    /// Write the Claude Code hooks into `./.claude/settings.json`
    ///
    /// Adds a hook running `intxt record hook` to every event that is recorded, merging into the
    /// file: other keys and hooks stay, running it twice changes nothing, and an older `day x
    /// intxt capture` entry becomes the new command. Malformed JSON is an error and the file is
    /// left alone.
    ClaudeCode {
        /// Write `~/.claude/settings.json` instead of the project's
        #[arg(long)]
        user: bool,
        /// Print the resulting JSON and write nothing
        #[arg(long)]
        dry_run: bool,
    },
}

/// What `day x intxt github` does.
#[derive(Debug, clap::Subcommand)]
pub enum GithubCommands {
    /// Import a pull request onto the node, with your `gh` credentials
    ///
    /// Fetches the pull request with `gh api` (so `gh` must be installed and logged in) and
    /// writes its title, description, state, branches, shas and update time into the node's
    /// GitHub document for the repository, which a micro can read. Pulling again overwrites it
    /// only when GitHub says the pull request changed. Prints the title and the intxts the
    /// description's intxt block names (`day x intxt pr-block` prints one); a malformed block is
    /// an error.
    #[command(
        after_long_help = "Examples:\n  day x intxt github pull dman-os/townframe#140\n  day x intxt github pull https://github.com/dman-os/townframe/pull/140"
    )]
    Pull {
        /// `<owner>/<repo>#<number>`, or the pull request's URL
        target: String,
    },
    /// List the pull requests imported onto this node
    Ls,
    /// Show an imported pull request and the intxts named by its description
    Show {
        /// `<owner>/<repo>#<number>`, or the pull request's URL
        target: String,
    },
}

pub async fn run(command: IntxtCommands) -> Res<ExitCode> {
    match command {
        IntxtCommands::Record { verbose, command } => command.run(verbose).await,
        IntxtCommands::Github { command } => {
            with_packaging(async |packaging| match command {
                GithubCommands::Pull { target } => {
                    packaging
                        .github_pull(&mut std::io::stdout(), &target, github::fetch_with_gh)
                        .await
                }
                GithubCommands::Ls => packaging.github_ls(&mut std::io::stdout()).await,
                GithubCommands::Show { target } => {
                    packaging.github_show(&mut std::io::stdout(), &target).await
                }
            })
            .await
        }
        IntxtCommands::Add {
            intxt,
            commits,
            branch,
            change,
            full,
        } => {
            let named = Named::from_flags(commits, branch, change);
            with_packaging(async |packaging| {
                packaging
                    .add(&mut std::io::stdout(), intxt.as_deref(), named, full)
                    .await
            })
            .await
        }
        IntxtCommands::Edit {
            intxt,
            print,
            apply,
            schema,
            dry_run,
            json,
        } => edit(intxt, print, apply, schema, dry_run, json).await,
        IntxtCommands::New {
            title,
            description,
            with,
        } => {
            with_packaging(async |packaging| {
                packaging
                    .create(
                        &mut std::io::stdout(),
                        &title,
                        description.as_deref(),
                        with.as_deref(),
                    )
                    .await
                    .map(|_| ())
            })
            .await
        }
        IntxtCommands::Rm { intxt, items } => {
            with_packaging(async |packaging| {
                packaging.rm(&mut std::io::stdout(), &intxt, &items).await
            })
            .await
        }
        IntxtCommands::Comment {
            intxt,
            target,
            message,
            side,
            motivation,
        } => {
            let motivation = serde_json::from_value(serde_json::Value::String(motivation))?;
            with_packaging(async |packaging| {
                packaging
                    .comment(
                        &mut std::io::stdout(),
                        &intxt,
                        &target,
                        &message,
                        side,
                        motivation,
                    )
                    .await
            })
            .await
        }
        IntxtCommands::Reply {
            comment,
            intxt,
            message,
            resolve,
            reopen,
        } => {
            // clap makes the flags exclusive: `--resolve` is true, `--reopen` is false.
            let resolved = (resolve || reopen).then_some(resolve);
            with_packaging(async |packaging| {
                packaging
                    .reply(
                        &mut std::io::stdout(),
                        intxt.as_deref(),
                        &comment,
                        message.as_deref(),
                        resolved,
                    )
                    .await
            })
            .await
        }
        IntxtCommands::Inbox { intxt } => {
            with_packaging(async |packaging| {
                packaging
                    .inbox(&mut std::io::stdout(), intxt.as_deref())
                    .await
            })
            .await
        }
        IntxtCommands::Which { commit } => {
            with_packaging(async |packaging| packaging.which(&mut std::io::stdout(), &commit).await)
                .await
        }
        IntxtCommands::Show { intxt } => {
            with_packaging(async |packaging| {
                packaging
                    .show(&mut std::io::stdout(), intxt.as_deref())
                    .await
            })
            .await
        }
        IntxtCommands::PrBlock { intxts } => {
            with_packaging(async |packaging| {
                packaging.pr_block(&mut std::io::stdout(), &intxts).await
            })
            .await
        }
        IntxtCommands::Use { intxt, none } => {
            with_packaging(async |packaging| {
                packaging
                    .use_intxt(&mut std::io::stdout(), intxt.as_deref(), none)
                    .await
            })
            .await
        }
        IntxtCommands::List => {
            with_packaging(async |packaging| packaging.list(&mut std::io::stdout()).await).await
        }
        IntxtCommands::Drop { intxt } => {
            with_packaging(async |packaging| packaging.drop(&mut std::io::stdout(), &intxt).await)
                .await
        }
        IntxtCommands::Status => {
            with_packaging(async |packaging| packaging.status(&mut std::io::stdout()).await).await
        }
        IntxtCommands::Install { handler } => install(&handler).await,
        IntxtCommands::Capture { verbose, command } => {
            capture::CaptureCommands::run(command, verbose).await
        }
    }
}

/// `day x intxt edit`: the document printed, applied from a file, or edited in `$EDITOR`.
async fn edit(
    intxt: Option<String>,
    print: bool,
    apply: Option<String>,
    schema: bool,
    dry_run: bool,
    json: bool,
) -> Res<ExitCode> {
    use std::io::IsTerminal;

    if schema {
        println!("{}", intxt_core::edit::EditDocument::json_schema());
        return Ok(ExitCode::SUCCESS);
    }
    if let Some(source) = apply {
        return with_packaging_exit(async |packaging| {
            packaging
                .edit_apply(
                    &mut std::io::stdout(),
                    &mut std::io::stderr(),
                    intxt.as_deref(),
                    &source,
                    json,
                    dry_run,
                )
                .await
        })
        .await;
    }
    if print {
        return with_packaging(async |packaging| {
            packaging
                .edit_print(&mut std::io::stdout(), intxt.as_deref())
                .await
        })
        .await;
    }
    edit::ensure_interactive(std::io::stdin().is_terminal() && std::io::stdout().is_terminal())?;
    with_packaging_exit(async |packaging| {
        packaging
            .edit_interactive(
                &mut std::io::stdout(),
                intxt.as_deref(),
                &edit::editor_command(),
                dry_run,
            )
            .await
    })
    .await
}

/// `day x intxt install`: reads the handler component at `handler` and installs it on the node
/// through [`intxt_core::install::install`], then reports whether the executor loaded it.
async fn install(handler: &std::path::Path) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let wasm = std::fs::read(handler)
        .wrap_err_with(|| format!("reading the handler {}", handler.display()))?;
    let caller = Caller::new(
        &*client,
        RequestContext::root(),
        None,
        "intxt-install".to_owned(),
    );
    // No drawers yet: at node-52 the nodules service actor holds none of the operator's drawers,
    // so the handles minted for a granted drawer are refused (`Denied`) and the install fails.
    // The intxts and the GitHub drawer are passed here once the service actor can hand them down.
    let installed = intxt_core::install::install(&caller, &wasm, &[]).await?;
    eyre::ensure!(
        installed.record.readiness == Readiness::Ready,
        "intxt is installed but not ready: {:?}",
        installed.record.readiness
    );
    println!(
        "{}",
        if installed.already_installed {
            "intxt is already installed"
        } else {
            "intxt is installed"
        }
    );
    Ok(ExitCode::SUCCESS)
}

/// Runs a packaging verb against the node, in the current directory.
async fn with_packaging(
    verb: impl AsyncFnOnce(&package::Packaging<'_>) -> Res<()>,
) -> Res<ExitCode> {
    with_packaging_exit(async |packaging| verb(packaging).await.map(|()| ExitCode::SUCCESS)).await
}

/// [`with_packaging`] for a verb that chooses its exit code.
async fn with_packaging_exit(
    verb: impl AsyncFnOnce(&package::Packaging<'_>) -> Res<ExitCode>,
) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let cwd = std::env::current_dir()?;
    let config = lazy::config().await?;
    // The environment is read here and nowhere below: verbs are handed who they write as.
    let participant = participant::Detected::from_process(config.node_dir.root())?;
    verb(
        &package::Packaging::new(&client, &cwd, config.node_dir.root())
            .participating_as(participant),
    )
    .await
}

/// Capture runs against the node's server only, and starts one in the background when none is
/// serving (as `day node start --bg` does); it stays up until `day node stop`. Booting the node in
/// the hook's own process instead would be paid on every hook, and parallel tool calls run
/// parallel hooks, of which only one process can hold the node. The socket is probed, not
/// connected to through [`NodeClient`], which would boot the node in this process. Nothing is
/// printed: a hook's stdout is the harness's to read.
pub(crate) async fn ensure_server(dir: &day_node::NodeDir) -> Res<()> {
    if dir.probe_socket().await? == day_node::SocketState::Live {
        return Ok(());
    }
    crate::cmds::serve::ensure_background(dir).await?;
    Ok(())
}

/// Calls `input`'s operation and decodes its answer; a non-success answer is the operation's own
/// error, reported as the node gave it.
pub(crate) async fn call<M>(client: &NodeClient, input: M) -> Res<M::Output>
where
    M: method_core::Unary,
    M::Error: std::fmt::Display,
{
    client
        .call(input)
        .await
        .map_err(|error| eyre::eyre!("{} failed: {error}", M::XRPC.nsid))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, clap::Parser)]
    struct Cli {
        #[command(subcommand)]
        command: IntxtCommands,
    }

    fn new_args(args: &[&str]) -> (String, Option<String>) {
        let parsed = <Cli as clap::Parser>::try_parse_from(
            ["intxt", "new"].into_iter().chain(args.iter().copied()),
        )
        .unwrap();
        match parsed.command {
            IntxtCommands::New { title, with, .. } => (title, with),
            other => panic!("parsed {other:?}, not new"),
        }
    }

    #[test]
    fn new_takes_an_optional_related_intxt_with() {
        assert_eq!(new_args(&["T"]), ("T".to_owned(), None));
        assert_eq!(
            new_args(&["T", "--with", "qx"]),
            ("T".to_owned(), Some("qx".to_owned()))
        );
        assert!(<Cli as clap::Parser>::try_parse_from(["intxt", "new", "T", "--with"]).is_err());
    }
}
