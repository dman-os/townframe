# Importing whole session logs (D92)

Status: Proposed, 2026-11-25. Not built.

## The need

Hooks record a session while it runs. Some sessions never meet the hooks: they ran before the
hooks were installed, ran on another machine, ran while recording was paused (the operator's
node is paused like this today, waiting for node-101), or ran in a harness that has no hooks.
Their logs still exist, so intxt should be able to import one whole after the fact.

## What exists

Most of the machinery is already there:

- `intxt_capture::transcript::TranscriptImporter` already reads a Claude Code transcript from
  offset 0 and returns the session datums (`agentSession`, one `promptTurn` per turn, the latest
  `plan`, and the tool calls with their output and diffs). It is chunking-invariant, so one call
  over the whole file gives the same datums as the hooks' many small calls.
- `host::import_transcripts` brings the main transcript and each subagent's
  (`<transcript>/subagents/agent-<id>.jsonl`) up to date, with each subagent's `parent` reference
  set.
- Capture records into the capture drawer, and `intxt add` pulls sessions from there into an
  intxt. An import never has to choose an intxt.
- The per-session state file, `<node>/intxt/sessions/<session id>.json`, holds the importers'
  offsets and the session's document id.

## Proposal

1. **Command.** `day x intxt record import <transcript>...`. It joins `record install`, `record
   hook`, `record acp` and `record git-rewrite`. It takes one or more transcript paths, so a
   backfill can pass a whole project directory's `*.jsonl`. The format is detected from the
   first record; Claude Code is the only format at first.
2. **One state, shared with the hooks.** The import takes the session id from the transcript and
   uses the same state file and lock as the hooks. It runs `import_transcripts`, records the
   batch, and commits the state with the same idempotency key. Because of that:
   - importing a session the hooks already recorded in part continues from the stored offset and
     creates no duplicates;
   - a session that was imported and then resumed with the hooks on continues where the import
     stopped;
   - importing the same file twice records nothing the second time.
3. **No worktree ops.** Live capture snapshots worktrees around each tool call. An import has no
   snapshots, so it records the conversation (turns, tool calls, their diffs) and no ops. The
   state's `worktrees` set stays empty, and the import never runs git.
4. **Provenance.** An imported session should say it was imported, so a reviewer can tell why it
   has no ops. The proposal is a `capture: "import"` (vs `"hooks"`, `"acp"`) on `agentSession`.
   This needs a schema change in `intxt_core::work::session`.
5. **Other harnesses.** Each format gets its own importer that produces the same datums
   (`TranscriptImporter` for Claude Code; ACP replay logs; pi and Codex session JSONL later). The
   command dispatches on the detected format. Nothing beyond Claude Code is in the first slice.

## Open questions for the operator

- **Attribution after the fact.** Should an import try to tie its tool calls' diffs to commits
  (match an Edit/Write diff against the hunks of commits made during the session's time span)?
  This is heuristic, and a wrong match is worse than no match. My recommendation is no for the
  first slice: record the conversation only and revisit once imported sessions are being used.
- **Provenance field.** Is it worth the schema change, or is "a session with no ops" enough of a
  signal?
- **Machine of origin.** A transcript copied from another machine names that machine's paths.
  Should the import keep them as recorded (proposed), or rewrite them to local worktrees?

## First slice

`record import` for Claude Code transcripts, sharing state with the hooks, with no ops and no
provenance field (until the question above is answered). Tests:

- importing a transcript fixture gives the same datums as replaying it through hook invocations;
- a second import records nothing;
- importing a session that the hooks recorded halfway continues from the stored offset.
