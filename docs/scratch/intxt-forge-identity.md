# intxt: forge authentication and identity

**Status:** Scratch for discussion, 2026-11-24 (ledger D80). It covers how the forge integration starts: who signs in where, how a forge account relates to a daytxt actor, and how a PR and an intxt are linked so that a reader can trust the link. It proposes defaults to talk over. Nothing here is decided.

## What exists

- **The intxt names the PR.** `githubPullEntry` is an entry in the intxt, written by its author's actor and so signed by the node.
- **The PR names the intxt.** The PR description carries a collapsed `<details>` block with JSON that lists `dtxt:doc:<drawer>?via=<node key>` URLs (`day x intxt pr-block`). It is written by whoever can edit the PR description on the forge.
- **The import.** `day x intxt github pull` fetches the PR with the operator's `gh` login, on the CLI's machine, into intxt's own GitHub document. This is citizenship entry C5: forge data should be general, fetched on the node.
- **Who wrote something on our side is claimed, not attested** (C7). The operator and an agent on one node are one actor.

So both halves of a link exist, but neither side checks the other, and nothing ties a forge account to an actor.

## Questions to settle

### 1. Who signs in, and where

- **Viewing a public intxt needs no sign-in.**
- **Proposal: sign-in is through the forge (OAuth),** on the web app's server: GitHub and GitLab are built into Better Auth (`src/btress_auth` already runs it as a jco component), and its generic OAuth plugin covers Forgejo, Codeberg and Gitea. Tangled signs in with ATProto OAuth; whether Better Auth covers that is to check.
- Sign-in is for what an anonymous viewer can't do:
  - see an intxt shared with them;
  - comment from the web;
  - link their forge account to their actor (section 2).
- **Open:** the relay provider runs the viewer, so its Better Auth instance holds the user's forge tokens. Is that acceptable for an MVP, or should a viewer store only the identity it learned and no token?

### 2. A forge account and a daytxt actor

Three ways to tie `github:alice` to `dtxt:actor:…`, from least to most trust in a third party:

- **(a) Self-published proof,** as Keyoxide does. The actor signs "I am `github:alice`", and the forge account publishes a pointer back to the actor URL, in its profile, a gist or a file in a repo. Anyone can verify it, with no party trusted. It is checked by fetching the forge, and it works for any forge with a public profile.
- **(b) Two-way reference, per PR.** The PR's block names the intxt and the intxt's entry names the PR. When both hold, the link is *mutual* for that PR, with no global binding of accounts at all. Both halves exist today.
- **(c) A binding from the viewer.** The user signs in to the viewer with forge OAuth, and their node signs a challenge from it. The viewer then issues a binding record, so everyone who reads it trusts the relay provider.

**Proposal:** (b) for the MVP, because it's built and needs no identity layer. (a) as the general identity mechanism after it, since it is forge-neutral and needs no trusted party. (c) only as a convenience that writes the (a) proof for the user.

An account is named by the forge's own user id, not the login, which can be renamed: forges.link keys repos the same way. It is a generic datum (`forgeAccount`: forge, instance, id, login at last sight), not intxt's (C5).

### 3. What a reader sees about a link

- **Mutual:** the PR names the intxt and the intxt names the PR.
- **Claimed:**
  - by the PR: the PR names the intxt, and the intxt doesn't name the PR;
  - by the intxt: the intxt names the PR, and the PR doesn't name the intxt.
- **Attested:** mutual, and the intxt's actor is bound to the PR author's forge account under (a). This comes after the MVP.

A caveat for "mutual": a forge lets maintainers edit a PR's description, so "the PR names it" means "someone with write access on the PR named it", not "the author did". The block could carry the actor's signature over the PR's number and repo key to close that gap later.

### 4. Where forge code runs, and in what language

- **Proposal: the `forges` TypeScript library in a jco component, for the MVP.** It covers 14 forges and normalises their refs, states and events; its README says it runs in browsers with no runtime dependencies, which suggests it runs under jco's JS runtime with `fetch`. That is to verify with a smoke test first.
- No Rust port was found. `vcs-forge` (GitHub, GitLab and Gitea PR lifecycle) is CLI-oriented.
- Our datums take forges.link's vocabulary anyway (refs with `forge` and `instance`, keys from repo ids, raw values kept), so swapping the implementation later doesn't change what is stored.
- **Open:** does this component run on the node as a cartridge's handler (the citizenship answer: egress through the `#net` permbind, tokens in node secret storage, both nodules'), or beside the web app's server for now (the product shortcut, logged as a C entry)?

### 5. Tokens for fetching

Fetching a private PR needs a token on the side that fetches. Options:
- the operator's PAT, as decided on 11-18, in node secret storage;
- the OAuth token from section 1, held by Better Auth.

Proposal: the PAT on the node for intxt's own imports; the OAuth token only for what the viewer shows to the signed-in user.

## Proposed first steps

1. A smoke test: `forges` in a jco component fetches one GitHub PR.
2. Generic forge datums: repo, thread and account refs, with forges.link's keys.
3. The viewer shows the link state from section 3, starting with mutual and claimed.
4. Sign-in through Better Auth's GitHub provider in the web app, followed by the (a) proof.
