# intxt: forge credentials, sign-in and PR links

Status: Proposal, second draft (ledger D80, D81); forge work deferred by D85
Owner: intxt lane
Updated: 2026-11-24
Scope: Where forge credentials live, what signing in means for the web viewer, and how a PR and an intxt are linked.

It covers how the forge integration starts:
- where forge credentials live;
- what "sign in" means in the web app;
- how a PR and an intxt are linked so a reader can trust the link.

It proposes defaults to talk over. The rule in the next section is the operator's (D81); the rest is undecided.

**Changed in the second draft** (operator, 2026-11-24):
- The web app signs the user in to the forge, not to itself.
- No server keeps identities, sessions or relationships. Better Auth leaves the viewer.
- Credentials are designed for the p2p and webnode future first.

## The rule: credentials live with the user, servers pass through

daytxt is p2p first, so a server must not be where identity and relationships accumulate. For every service integration, forges first:

- **The user's side holds the credential.** That is the user's node today, and their webnode in the browser later.
- **A server only passes through.** Anything a server does with a credential (the web app's server functions, a relay, a CORS proxy) is stateless: the credential comes with the request and is gone after it. No session table, no stored token, no "this GitHub account is that actor" kept on the server.
- **"Sign in to GitHub", not "sign in to intxt".** The web app has no accounts. Signing in gives the browser a forge credential and nothing else.

This also rules out a plain Better Auth deployment for the viewer, because its sessions and stored OAuth tokens are server state. Better Auth stays a possibility for services that are meant to hold accounts (btress).

## Getting a credential without registering every tool

Classic OAuth assumes a client registered with the forge, a fixed redirect URL and a secret on a server. A webnode or a throwaway tool has none of these. What each kind of forge offers (from memory, to verify per forge before building on it):

- **Personal access tokens.** Every forge has them: GitHub's fine-grained tokens can be scoped to one repo and read-only; GitLab, Forgejo, Gitea and Codeberg have their own. No registration. `gh auth token` hands one over on a developer's machine. This is the universal floor.
- **Device flow** (RFC 8628). GitHub supports it for OAuth and GitHub Apps. The tool shows a code and the user approves it at github.com. It needs only the app's public client id: no secret and no redirect URL. One daytxt-wide client id could ship in every node and webnode.
  - The catch, as remembered: GitHub's OAuth endpoints don't send CORS headers, so a browser can't call them directly and a webnode needs a stateless proxy for those two requests. That would be a Citizenship register entry, owned by whoever runs relays.
- **Client ids that are URLs.** ATProto OAuth (so Tangled) names a client by the URL of a metadata document the client publishes, so nothing is registered in advance. The MCP authorization spec moved the same way (client ID metadata documents), for the same reason: every agent is a new client.
- **Dynamic client registration** (RFC 7591) lets a client register itself at runtime. Few forges offer it, and GitHub doesn't.

**Proposal, in order:**
1. **Tokens first.** The user pastes a token, or the node reads `gh auth token`. This works on every forge today, and the forges library takes it directly.
2. **GitHub device flow** under one daytxt client id, as the "Sign in to GitHub" button, with the CORS proxy above for the browser.
3. **ATProto OAuth** for Tangled, which needs nothing registered.
4. Web-redirect OAuth only for a forge that offers nothing else.

## Where a credential is kept and used

- **On a node:** in node secret storage, which nodules owns and which doesn't exist yet. It is used by the forge cartridge's handler through egress, the `#net` permbind (#198). Until secret storage exists, the CLI passes the token per call. That is today's `gh` path, and it is entry C5.
- **In the web app, for the MVP:** in the browser, in session storage by default and in local storage only if the user asks to be remembered. It is used:
  - from the browser, where the forge allows CORS for API calls (GitHub's REST API does, as remembered);
  - otherwise through a server function that receives it with each request and keeps nothing.
- **In a webnode, later:** in the webnode's own secret store; the same code as the node's, built for wasm.

The forges library (`forges`, the TypeScript client behind forges.link) fits this. A provider takes a credential and never runs a sign-in flow itself: `github({ auth: { type: 'token', token } })`. Forgejo, GitLab and the rest take tokens the same way, Tangled also takes `anonymous`, `app_password` or `oauth`, and there's a `FORGES_*` environment-variable guide.

## A PR and an intxt

What exists:
- **The intxt names the PR:** `githubPullEntry`, written by the intxt author's actor and signed by their node.
- **The PR names the intxt:** the description's `<details>` block of `dtxt:doc:<drawer>?via=<node key>` URLs (`day x intxt pr-block`).
- **Who wrote something on our side is claimed, not attested** (C7).

**Proposal: the link is the two references, checked by whoever reads them.** No server binds accounts to actors.

| State | Means |
| --- | --- |
| **mutual** | the PR names the intxt and the intxt names the PR |
| **claimed by the PR** | the PR names the intxt, but the intxt doesn't name it |
| **claimed by the intxt** | the intxt names the PR, but the PR doesn't name it |

- Anyone can compute the state: the web app, an anonymous viewer, a node, a reviewing agent. Reading a public PR needs no credential at all.
- **The caveat:** maintainers can edit a PR's description, so "the PR names it" means someone with write access on the PR named it, not necessarily its author.
  - Later, the block can carry the actor's signature over the PR's repo key and number.
  - That would show that the intxt's actor put the link there, whoever edited the text.
- **Account to actor, later and still serverless:** a self-published proof, as Keyoxide does. The actor signs "I am `github:<user id>`" in its own drawer, and the forge account points back at the actor URL from its profile, a gist or a repo file. Anyone can verify it by fetching both.
  - Combined with a mutual link, that gives a third state, **attested**: the intxt's author is the PR's author.
- An account is named by the forge's own user id, not the login, which can be renamed (forges.link keys repos the same way). The datum is generic (`forgeAccount`: forge, instance, id, login at last sight), not intxt's (C5).

## Where forge code runs

- **The forges library, run in a jco component** (as `src/btress_auth` runs Better Auth). It covers 14 forges and normalises refs, states and events. Its README says it runs in browsers with no runtime dependencies, which suggests it also runs under jco's JS runtime with `fetch`: a smoke test settles it.
- **No Rust port was found.** `vcs-forge` (GitHub, GitLab and Gitea PR lifecycle) is CLI-oriented.
- Our datums take forges.link's vocabulary either way:
  - refs with `forge` and `instance`;
  - keys built from repo ids;
  - normalised states, with the raw value kept.
  
  So replacing the implementation later doesn't change what is stored.
- **On the node, the component is the forge cartridge's handler,** with the credential from node secret storage and egress through `#net`. That is the citizenship answer.
- **In the web app**, the same library runs in the browser bundle or in the server function, with the credential the request brings.

## Open

1. Is the browser's session storage the right default for a pasted token, or should the web app never keep it past the page?
2. Who runs the device-flow CORS proxy: every relay provider, or a daytxt-run endpoint? It is stateless either way.
3. Does a public PR need any credential to show? GitHub's unauthenticated rate limit is 60 requests an hour per IP, which a shared relay exhausts quickly. Proposal: the viewer works anonymously and asks for a token when the limit hits.
4. Should the signed PR block (the caveat above) come in the MVP or after it?

## Proposed first steps

1. A smoke test: `forges` in a jco component fetches one GitHub PR with a token.
2. Generic forge datums: repo, thread and account refs, with forges.link's keys.
3. The web app computes and shows the link state (mutual or claimed) for an intxt's PRs, anonymously, with a token prompt when GitHub's limit hits.
4. GitHub's device flow, as "Sign in to GitHub".
