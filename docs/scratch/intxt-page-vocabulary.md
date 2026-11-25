# intxt: the intxt page's vocabulary

Owner: intxt lane; operator decides; claytxt owns the look (D88).
Updated: 2026-11-25
Scope: The parts of one intxt's page, named, with what each shows and does. Describes structure, not appearance. Narrows `intxt-app-structure.md` to the page the operator asked to start from (D97).

Status: Structure accepted by the operator on 2026-11-25 from a clickable mock. The mock's visuals were rejected as slop; see Restraint.

## The page

From top to bottom: header, description, tabs, then the selected tab's view. Related intxts and open discussions sit beside the view when the window is wide and below the header when it is narrow.

## Parts

- **Header.** The title and the intxt's short id. Whether work is going on (an actor active within the last few minutes) and when the last activity was. **Actor chips** name the people and agents who took part and mark the ones active now. **Source chips** name repositories, branches and PRs; each links out.
- **Description.** Markdown, pinned above the tabs like a PR's body, folded after a few lines.
- **Tabs.** Conversation, Commits, Files changed, Sessions, Guide. Each tab shows its count. Checks joins when anything records checks.
- **Conversation.** The feed: every item in the intxt, newest first, with day dividers.
- **Item.** One anatomy for every kind: a mark for the kind, the actor, a verb line ("added 2 commits", "commented on transcript.rs:284–291"), the time, and a folded preview specific to the kind. In the app, actions follow (reply, resolve); the web has none (D86).
- **Running item.** A session in progress is one item that updates in place: turns, tool calls, files, the last prompt.
- **Burst.** Consecutive items of one kind by one actor within a short time fold into one ("added 5 commits over 20 minutes"), expandable.
- **Generic item.** A kind the page doesn't know shows its kind name, actor, time and key fields. It is never dropped (FDR 006).
- **Gap marker.** What couldn't be read is stated where it would have been (D95).
- **New items.** While the reader is scrolled into the feed, live items wait behind a "N new" control instead of moving what they are reading; at the top they appear directly.
- **Commits.** Revisions (each `add` of a range is one), newest first; each lists its commits with subject, sha and +/−.
- **Files changed.** The files of the selected revision with their diffs and discussions inline at their anchors. Ordered by path, or by the guide when there is one.
- **Sessions.** One per recorded session: the summary, then turns with the operator's prompts, the agent's messages, and tool calls folded to one line each. A tool call that changed files links into Files changed.
- **Guide.** The author's guide document: sections in the author's order, each with the hunks it references inline.
- **Related intxts.** Other intxts by relationship (stacked on, shares a session, referenced by), each with title and short id. Making a link also adds an item to the feed.
- **Discussions.** The open comment threads, each with its anchor and author, linking to where it sits.

## Restraint

The look is claytxt's (D88). Until their design session, the page is plain: text, rules, monospace for code and shas, the kit's neutral tokens. Specifically none of the tropes the operator flagged (slopstrap.mikelacher.com):

- no blinking or pulsing status dots: "active now" is words;
- no colored stripes down the side of cards or sections;
- no pill badges for states or counts where plain text reads;
- no colored avatar circles or glyph bubbles: actors are names, kinds are words or a plain mark;
- no entrance or hover animation;
- no emoji;
- cards only where an element is a separate object (a file's diff, a preview), not around every block.

## Open

- The marks for item kinds: a word ("commit", "session") or a plain symbol. claytxt's call in the design session.
- How a revision is presented when a later `add` repeats an earlier range's commits (F46's reader packs change what a revision holds).
