# > *townframe*

Experimental.

> [!NOTE] 
>
> #### what's in the oven ‍‍👩🏿‍🍳?
> 
> - `mltools` base.
>   - This is where supporting platform for ML based features will go.

## Daybook

Daybook is an experiental attempt to build to build a "notes app" heavily informed by my tastes and capabilites.

It currently consists of:
- WIP CLI [^](./src/daybook_cli/)
- WIP Compose Multiplatform app [^](./src/daybook_compose/)

## Why?

I think tools like these tend to be highly personal to each person and I recommend everyone try to build one for themselves.
Either from scratch as is foolishly done here or by customizing an existing platform like Obsidian, Emacs, Notion and soforth

Specifically, daybook design is informed by tech I find shiny, gaps I see in the current landscape for such solutions and my personal politics.

- Shiny tech
  - Everything that is and around LLMs.
  - Uses Automerge for an offline and local-first experience
    - With futue expectations around collaborative grounding
  - Uses Compose Multiplatform for a reliable experience on Android and Desktop
  - Tries to leverage WASM as a plugin system. 
- Gaps
  - Emacs/Vim
    - State of the art editing experience
    - Pre-mobile tech
  - Notion 
    - Excellent design 
    - Questionable performance on Android
  - Obsidian 
    - Design is excellent and the performance acceptable on mobile
    - The plugins seem insecure
    - Collaboration is secondary?
- Politics
  - I'm an adblock person.

Right now, it's in the early experimentation phase trying to prove out tech foundations.
There are zero features implemented.

### Prior art | Giant shoulders

- [Patchwork](https://www.inkandswitch.com/patchwork)
  - The lab's entire corpus really.
- [Obsidian](https://obsidian.md/)
  - The right senseblities.
    - File first.
    - Easy portability with minimal lock-in.
- [Notion](https://www.notion.so/)
  - Great usablitiy that's accessible.
  - World class collaboration.
- [Org mode](https://orgmode.org/)
  - Excellent power user design.
    - Unix senseblities.
- [Anytype](https://anytype.io/)
  - Encrypted offline-first with sync/collaboration
- TODO: mention more

### Points of research and experimentation

More details can be found in the [design docs](./docs/DEVDOC/design.md) but actively undecided questions include:

- ~~Local~~ Privacy friendly machine learning use
  - If I want to, I ought to be able to self-host for for me and mine.
- Mobile first design
  - I and many others just won't use it unless it's easily usable on the go.
- Long term sustainablity
  - How to become sustainable without relying on VC money and its strings.
- Long term use
  - How can I have the whole or parts of the system convincingly useful even on my deathbed.
  - Using local-first design where the server is optional is a big help here.
- Extensible document store/design
  - A lot of solutions here rely on webtech to support maximum programmability.
    - How to adapt that while previous constraints hold?
  - One early intiution is that we shouldn't adapt a single document format like markdown as a default and allow generic documents.

## Want to hack on this?

Feel free to throw yourself or tokens at the code though I'd personally appreciate help the most in the design and research aspect.
