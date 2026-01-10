## design-doc

### Usecases

- Expense tracking [MVP]
- Sleep tracking 
- Work tracking
- Goals and planning
	- Budgeting
	- Tasks

### Stack

- Compose Multiplatform
  - Best for both Android and Desktop?
  - Can't wait till a pure Rust solution is viable :)
- Custom wash runtime
  - Locally for plugin execution
  - And also through wasmcloud for server features
    - This might be pre-mature? Do we need to roll our own hosts?
- SQlite or PGLite?
- Durable execution
  - Homebrew

### Features

- Immediate proveout
  - [ ] Plugins/extensability
    - [ ] Registry
      - [ ] OCI based
    - [ ] Un-previlaged processors should go in their own branch
  - [ ] Chatbot
  - [ ] Dynamic UI
  - [ ] FUSE
  - [ ] Programmability
  - [ ] Granary

- Daybook
  - Core
    - E2ee
    - Muti-device
    - Auth
    - Multi-user 
      - Multiplayer editing
      - Collaborative vaults
    - Branch based workflow (think Patchwork)
  - Application
    - For you screen
      - Short form like swiping based call-to-action/summary
    - Inbox screen
      - Gen UI based assistant
    - Capture screen [MVP]
      - Photo [MVP]
      - Video [MVP]
        - Live transcript
      - Audio [MVP]
        - Live transcript
      - Text [MVP]
    - Config screen [MVP]
    - Drawer screen
      - Timeline [MPV]
        - Dayspan
        - Weekspan
        - Monthspan
        - Longspan
      - Path based tree [MVP]
      - Table
      - Kanban [Stretch]
    - Markdown editor
      - Subtext better?
    - Screenshots [MVP]
    - Magic wand [MVP]
    - Share reciever [MVP]
    - Print doc [Stretch]
    - Import/export
      - Markdown
    - Multiple window support [Stretch]
    - Self hosted auto-updates for android app (think Telegram)
    - Document types
      - Images
        - Thumbnails
      - Videos
        - Thumbnails
      - Audio
      - Markdown/subtext
    - Embed web browser [Stretch]
  - Server [MVP]
    - Object store [MVP]
  - Processors
    - Pseudo labeler [MVP]
    - OCR [MVP]
    - Transcript
    - Thumbnails
  - Ingest
    - Telegram bot [MVP]
    - Browser extension
    - Discord bot
    - Mastodon bot
  - CLI
    - FUSE tree on Linux (think Ethersync) [MVP]
    - Directly run processor on given file
  - Quality
    - CI/CD [MVP]
    - GUI tests
- Granary
  - Metering for LLM requests
  - Metering for object store

### Guiding stars

- I don't want to be a librarian
  - I don't want the burden of maintaining the documents of my life
- I should be able to use just one feature without being overloaded by the others.
- I should be able to get some value even during periods of low usage.
- I don't want to talk to a robot unless I want to
  - Language is a vector of manipulation (tin-foil hat?)
- Third-party server host should have zero leverage over me
  - Might be impossible but let's try
- Don't assume the resources of silicon valley
  - Captial
  - Brain cells
  - Free time
  - I.e. codebase should require low-effort maintaince
- No walled garden
  - Easily find new uses for their vault
- Long term recoverability and usability of these documents is very important
