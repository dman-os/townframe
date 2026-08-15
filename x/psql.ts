#!/usr/bin/env -S deno run --allow-all

import { $, DOCKER_CMD, toolsDir } from "./utils.ts";

await $
  .raw`${DOCKER_CMD} compose --profile db exec postgres psql -d postgres -v SEARCH_PATH=zitadel,spicedb,granary,btress ${$.argv}`
  .cwd(
    toolsDir(),
  );
