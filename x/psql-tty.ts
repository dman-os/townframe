#!/usr/bin/env -S deno run --allow-all

import { $, DOCKER_CMD, toolsDir } from "./utils.ts";

await $
  .raw`${DOCKER_CMD} exec -i townframe_postgres_1 psql -U postgres -v SEARCH_PATH=zitadel,spicedb,granary,btress ${$.argv}`
  .cwd(
    toolsDir(),
  );
