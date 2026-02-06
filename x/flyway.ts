#!/usr/bin/env -S deno run --allow-all

import { $, DOCKER_CMD, toolsDir } from "./utils.ts";

await $
  .raw`${DOCKER_CMD} compose --profile cli run -e DB_NAME=$DB_NAME -e MIG_DIR=$MIG_DIR --rm flyway ${$.argv}`
  .cwd(
    toolsDir(),
  );
