#!/usr/bin/env -S deno run --allow-all

import { $, DOCKER_CMD, toolsDir } from "./utils.ts";

await $
  .raw`${DOCKER_CMD} compose --profile cli run -e DB_NAME=daybook -e MIG_DIR=./src/daybook_api/migrations --rm flyway migrate`
  .cwd(
    toolsDir(),
  );
