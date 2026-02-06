#!/usr/bin/env -S deno run --allow-all

import { $, DOCKER_CMD, toolsDir } from "./utils.ts";

await $.raw`${DOCKER_CMD} compose --profile cli run --rm restate-cli ${$.argv}`
  .cwd(
    toolsDir(),
  );
