#!/usr/bin/env -S deno run --allow-all

import { $, DOCKER_CMD, toolsDir } from "./utils.ts";

await $.raw`${DOCKER_CMD} compose --profile auth exec kanidmd kanidmd ${$.argv}`
  .cwd(
    toolsDir(),
  );
