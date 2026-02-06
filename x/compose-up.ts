#!/usr/bin/env -S deno run --allow-all

import { $, DOCKER_CMD, toolsDir } from "./utils.ts";

const profiles = $.argv
  .map((prof) => `--profile ${prof}`)
  .join(" ");

await $.raw`${DOCKER_CMD} compose ${profiles} up -d`.cwd(toolsDir());
