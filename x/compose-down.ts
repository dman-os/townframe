#!/usr/bin/env -S deno run --allow-all

import { $, allProfiles, DOCKER_CMD, toolsDir } from "./utils.ts";

const profileList = $.argv.length ? $.argv : allProfiles();
const profiles = profileList
  .map((prof) => `--profile ${prof}`)
  .join(" ");

await $.raw`${DOCKER_CMD} compose ${profiles} down -v`.cwd(toolsDir());
