#!/usr/bin/env -S deno run --allow-all

import { $, allProfiles, DOCKER_CMD, toolsDir } from "./utils.ts";

const profilesString = allProfiles()
  .map((profileName) => `--profile ${profileName}`)
  .join(" ");

await $.raw`${DOCKER_CMD} compose ${profilesString} logs ${$.argv}`.cwd(
  toolsDir(),
);
