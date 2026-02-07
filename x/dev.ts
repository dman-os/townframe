#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`wash build`.cwd($.relativeDir("../src/btress_api/"));
