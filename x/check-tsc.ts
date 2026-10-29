#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`pnpm -r --filter '!@townframe/btress_*' --fail-if-no-match exec tsc --noEmit`
  .cwd($.relativeDir("../src"));
