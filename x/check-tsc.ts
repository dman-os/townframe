#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`pnpm -r exec tsc --noEmit`.cwd($.relativeDir("../src"));
