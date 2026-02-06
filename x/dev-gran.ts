#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`trunk serve`
  .cwd($.relativeDir("../src/granary_web/"))
  .env({ TRUNK_SERVE_PORT: "3000" });
