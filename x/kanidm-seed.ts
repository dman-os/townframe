#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`${$.path(import.meta.resolve("./kanidm-login.ts"))}`;
await $`cargo x seed-kanidm`;
