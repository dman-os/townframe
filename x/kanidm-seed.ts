#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`./x/kanidm-login.ts`;
await $`cargo x seed-kanidm`;
