#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`scrcpy --no-audio -S`;
