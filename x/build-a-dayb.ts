#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`./gradlew installDebug`.cwd($.relativeDir("../src/daybook_compose/"));
