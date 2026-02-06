#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $.raw`./src/daybook_compose/gradlew ${$.argv}`;
