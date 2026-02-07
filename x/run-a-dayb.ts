#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`adb shell am start -n org.example.daybook/.MainActivity`.cwd(
  $.relativeDir("../src/daybook_compose/"),
);
