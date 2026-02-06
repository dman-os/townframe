#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

const commandOutput = await $`./x/kanidmd.ts recover-account idm_admin -o json`
  .text();
const passwordMatch = commandOutput.match(/"password":"([^"]*)"/);
if (!passwordMatch) {
  throw new Error("failed to parse password from kanidmd output");
}
const adminPassword = passwordMatch[1];

const envPath = $.path(".env");
const envRaw = await envPath.readText();
const envPrefix = "KANIDM_ADMIN_PASSWORD=";
let lineReplaced = false;

const nextEnv = [
  ...envRaw
    .split("\n")
    .filter((lineValue, lineIndex, allLines) => {
      // Preserve trailing newline behavior from ghjk task.
      if (lineIndex === allLines.length - 1 && lineValue === "") {
        return false;
      }
      return true;
    })
    .map((lineValue) => {
      if (lineValue.startsWith(envPrefix)) {
        lineReplaced = true;
        return `${envPrefix}${adminPassword}`;
      }
      return lineValue;
    }),
  lineReplaced ? "" : `${envPrefix}${adminPassword}`,
].join("\n");

await envPath.writeText(nextEnv);
