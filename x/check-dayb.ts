#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

const failurePattern = /(^|\n)(FAILED|FAILURE:|^e:|\bFAILED\b|\bException\b)/im;

const processHandle = new Deno.Command("/bin/sh", {
  args: ["-lc", "./gradlew hotReloadDesktopMain"],
  stdout: "piped",
  stderr: "piped",
  cwd: $.dbg($.relativeDir("../src/daybook_compose/").toString()),
}).spawn();
const textDecoder = new TextDecoder();

const stdoutReader = processHandle.stdout.getReader();
const stderrReader = processHandle.stderr.getReader();

let outputBuffer = "";

const appendChunk = (chunkValue: Uint8Array | null) => {
  if (!chunkValue) {
    return;
  }
  outputBuffer += textDecoder.decode(chunkValue);
};

const pumpReader = async (reader: ReadableStreamDefaultReader<Uint8Array>) => {
  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    appendChunk(value || null);
  }
};

await Promise.all([pumpReader(stdoutReader), pumpReader(stderrReader)]);

const statusValue = await processHandle.status;
stdoutReader.releaseLock();
stderrReader.releaseLock();
// processHandle.close();

if (statusValue.code !== 0 || failurePattern.test(outputBuffer)) {
  console.log(outputBuffer);
  throw new Error("gradle check reported FAILED/ERROR");
}
