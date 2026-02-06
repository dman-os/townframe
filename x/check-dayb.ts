#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

const failurePattern = /(^|\n)(FAILED|FAILURE:|^e:|\bFAILED\b|\bException\b)/im;

const processHandle = Deno.run({
  cmd: ["/bin/sh", "-lc", "./gradlew hotReloadDesktopMain"],
  stdout: "piped",
  stderr: "piped",
  cwd: $.relativeDir("../src/daybook_compose/").toString(),
});
const textDecoder = new TextDecoder();

const stdoutReader = processHandle.stdout.readable.getReader();
const stderrReader = processHandle.stderr.readable.getReader();

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

const statusValue = await processHandle.status();
stdoutReader.releaseLock();
stderrReader.releaseLock();
processHandle.close();

if (statusValue.code !== 0 || failurePattern.test(outputBuffer)) {
  console.log(outputBuffer);
  throw new Error("gradle check reported FAILED/ERROR");
}
