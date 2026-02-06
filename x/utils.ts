import { $ as old$, CommandBuilder } from "jsr:@david/dax@0.45.0";

/**
 * This assumes that the script is run from the x/ directory or via deno run
 */
export const $ = Object.assign(
  old$.build$({
    commandBuilder: new CommandBuilder()
      .cwd(old$.path(import.meta.resolve("../")).dirname())
      .printCommand(true),
    extras: {
      relativeDir(path: string) {
        return $.path(import.meta.resolve(path)).dirname();
      },
    },
  }),
  {
    argv: Deno.args,
    env: Deno.env.toObject(),
  },
);

export const DOCKER_CMD = Deno.env.get("DOCKER_CMD") ?? "docker";

export function toolsDir() {
  return $.relativeDir("../tools/");
}

// FIXME parity with ghjk.ts: podman compose profile discovery is still flaky.
export function allProfiles(): string[] {
  return ["auth", "db", "cli"];
}
