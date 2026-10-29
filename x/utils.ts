import { $ as old$, CommandBuilder, type Path } from "jsr:@david/dax@0.45.0";

/**
 * This assumes that the script is run from the x/ directory or via deno run
 */
export const $ = Object.assign(
  old$.build$({
    commandBuilder: new CommandBuilder()
      .cwd(old$.path(import.meta.resolve("../")))
      .printCommand(true),
    extras: {
      dbg<T>(val: T, ...ctx: unknown[]) {
        console.log("DBG", val, ...ctx);
        return val;
      },
      relativeDir(path: string) {
        return $.path(import.meta.resolve(path));
      },
      chdir(path: string | Path) {
        Deno.chdir($.path(path).toString());
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

/**
 * Point Cargo at the `.env` `CARGO_TARGET_DIR` when the shell did not set one. The dev shell
 * sources `.env`, but the CI shells do not, and a target dir inside the checkout is shared by
 * nothing and copied into the store by path flakes.
 */
export async function useDotenvTargetDir() {
  const current = Deno.env.get("CARGO_TARGET_DIR");
  let configured = current;
  if (!configured) {
    const dotenvPath = $.relativeDir("../").join(".env");
    if (!await dotenvPath.exists()) {
      return;
    }
    const dotenvTarget = (await dotenvPath.readText())
      .match(/^(?:export\s+)?CARGO_TARGET_DIR\s*=\s*["']?([^"'\r\n#]+)["']?/m)
      ?.[1]
      ?.trim();
    if (dotenvTarget) {
      configured = $.relativeDir("../").resolve(dotenvTarget).toString();
    }
  }
  if (!configured) {
    return;
  }

  const worktree = $.path(Deno.cwd()).basename();
  let subdir = worktree;
  const explicitSide = Deno.env.get("CONTESTANT_SIDE");
  if (explicitSide === "a" || explicitSide === "b") {
    subdir = `side-${explicitSide}`;
  } else {
    const match = worktree.match(/^generation-\d+-([ab])$/);
    if (match) {
      subdir = `side-${match[1]}`;
    }
  }

  if (/^generation-\d+-[ab]$/.test($.path(configured).basename())) {
    configured = $.path(configured).parent()!.toString();
  }

  const shared = Deno.env.get("TOWNFRAME_SHARED_TARGET_DIR") === "1" ||
    $.path(configured).basename() === subdir;
  const resolved = shared
    ? configured
    : $.path(configured).join(subdir).toString();
  await Deno.mkdir(resolved, { recursive: true });
  Deno.env.set("CARGO_TARGET_DIR", resolved);
}
