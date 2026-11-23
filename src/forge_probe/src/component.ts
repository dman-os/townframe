import { getEnvironment } from "wasi:cli/environment@0.2.6";
import { github } from "forges/github";
import { forgejo } from "forges/forgejo";
import { createForges } from "forges/web";
import { E as threadKey } from "forges-internal-model";

async function probe() {
  // wasi:cli/environment cannot be called during Wizer initialisation, so env is read per run.
  const env = new Map(getEnvironment());
  const url = env.get("FORGE_URL");
  if (!url) throw new Error("FORGE_URL is required");
  const token = env.get("FORGE_TOKEN");
  const auth = token ? ({ type: "token", token } as const) : undefined;
  const forges = createForges([
    github({ auth }),
    forgejo({ baseUrl: "https://codeberg.org", auth }),
  ]);
  const parsed = forges.parseUrl(url);
  if (!parsed?.thread) throw new Error(`no thread in url: ${url}`);
  const thread = await parsed.provider.threads.get(parsed.thread);
  console.log(
    JSON.stringify(
      {
        forge: parsed.provider.forge,
        instance: parsed.provider.instance,
        authKind: parsed.provider.authKind,
        key: threadKey(thread.ref),
        thread,
      },
      null,
      2,
    ),
  );
}

export const run = {
  async run() {
    try {
      await probe();
    } catch (error) {
      console.error("probe failed:", error);
      throw error;
    }
  },
};
