// Style spike measurements (docs/scratch/pierre-spikes.md, "P1 style"): not a test. Renders each
// option of `micro_pierre_spike` in the served `dayframe_web`, renders the same diff with real
// Pierre 1.5.2 in a plain page, and writes into `--out`:
//   - `<option>.shell.png` / `<option>.ref.png`: the shell's rows beside Pierre's, cropped to the
//     same size, and `<option>.diff.png`: the per-pixel difference;
//   - `<option>.json`: pixel counts, computed-style differences of sample elements, and the
//     mount timing (click to the last row in the DOM and the next animation frame; one cold
//     mount that starts the guest, then `--reps` warm remounts of its section).
//
//   node pierre-style-parity.ts --ref <dir with index.html + ref.js> --out <dir>
//        [--url http://127.0.0.1:4518] [--reps 20] [--option a,b,c]
//
// `--ref` is the Vite-less bundle of `@pierre/diffs@1.5.2` built from `entry.js` (see the report):
// a page that renders the diff with `FileDiff` into `#root`, whose width the tool sets to the
// shell's.
import { chromium, type Page } from "@playwright/test";
import { parseArgs } from "node:util";
import { readFileSync, writeFileSync } from "node:fs";
import { extname, join } from "node:path";

const { values: args } = parseArgs({
  options: {
    ref: { type: "string" },
    out: { type: "string" },
    url: { type: "string", default: "http://127.0.0.1:4518" },
    reps: { type: "string", default: "20" },
    option: { type: "string", default: "a,b,c" },
  },
});
if (!args.ref || !args.out) throw new Error("--ref and --out are required");
const refDirectory = args.ref;
const out = args.out;
const reps = Number(args.reps);

const options = {
  a: {
    css: "verbatim",
    title: "Pierre rows (a) widened vocabulary",
    line: "[data-line]",
    gutter: "[data-column-number]",
    addition: "[data-line][data-line-type='change-addition']",
    deletion: "[data-line][data-line-type='change-deletion']",
    codeColumn: "[data-code]",
    root: "pre[data-diff]",
  },
  b: {
    css: undefined,
    title: "Pierre rows (b) host stylesheet",
    line: ".pd-line",
    gutter: ".pd-column-number",
    addition: ".pd-line.pd-line-type--change-addition",
    deletion: ".pd-line.pd-line-type--change-deletion",
    codeColumn: ".pd-code",
    root: "pre.pd-diff",
  },
  c: {
    css: "classes",
    title: "Pierre rows (c) class contract",
    line: ".pd-line",
    gutter: ".pd-column-number",
    addition: ".pd-line.pd-line-type--change-addition",
    deletion: ".pd-line.pd-line-type--change-deletion",
    codeColumn: ".pd-code",
    root: "pre.pd-diff",
  },
} as const;

const PROPERTIES = [
  "display",
  "color",
  "background-color",
  "font-family",
  "font-size",
  "line-height",
  "padding-left",
  "padding-right",
  "grid-template-columns",
  "grid-column-start",
  "width",
  "height",
  "min-width",
  "text-align",
  "white-space",
  "position",
  "overflow-x",
  "border-left-color",
  "box-shadow",
];

async function openSection(page: Page, title: string, enable: boolean) {
  await page.getByRole("button", { name: "Open Settings", exact: true })
    .waitFor({ timeout: 60_000 });
  if (enable) {
    await page.getByRole("button", { name: "Show Settings" }).click();
    await page.getByRole("button", { name: "Open Micro lifecycle settings" })
      .click();
    await page.getByRole("button", { name: "Enable dayframe.pierrespike micro" })
      .click();
  }
  await page.getByRole("button", { name: "Show Settings" }).click();
  await page.getByRole("button", { name: `Open ${title} settings` }).click();
}

function percentile(sorted: number[], fraction: number): number {
  return sorted[Math.min(sorted.length - 1, Math.ceil(fraction * sorted.length) - 1)];
}

const browser = await chromium.launch();
const results: Record<string, unknown> = {};
for (const id of (args.option as string).split(",") as (keyof typeof options)[]) {
  const option = options[id];
  const context = await browser.newContext({
    viewport: { width: 1400, height: 4800 },
  });
  const page = await context.newPage();
  const failures: string[] = [];
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(`console: ${message.text()}`);
  });
  await page.goto(
    `${args.url}/${option.css ? `?pierre-css=${option.css}` : ""}`,
  );
  const countRows = `document.querySelectorAll(${JSON.stringify(option.line)}).length`;
  // Cold mount: the click that starts the guest, to the rows in the DOM.
  const cold = await timeMount(page, option.title, true, countRows);
  const root = page.locator(option.root);
  const box = (await root.boundingBox())!;
  await root.screenshot({ path: join(out, `${id}.shell.png`) });
  const shellStyles = await sampleStyles(page, option, false);

  // The same diff in real Pierre at the shell's width.
  const refPage = await context.newPage();
  await refPage.route("http://pierre-ref.test/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const file = join(refDirectory, path === "/" ? "index.html" : path);
    const types: Record<string, string> = {
      ".html": "text/html",
      ".js": "text/javascript",
    };
    await route.fulfill({
      body: readFileSync(file),
      contentType: types[extname(file)] ?? "application/octet-stream",
    });
  });
  await refPage.setViewportSize({ width: 1400, height: 4800 });
  await refPage.goto("http://pierre-ref.test/?theme=dark");
  // Behind its rows the shell has the first opaque background up the tree; give the reference
  // page the same one, so the screenshots differ only where the rows do.
  const behind = await root.evaluate((pre) => {
    for (let node: Element | null = pre; node; node = node.parentElement) {
      const color = getComputedStyle(node).backgroundColor;
      if (color !== "rgba(0, 0, 0, 0)") return color;
    }
    return "rgb(0, 0, 0)";
  });
  await refPage.evaluate(({ width, behind }) => {
    document.body.style.background = behind;
    document.getElementById("root")!.style.cssText = `width:${width}px`;
  }, { width: box.width, behind });
  await refPage.waitForFunction(
    () => document.querySelector("diffs-container")?.shadowRoot
      ?.querySelectorAll("[data-line]").length === 217,
    null,
    { timeout: 60_000 },
  );
  // Pierre swaps plain text for highlighted tokens asynchronously: wait for the last row's
  // tokens (a span carrying `--diffs-token-dark`).
  await refPage.waitForFunction(
    () => {
      const rows = document.querySelector("diffs-container")!.shadowRoot!
        .querySelectorAll("[data-line]");
      return rows[rows.length - 1].innerHTML.includes("--diffs-token-dark");
    },
    null,
    { timeout: 60_000 },
  );
  const refRoot = refPage.locator("diffs-container").first();
  await refRoot.screenshot({ path: join(out, `${id}.ref.png`) });
  const refStyles = await sampleStyles(refPage, option, true);

  const pixels = await pixelDiff(
    page,
    readFileSync(join(out, `${id}.shell.png`)).toString("base64"),
    readFileSync(join(out, `${id}.ref.png`)).toString("base64"),
    join(out, `${id}.diff.png`),
  );

  // Warm remounts: leave the section for the lifecycle section and open it again.
  const warm: number[] = [];
  for (let rep = 0; rep < reps; rep++) {
    await page.getByRole("button", { name: "Open Micro lifecycle settings" })
      .click();
    await page.locator(option.line).first().waitFor({ state: "detached" });
    warm.push(await timeMount(page, option.title, false, countRows));
  }
  warm.sort((a, b) => a - b);
  const differences = Object.fromEntries(
    Object.keys(shellStyles).flatMap((sample) => {
      const changed = PROPERTIES.filter((property) =>
        shellStyles[sample]?.[property] !== refStyles[sample]?.[property]
      ).map((property) => [
        property,
        { shell: shellStyles[sample]?.[property], ref: refStyles[sample]?.[property] },
      ]);
      return [[sample, changed.length === 0 ? "identical" : Object.fromEntries(changed)]];
    }),
  );
  results[id] = {
    shellBox: box,
    pixels,
    styleDifferences: differences,
    mountMs: {
      coldFirstOpen: cold,
      warm: {
        reps,
        median: percentile(warm, 0.5),
        p95: percentile(warm, 0.95),
        min: warm[0],
        max: warm[warm.length - 1],
      },
    },
    failures,
  };
  writeFileSync(join(out, `${id}.json`), JSON.stringify(results[id], null, 2));
  console.log(id, JSON.stringify(results[id]));
  await context.close();
}
await browser.close();

// Click `title`'s section and report the milliseconds to every row being in the DOM plus the next
// animation frame, measured in the page with `performance.now()`.
async function timeMount(
  page: Page,
  title: string,
  firstOpen: boolean,
  countRows: string,
): Promise<number> {
  if (firstOpen) {
    await openSectionPrepared(page, title);
  }
  const button = page.getByRole("button", { name: `Open ${title} settings` });
  await button.waitFor();
  return await button.evaluate(
    (element, countRows) =>
      new Promise<number>((resolve) => {
        const start = performance.now();
        const observer = new MutationObserver(() => {
          if (eval(countRows) === 217) {
            observer.disconnect();
            requestAnimationFrame(() => resolve(performance.now() - start));
          }
        });
        observer.observe(document.body, { subtree: true, childList: true });
        (element as HTMLElement).click();
      }),
    countRows,
  );
}

// The first mount is timed from the section's own button, so the lifecycle enable and the
// navigation to the section list happen before the clock starts.
async function openSectionPrepared(page: Page, title: string) {
  await page.getByRole("button", { name: "Open Settings", exact: true })
    .waitFor({ timeout: 60_000 });
  await page.getByRole("button", { name: "Show Settings" }).click();
  await page.getByRole("button", { name: "Open Micro lifecycle settings" })
    .click();
  await page.getByRole("button", { name: "Enable dayframe.pierrespike micro" })
    .click();
  await page.getByRole("button", { name: `Open ${title} settings` }).waitFor();
}

// Computed styles of one sample of each kind: the first and last row, the first added and first
// deleted row, a gutter cell, and a token span. The ref's rows live in a shadow root.
async function sampleStyles(
  page: Page,
  option: (typeof options)[keyof typeof options],
  shadow: boolean,
) {
  return await page.evaluate(
    ({ option, properties, shadow }) => {
      // The reference's rows are real Pierre's, in its shadow root and its own attributes.
      const scope: ParentNode = shadow
        ? document.querySelector("diffs-container")!.shadowRoot!
        : document;
      const selectors = shadow
        ? {
          line: "[data-line]",
          gutter: "[data-column-number]",
          addition: "[data-line][data-line-type='change-addition']",
          deletion: "[data-line][data-line-type='change-deletion']",
          codeColumn: "[data-code]",
        }
        : option;
      const picks: Record<string, Element | null> = {
        firstRow: scope.querySelector(selectors.line),
        addedRow: scope.querySelector(selectors.addition),
        deletedRow: scope.querySelector(selectors.deletion),
        gutterCell: scope.querySelector(selectors.gutter),
        token: scope.querySelector(`${selectors.line} span`),
        codeColumn: scope.querySelector(selectors.codeColumn),
        pre: scope.querySelector("pre"),
      };
      const result: Record<string, Record<string, string>> = {};
      for (const [name, element] of Object.entries(picks)) {
        if (!element) {
          result[name] = { missing: "true" };
          continue;
        }
        const style = getComputedStyle(element);
        result[name] = Object.fromEntries(
          properties.map((
            property: string,
          ) => [property, style.getPropertyValue(property)]),
        );
      }
      return result;
    },
    { option, properties: PROPERTIES, shadow },
  );
}

// Compare two PNGs in the browser (no image library): per-pixel, any channel differing by more
// than `threshold` counts as different; writes the difference image.
async function pixelDiff(
  page: Page,
  shellBase64: string,
  refBase64: string,
  diffPath: string,
) {
  const diff = await page.evaluate(async ({ shellBase64, refBase64 }) => {
    const load = (base64: string) =>
      new Promise<HTMLImageElement>((resolve) => {
        const image = new Image();
        image.onload = () => resolve(image);
        image.src = `data:image/png;base64,${base64}`;
      });
    const [a, b] = await Promise.all([load(shellBase64), load(refBase64)]);
    const width = Math.min(a.width, b.width);
    const height = Math.min(a.height, b.height);
    const pixels = (image: HTMLImageElement) => {
      const canvas = document.createElement("canvas");
      canvas.width = width;
      canvas.height = height;
      const context = canvas.getContext("2d")!;
      context.drawImage(image, 0, 0);
      return context.getImageData(0, 0, width, height);
    };
    const [left, right] = [pixels(a), pixels(b)];
    const output = new ImageData(width, height);
    let different = 0;
    let large = 0;
    let max = 0;
    let firstRow = -1;
    for (let at = 0; at < left.data.length; at += 4) {
      const delta = Math.max(
        Math.abs(left.data[at] - right.data[at]),
        Math.abs(left.data[at + 1] - right.data[at + 1]),
        Math.abs(left.data[at + 2] - right.data[at + 2]),
      );
      if (delta > 0) {
        different++;
        if (firstRow < 0) firstRow = Math.floor(at / 4 / width);
      }
      if (delta > 8) large++;
      max = Math.max(max, delta);
      output.data[at] = Math.min(255, delta * 8);
      output.data[at + 1] = 0;
      output.data[at + 2] = 0;
      output.data[at + 3] = 255;
    }
    const canvas = document.createElement("canvas");
    canvas.width = width;
    canvas.height = height;
    canvas.getContext("2d")!.putImageData(output, 0, 0);
    return {
      shellSize: [a.width, a.height],
      refSize: [b.width, b.height],
      compared: width * height,
      different,
      differentOver8: large,
      maxChannelDelta: max,
      firstDifferentRow: firstRow,
      png: canvas.toDataURL("image/png").split(",")[1],
    };
  }, { shellBase64, refBase64 });
  writeFileSync(diffPath, Buffer.from(diff.png, "base64"));
  const { png: _png, ...summary } = diff;
  return summary;
}
