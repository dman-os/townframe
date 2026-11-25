// The browser half of `x/daytxt-gui.ts`: runs under Node (which strips the types) from this
// package, so `@playwright/test` is the pinned runner that matches the flake's browser farm.
// Everything here is headless; nothing opens a window on a display.
import { chromium, firefox, webkit, type Browser, type Page } from "@playwright/test";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";

const engines = { webkit, chromium, firefox } as const;

type Options = {
  base: string;
  browser: keyof typeof engines;
  action: "shot" | "inspect" | "run";
  /// `home`, `settings`, `theme-lab`, or a raw `DAYTXT_SHELL_OPEN` spec (`settings,settings/counter`).
  route: string;
  out?: string;
  selector?: string;
  script?: string;
  theme?: string;
  width: number;
  height: number;
  fullPage: boolean;
};

// Named routes are `DAYTXT_SHELL_OPEN` specs; the shell starts on the stack they describe.
const routes: Record<string, string> = {
  home: "",
  settings: "settings",
  "theme-lab": "dayframe.theme-lab",
};

// The properties `inspect` reports besides the contract attributes: what the styling contract's
// layers (kit, micro, user theme) end up deciding.
const reportedStyles = [
  "display", "color", "background-color", "font-family", "font-size", "font-weight",
  "text-transform", "letter-spacing", "padding", "margin", "border-radius", "border",
  "opacity", "visibility",
];

function fail(message: string): never {
  process.stderr.write(`daytxt-gui: ${message}\n`);
  process.exit(1);
}

function url(options: Options, route: string): string {
  const open = routes[route] ?? route;
  const query = new URLSearchParams({ enable: "all" });
  if (open) query.set("open", open);
  return `${options.base}/?${query}`;
}

/// Waits for the shell's own signal (`data-daytxt-ready`, set once the runtime is built and the
/// shell mounted), then for every "Loading" status to go, and fails with the shell's own message
/// if it showed an alert instead (a start spec it refused, a runtime that could not start).
async function settle(page: Page) {
  await page.locator("html[data-daytxt-ready]").waitFor({ state: "attached", timeout: 120_000 });
  await page.locator("[role=region]").first().waitFor({ state: "visible", timeout: 60_000 });
  await page.waitForFunction(
    () => ![...document.querySelectorAll("[role=status]")].some((el) => /loading/i.test(el.textContent ?? "")),
    undefined,
    { timeout: 60_000 },
  );
  const alerts = await page.locator("[role=alert]").allInnerTexts();
  const refused = alerts.filter((text) => /DAYTXT_SHELL_OPEN|cannot start/.test(text));
  if (refused.length) fail(`the shell refused the route: ${refused.join(" | ")}`);
}

/// Applies a user-layer theme through the theme lab itself (the only way a theme is written),
/// then returns once the host reports it applied. The browser store persists it for the reload
/// that follows.
async function applyTheme(page: Page, options: Options, css: string) {
  await page.goto(url(options, "theme-lab"));
  await settle(page);
  const field = page.locator("#theme-css");
  await field.fill(css);
  const apply = page.getByRole("button", { name: "Apply theme" });
  await apply.click();
  // The lab disables Apply once the draft equals the applied theme.
  await page.locator("[aria-label='Apply theme'][disabled]").waitFor({ state: "attached", timeout: 30_000 });
}

async function main() {
  const options = JSON.parse(process.argv[2]) as Options;
  const engine = engines[options.browser] ?? fail(`unknown browser ${options.browser}`);
  const browser: Browser = await engine.launch({ headless: true });
  try {
    const context = await browser.newContext({
      viewport: { width: options.width, height: options.height },
    });
    const page = await context.newPage();
    const problems: string[] = [];
    page.on("pageerror", (error) => problems.push(`pageerror: ${error.message}`));
    page.on("console", (message) => {
      if (message.type() === "error") problems.push(`console: ${message.text()}`);
    });
    if (options.theme) {
      const { readFile } = await import("node:fs/promises");
      await applyTheme(page, options, await readFile(options.theme, "utf8"));
    }
    if (options.action !== "run" || !options.script) {
      await page.goto(url(options, options.route));
      await settle(page);
    }
    if (options.action === "shot") {
      await page.screenshot({ path: options.out, fullPage: options.fullPage });
      process.stdout.write(`${options.out}\n`);
    } else if (options.action === "inspect") {
      const found = await page.locator(options.selector!).evaluateAll(
        (elements, styles) =>
          elements.slice(0, 20).map((el) => {
            const computed = getComputedStyle(el);
            const implied: Record<string, string> = {
              button: "button", a: "link", h1: "heading", h2: "heading", h3: "heading",
              input: "textbox", textarea: "textbox", nav: "navigation", main: "main",
              section: "region", ul: "list", li: "listitem", dialog: "dialog",
            };
            return {
              tag: el.tagName.toLowerCase(),
              role: el.getAttribute("role") ?? implied[el.tagName.toLowerCase()] ?? null,
              name: el.getAttribute("aria-label"),
              text: (el.textContent ?? "").trim().slice(0, 200),
              ctClasses: [...el.classList].filter((name) => name.startsWith("ct-")),
              dataAttributes: Object.fromEntries(
                [...el.attributes].filter((a) => a.name.startsWith("data-") && a.name !== "data-dioxus-id").map((a) => [a.name, a.value]),
              ),
              styles: Object.fromEntries(styles.map((name) => [name, computed.getPropertyValue(name)])),
            };
          }),
        reportedStyles,
      );
      if (!found.length) fail(`no element matches ${options.selector}`);
      process.stdout.write(`${JSON.stringify(found, null, 2)}\n`);
    } else {
      // The agent's script: `export default async ({ page, goto, settle, context, browser }) => {...}`.
      const module = await import(pathToFileURL(resolve(options.script!)).href);
      await module.default({
        page,
        context,
        browser,
        settle: () => settle(page),
        goto: async (route: string) => {
          await page.goto(url(options, route));
          await settle(page);
        },
      });
    }
    if (problems.length) {
      process.stderr.write(`daytxt-gui: ${problems.length} page error(s):\n${problems.join("\n")}\n`);
      if (process.env.DAYTXT_GUI_STRICT) process.exitCode = 1;
    }
  } finally {
    await browser.close();
  }
}

main().catch((error) => fail(error?.stack ?? String(error)));
