import { expect, type Request, test } from "@playwright/test";
import { installDeepQuery, showCounterPanel, watchFailures } from "./shell";

// The web host's egress controls (`clay_surface` value policy, the presentation box, and the CSP
// in `x/daytxt-web.ts`). The guest vocabulary refuses hostile values before they reach the DOM,
// so these specs inject the hostile DOM directly: they pin what the browser does if a value ever
// slips past validation, which is the CSP's and the presentation box's whole job.

const FOREIGN = "http://127.0.0.1:9";

test("the app boots under the CSP and a DOM-injected remote image, css url() and javascript: href fetch and run nothing", async ({ page }) => {
  const failures = watchFailures(page);
  await installDeepQuery(page);
  // Chromium announces a CSP-blocked request with a `request` event and then fails it with the
  // error text `csp` (observed on the pinned chromium-1248 headless shell) before it leaves the
  // renderer, so a `request` event alone does not mean a fetch. A request that reached the network instead fails with a connection error
  // (nothing listens on the discard port) or finishes, so any foreign request whose failure is
  // not the CSP block counts as an egress.
  const foreignRequests: Request[] = [];
  page.on("request", (request) => {
    if (request.url().startsWith(FOREIGN)) foreignRequests.push(request);
  });

  const response = await page.goto("/");
  const csp = response?.headers()["content-security-policy"] ?? "";
  // No `'unsafe-inline'` in `script-src`: that is what stops a `javascript:` href.
  expect(csp).toContain("script-src 'self' 'wasm-unsafe-eval';");
  expect(csp).toContain("img-src 'self' blob: data:");
  expect(csp).toContain("form-action 'none'");
  expect(csp).toContain("frame-src 'none'");
  expect(csp).toContain("object-src 'none'");
  expect(csp).toContain("base-uri 'none'");

  // The shell still boots: its home lists Settings, and a clean boot logged no CSP violation
  // (a violation is a console error, which `watchFailures` collects).
  await expect(
    page.getByRole("button", { name: "Open Settings", exact: true }),
  ).toBeVisible({ timeout: 60_000 });
  // A guest presentation (and so a shadow-rooted mount) must be on screen to inject into.
  await showCounterPanel(page);
  expect(failures).toEqual([]);

  const violated = await page.evaluate(async (foreign) => {
    const violations: string[] = [];
    document.addEventListener(
      "securitypolicyviolation",
      (event) => violations.push(event.effectiveDirective),
    );
    const host = (window as any).deepQueryAll("[data-clay-presentation]")[0];
    const img = document.createElement("img");
    img.src = `${foreign}/img.png`;
    const styled = document.createElement("div");
    styled.style.setProperty("background", `url(${foreign}/bg.png)`);
    styled.style.width = "10px";
    styled.style.height = "10px";
    const link = document.createElement("a");
    link.href = "javascript:window.__pwned = true";
    link.textContent = "bait";
    host.append(img, styled, link);
    link.click();
    // Violations are reported asynchronously.
    await new Promise((resolve) => setTimeout(resolve, 500));
    return violations;
  }, FOREIGN);

  const reachedNetwork = foreignRequests
    .filter((request) => request.failure()?.errorText !== "csp")
    .map(
      (request) =>
        `${request.url()} ${request.failure()?.errorText ?? "not failed"}`,
    );
  expect(reachedNetwork).toEqual([]);
  expect(violated).toContain("img-src");
  expect(violated.some((directive) => directive.startsWith("script-src"))).toBe(
    true,
  );
  expect(await page.evaluate(() => (window as any).__pwned)).toBeUndefined();
});

test("a guest node with position:fixed and inset:0 cannot cover the shell outside its presentation box", async ({ page }) => {
  watchFailures(page);
  await installDeepQuery(page);
  await page.goto("/");
  await expect(
    page.getByRole("button", { name: "Open Settings", exact: true }),
  ).toBeVisible({ timeout: 60_000 });
  // A layout presentation with the counter's presentation nested in it, beside the shell chrome.
  await showCounterPanel(page);

  const result = await page.evaluate(() => {
    const boxes: Element[] = (window as any).deepQueryAll(
      "[data-clay-presentation]",
    );
    const viewport = { w: window.innerWidth, h: window.innerHeight };
    const area = (el: Element) => {
      const r = el.getBoundingClientRect();
      return r.width * r.height;
    };
    // The smallest presentation box that is on screen: a panel, not the whole shell.
    const box = boxes
      .filter((el) => area(el) > 0)
      .sort((a, b) => area(a) - area(b))[0];
    const boxRect = box.getBoundingClientRect();
    const intruder = document.createElement("div");
    intruder.id = "intruder";
    intruder.style.cssText =
      "position:fixed;top:0;right:0;bottom:0;left:0;z-index:2147483647;background:red";
    box.append(intruder);
    const rect = intruder.getBoundingClientRect();
    // A point outside the box, inside the viewport.
    const outside = boxRect.left > 8
      ? { x: 2, y: 2 }
      : boxRect.top > 8
      ? { x: 2, y: 2 }
      : { x: Math.min(viewport.w - 2, boxRect.right + 2), y: 2 };
    // `document.elementFromPoint` stops at a shadow host; descend so a hit on the intruder inside
    // a mount's shadow root is seen as the intruder, not as its host.
    let hit: Element | null = document.elementFromPoint(outside.x, outside.y);
    while (hit?.shadowRoot) {
      const inner = hit.shadowRoot.elementFromPoint(outside.x, outside.y);
      if (!inner || inner === hit) break;
      hit = inner;
    }
    return {
      boxes: boxes.length,
      boxRect: JSON.parse(JSON.stringify(boxRect)),
      rect: JSON.parse(JSON.stringify(rect)),
      viewport,
      outsideHitIsIntruder: hit?.id === "intruder",
      outsideInsideBox: outside.x >= boxRect.left &&
        outside.x <= boxRect.right &&
        outside.y >= boxRect.top &&
        outside.y <= boxRect.bottom,
    };
  });

  expect(result.boxes).toBeGreaterThan(1);
  // The box is smaller than the viewport, so covering it entirely would be visible as equality.
  expect(result.boxRect.width * result.boxRect.height).toBeLessThan(
    result.viewport.w * result.viewport.h,
  );
  expect(result.rect.left).toBeGreaterThanOrEqual(result.boxRect.left - 1);
  expect(result.rect.top).toBeGreaterThanOrEqual(result.boxRect.top - 1);
  expect(result.rect.right).toBeLessThanOrEqual(result.boxRect.right + 1);
  expect(result.rect.bottom).toBeLessThanOrEqual(result.boxRect.bottom + 1);
  expect(result.outsideInsideBox).toBe(false);
  expect(result.outsideHitIsIntruder).toBe(false);
});
