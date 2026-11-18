import { expect, test } from "@playwright/test";
import { openReview, screenshot, watchFailures } from "./shell";

// A plain serve (`playwright.unconfigured.config.ts`, no `DAYFRAME_WEB_REVIEW_FIXTURE`) serves the
// shipped review guest. The web composition grants it no configuration entry, and it must say so
// instead of showing sample pull requests as if they were real (commit f2f14a5d).
test("the shipped review micro without a node reads Unconfigured", async ({
  page,
}) => {
  const failures = watchFailures(page);
  const review = await openReview(page);

  await expect(review.getByRole("alert")).toContainText("not configured", {
    timeout: 60_000,
  });
  await expect(review.getByRole("button", { name: /#15\d/ })).toHaveCount(0);
  await screenshot(page, "unconfigured");
  expect(failures).toEqual([]);
});
