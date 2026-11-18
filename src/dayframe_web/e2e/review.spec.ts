import { expect, test } from "@playwright/test";
import { openReview, screenshot, watchFailures } from "./shell";

// The intxt review micro (ledger D62) on the browser shell, over the fixture node
// (`Fixture::showcase`, served when `DAYFRAME_WEB_REVIEW_FIXTURE=1`, which the config sets).
// Everything is found by role, accessible name and text, never by markup, so a restyle of the
// screens does not break it; the screens' own look is out of scope here.
const pulls = [
  "intxt: read a PR and its intxts on the phone",
  "chore: pin the review micro's lockfile",
  "intxt: a review from another node",
  "chore: a description with a broken block",
];

test("the pull request list shows the node's pull requests", async ({
  page,
}) => {
  const failures = watchFailures(page);
  const review = await openReview(page);

  await expect(
    review.getByRole("button", { name: pulls[0] }),
  ).toBeVisible({ timeout: 60_000 });
  for (const title of pulls) {
    await expect(review.getByRole("button", { name: title })).toBeVisible();
  }
  await screenshot(page, "pull-list");
  // One row per pull request, each opening one.
  await expect(review.getByRole("button", { name: /#15\d/ })).toHaveCount(
    pulls.length,
  );
  await expect(review.getByText("dman-os/townframe#150 · open")).toBeVisible();
  await expect(
    review.getByText("dman-os/townframe#151 · merged"),
  ).toBeVisible();
  // The root screen has no way back.
  await expect(review.getByRole("button", { name: "Back" })).toHaveCount(0);
  expect(failures).toEqual([]);
});

test("a pull request shows its description and the outcome of each intxt it names", async ({
  page,
}) => {
  const failures = watchFailures(page);
  const review = await openReview(page);
  const back = review.getByRole("button", { name: "Back", exact: true });

  // Found, found, and a document that is no intxt.
  await review.getByRole("button", { name: pulls[0] }).click({
    timeout: 60_000,
  });
  await expect(
    review.getByRole("heading", { name: pulls[0] }),
  ).toBeVisible();
  await expect(
    review.getByText(
      "Reads a pull request and the intxts it names on the phone.",
    ),
  ).toBeVisible();
  // The machine block is not a collapsible `<details>` element on the screen.
  await expect(review.locator("details")).toHaveCount(0);
  await expect(
    review.getByRole("button", { name: /intxt: commits live with the intxt/ }),
  ).toContainText("3 entries");
  await expect(
    review.getByRole("button", { name: /review micro: the PR on the phone/ }),
  ).toBeVisible();
  await expect(
    review.getByRole("alert").filter({ hasText: "Not an intxt" }),
  ).toHaveCount(1);
  await screenshot(page, "pull-view");
  await back.click();

  // A document the node does not hold is unreadable, and its row says why.
  await review.getByRole("button", { name: pulls[2] }).click();
  const unreadable = review.getByRole("alert").filter({
    hasText: "Unreadable:",
  });
  await expect(unreadable).toContainText("intxt: held by another node", {
    timeout: 60_000,
  });
  await expect(unreadable).toContainText("does not hold this document");
  await back.click();

  // A malformed block names nothing and is reported, not read as an empty list.
  await review.getByRole("button", { name: pulls[3] }).click();
  await expect(review.getByRole("alert")).toContainText(
    "The intxt block in this description cannot be read",
    { timeout: 60_000 },
  );
  await back.click();

  // A description without a block names no intxts.
  await review.getByRole("button", { name: pulls[1] }).click();
  await expect(review.getByText("This pull request names no intxts.")).toBeVisible({
    timeout: 60_000,
  });
  expect(failures).toEqual([]);
});

test("an intxt shows its entries and a commit's patch in the diff view, and back unwinds", async ({
  page,
}) => {
  const failures = watchFailures(page);
  const review = await openReview(page);
  const back = review.getByRole("button", { name: "Back", exact: true });
  const intxt = "intxt: commits live with the intxt";

  await review.getByRole("button", { name: pulls[0] }).click({
    timeout: 60_000,
  });
  await review.getByRole("button", { name: new RegExp(intxt) }).click({
    timeout: 60_000,
  });
  // The title is in the app bar and again over the guide.
  await expect(review.getByRole("heading", { name: intxt }).first()).toBeVisible();
  await expect(
    review.getByText("Commits travel with the intxt that names them."),
  ).toBeVisible();
  // Its three entries, in the body's order: two commit ranges and the pull request.
  await expect(review.getByText("9a0e11c..4c1f0a7")).toBeVisible();
  await expect(review.getByText("77b2d03")).toBeVisible();
  await expect(review.getByText("dman-os/townframe/150")).toBeVisible();
  // The first commit carries a patch, shown in the diff region.
  const patch = review.getByRole("region", { name: "Patch" });
  await expect(patch).toHaveCount(1);
  await expect(patch).toContainText("read the PR");
  await screenshot(page, "intxt-view");
  expect(failures).toEqual([]);

  // Back: intxt -> pull request -> list.
  await back.click();
  await expect(review.getByRole("heading", { name: pulls[0] })).toBeVisible();
  await expect(patch).toHaveCount(0);
  await back.click();
  await expect(review.getByRole("button", { name: pulls[1] })).toBeVisible();
  await expect(back).toHaveCount(0);
});
