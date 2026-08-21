import { type APIRequestContext, expect, test } from "@playwright/test";

const inbucketURL = "http://localhost:9021";

async function readMagicLink(
  request: APIRequestContext,
  mailbox: string,
): Promise<string | undefined> {
  const listResponse = await request.get(
    `${inbucketURL}/api/v1/mailbox/${mailbox}`,
  );
  if (!listResponse.ok()) return undefined;

  const messages = await listResponse.json();
  const message = messages.at(-1);
  if (!message) return undefined;

  const detailResponse = await request.get(
    `${inbucketURL}/api/v1/mailbox/${mailbox}/${message.id}`,
  );
  if (!detailResponse.ok()) return undefined;

  const detail = await detailResponse.json();
  const body = `${detail.body?.text ?? ""}\n${detail.body?.html ?? ""}`;
  const match = body.match(
    /https?:\/\/localhost:8071\/api\/auth\/magic-link\/verify\?[^\s<>"']+/,
  );
  return match?.[0].replaceAll("&amp;", "&");
}

test("logs in with a magic link", async ({ page, request }, testInfo) => {
  const mailbox = `playwright-${Date.now()}-${testInfo.workerIndex}`;
  const email = `${mailbox}@example.com`;

  await test.step("open the sysadmin login page", async () => {
    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "btress sysadmin" }),
    ).toBeVisible();
    await expect(page.getByLabel("Email address")).toBeVisible();
  });

  await test.step("request a magic link", async () => {
    await page.getByLabel("Email address").fill(email);
    await page.getByRole("button", { name: "Send magic link" }).click();
    await expect(
      page.getByText("Check your email for the login link."),
    ).toBeVisible();
  });

  let magicLink: string | undefined;
  await expect
    .poll(
      async () => {
        magicLink = await readMagicLink(request, mailbox);
        return magicLink ?? "";
      },
      {
        message: "waiting for the magic-link email in Inbucket",
        timeout: 15_000,
        intervals: [250, 500, 1_000],
      },
    )
    .not.toBe("");

  if (!magicLink) throw new Error("Inbucket did not return a magic link");
  await page.goto(magicLink);
  await expect(page).toHaveURL("http://localhost:3000/");
  await expect(page.getByText(`Logged in as ${email}`)).toBeVisible();
});
