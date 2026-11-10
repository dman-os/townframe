import { test } from "@playwright/test";
// Scratch: does Chromium's scroll anchoring compensate a growth above the viewport when the
// scroll is driven by script each frame (vs by wheel)? Plain DOM, no Clay.
for (const abs of [false, true]) for (const drive of ["none", "script", "wheel"]) {
  test(`anchor ${drive} abs=${abs}`, async ({ page }) => {
    await page.setContent(`<div id=sc style="height:600px;overflow-y:auto;position:relative"><div style="position:relative"><div id=win style="${abs ? "position:absolute;left:0;right:0;top:0" : ""}"><div id=th style="height:100px;background:#fc8"></div>${Array.from({ length: 300 }, (_, i) => `<div class=row style="height:20px">row ${i}</div>`).join("")}</div>${abs ? '<div style="height:7000px"></div>' : ""}</div></div>`);
    await page.evaluate(() => { (document.getElementById("sc") as HTMLElement).scrollTop = 1000; });
    await page.waitForTimeout(100);
    if (drive === "script") await page.evaluate(() => { const sc = document.getElementById("sc")!; const t = (n: number) => { sc.scrollTop += 10; requestAnimationFrame(t); }; requestAnimationFrame(t); });
    if (drive === "wheel") { await page.mouse.move(100, 300); (async () => { for (let i = 0; i < 40; i++) { await page.mouse.wheel(0, 40); await page.waitForTimeout(16); } })(); }
    await page.waitForTimeout(300);
    const r = await page.evaluate(async () => {
      const sc = document.getElementById("sc")!;
      const row = document.querySelectorAll(".row")[60] as HTMLElement;
      const before = row.getBoundingClientRect().top - sc.getBoundingClientRect().top;
      const st0 = sc.scrollTop;
      document.getElementById("th")!.style.height = "180px";
      await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
      return { rowYBefore: before, rowYAfter: row.getBoundingClientRect().top - sc.getBoundingClientRect().top, st0, st1: sc.scrollTop };
    });
    console.log(`ANCHOR drive=${drive} abs=${abs} ${JSON.stringify(r)}`);
  });
}
