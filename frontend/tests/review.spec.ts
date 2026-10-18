import { expect, test } from "@playwright/test";
import path from "node:path";
import AxeBuilder from "@axe-core/playwright";

test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await expect(
    page.getByRole("table", { name: "Transactions needing review" }),
  ).toBeVisible({ timeout: 20000 });
});

test("real engine results, filters, linked evidence, decisions, undo, export", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await expect(
    page.getByText("200 transactions", { exact: true }).first(),
  ).toBeVisible();
  await page.screenshot({
    path: "test-results/review-desktop.png",
    fullPage: true,
  });
  const firstCase = page.getByRole("button", { name: /^Investigate / }).first();
  const transaction = (await firstCase.getAttribute("aria-label"))!.replace(
    "Investigate ",
    "",
  );
  await firstCase.click();
  const dialog = page.getByRole("dialog");
  await expect(
    dialog.getByRole("heading", { name: "Why this needs review" }),
  ).toBeVisible();
  await expect(
    dialog.getByRole("heading", { name: "Linked payments" }),
  ).toBeVisible();
  await expect(
    dialog.getByRole("heading", { name: "Model signal", exact: true }),
  ).toBeVisible();
  await expect(dialog).toHaveCSS("transform", "none");
  await page.screenshot({ path: "test-results/review-case.png" });
  await dialog.getByLabel("Review decision").selectOption("reviewed");
  await page.getByRole("button", { name: "Undo", exact: true }).click();
  await expect(dialog.getByLabel("Review decision")).toHaveValue(
    "needs_review",
  );
  await dialog
    .getByLabel("Review note")
    .fill("Check invoice evidence with finance.");
  await dialog.getByRole("button", { name: "Save note" }).click();
  await dialog.getByRole("tab", { name: /^Activity/ }).click();
  await expect(dialog.getByText("Review note saved.")).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(dialog).not.toBeVisible();
  await expect(
    page.getByRole("button", {
      name: `Investigate ${transaction}`,
      exact: true,
    }),
  ).toBeFocused();
  await page.getByLabel("Search payments").fill(transaction);
  await expect(page.getByRole("button", { name: /^Investigate / })).toHaveCount(
    1,
  );
  const downloadEvent = page.waitForEvent("download");
  await page.getByRole("button", { name: "Export review" }).click();
  const download = await downloadEvent;
  expect(download.suggestedFilename()).toBe("transaction-review.csv");
  await page.getByRole("button", { name: "Reset", exact: true }).click();
  await page.getByLabel("Priority filter").selectOption("medium");
  await expect(page.locator(".queue-table .priority.high")).toHaveCount(0);
  expect(errors).toEqual([]);
});

test("appearance, live model cutoff, mobile sheet and keyboard", async ({
  page,
}) => {
  await page.getByRole("button", { name: "Use dark appearance" }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await page.screenshot({
    path: "test-results/review-dark.png",
    fullPage: true,
  });
  await page
    .getByRole("button", { name: "Review settings", exact: true })
    .click();
  const settings = page.getByRole("dialog");
  await settings.getByLabel("Minimum model score").fill("100");
  await expect(settings.locator("output")).toHaveText("100 / 100");
  await expect(settings.locator(".live-preview strong")).toHaveText("1");
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Use light appearance" }).click();
  await page.setViewportSize({ width: 390, height: 844 });
  const paymentCell = page
    .getByRole("table", { name: "Transactions needing review" })
    .locator("tbody tr")
    .first()
    .locator("td")
    .nth(1);
  expect((await paymentCell.boundingBox())!.width).toBeGreaterThan(110);
  await expect(
    page
      .getByRole("table", { name: "Transactions needing review" })
      .locator(".amount-cell")
      .first(),
  ).toBeVisible();
  await page.screenshot({
    path: "test-results/review-mobile.png",
    fullPage: true,
  });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page
    .getByRole("button", { name: /^Investigate / })
    .first()
    .click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCSS("transform", "none");
  await page.screenshot({ path: "test-results/review-mobile-case.png" });
  await page.keyboard.press("Tab");
  expect(
    await page.evaluate(
      () => !!document.activeElement?.closest('[role="dialog"]'),
    ),
  ).toBe(true);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).not.toBeVisible();
});

test("upload and manual mapping use the real analysis endpoint", async ({
  page,
}) => {
  await page.getByRole("button", { name: "New review" }).click();
  await page
    .getByLabel("Transaction file", { exact: true })
    .setInputFiles(
      path.resolve(
        "../sample_data/reduced_transactions_200_with_anomalies.csv",
      ),
    );
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("200 source rows")).toBeVisible();
  await dialog.getByLabel("Payee", { exact: true }).selectOption("description");
  await dialog
    .getByText("Invoice and optional columns", { exact: true })
    .click();
  await dialog.getByLabel("Description", { exact: true }).selectOption("");
  await dialog.getByRole("button", { name: "Start review" }).click();
  await expect(dialog).not.toBeVisible({ timeout: 20000 });
  await expect(
    page.getByRole("table", { name: "Transactions needing review" }),
  ).toBeVisible();
  await expect(page.getByText("Uploaded file", { exact: true })).toBeVisible();
});

test("reduced motion uses a usable panel and does not hide content", async ({
  page,
}) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page
    .getByRole("button", { name: /^Investigate / })
    .first()
    .click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Why this needs review" }),
  ).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).not.toBeVisible();
});

test("queue and case panel meet automated WCAG AA checks", async ({ page }) => {
  const queue = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
    .analyze();
  expect(
    queue.violations.map((violation) => ({
      id: violation.id,
      nodes: violation.nodes.map((node) => ({
        target: node.target,
        summary: node.failureSummary,
      })),
    })),
  ).toEqual([]);
  await page
    .getByRole("button", { name: /^Investigate / })
    .first()
    .click();
  await expect(page.getByRole("dialog")).toHaveCSS("transform", "none");
  const panel = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
    .analyze();
  expect(
    panel.violations.map((violation) => ({
      id: violation.id,
      nodes: violation.nodes.map((node) => ({
        target: node.target,
        summary: node.failureSummary,
      })),
    })),
  ).toEqual([]);
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Use dark appearance" }).click();
  const dark = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
    .analyze();
  expect(
    dark.violations.map(({ id, nodes }) => ({
      id,
      targets: nodes.map((node) => node.target),
    })),
  ).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  const mobile = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
    .analyze();
  expect(
    mobile.violations.map(({ id, nodes }) => ({
      id,
      targets: nodes.map((node) => node.target),
    })),
  ).toEqual([]);
});
