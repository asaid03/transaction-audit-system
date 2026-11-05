import { expect, test, type Page } from "@playwright/test";
import { readFile } from "node:fs/promises";
import path from "node:path";
import AxeBuilder from "@axe-core/playwright";

const csv = `transaction_id,transaction_date,account_id,counterparty,currency,amount,invoice_reference,invoice_amount,vendor_id
PAY-A,2025-06-01,AP,Supplier A,GBP,2500,INV-004582,2500,V1
PAY-B,2025-06-08,AP,Supplier A,GBP,2480,inv 4582,2500,V1
PAY-C,2025-06-09,AP,Supplier B,GBP,12000,INV-2,12000,V2
PAY-D,2025-06-10,AP,Supplier C,USD,7000,,7000,V3`;

async function uploadCaseFile(page: Page, contents = csv, rowCount = 4) {
  await page.getByRole("button", { name: "New review", exact: true }).click();
  await page.getByLabel("Transaction file", { exact: true }).setInputFiles({
    name: "case-fixture.csv",
    mimeType: "text/csv",
    buffer: Buffer.from(contents),
  });
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText(`${rowCount} source rows`)).toBeVisible();
  await dialog.getByRole("button", { name: "Start review" }).click();
  await expect(dialog).not.toBeVisible();
  await expect(
    page.getByText("case-fixture.csv", { exact: true }),
  ).toBeVisible();
}

test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await expect(
    page.getByRole("region", { name: "Review cases", exact: true }),
  ).toBeVisible({ timeout: 20000 });
});

test("desktop queue balances columns and uses readable compact rows", async ({
  page,
}) => {
  for (const width of [1280, 1920]) {
    await page.setViewportSize({ width, height: 1000 });
    const row = page.locator(".case-queue-row").first();
    await expect(row).toBeVisible();
    await expect(row).toHaveCSS("height", "72px");
    await expect(row.locator(".case-row-title strong")).toHaveCSS(
      "font-size",
      "14px",
    );
    await expect(row.locator(".case-row-reason")).toBeVisible();
    await expect(row.locator(".case-row-context")).toBeVisible();
    expect(
      (await row.locator(".case-row-title").boundingBox())!.width,
    ).toBeLessThan(450);
    expect(
      (await page.locator(".case-search").boundingBox())!.height,
    ).toBeGreaterThanOrEqual(40);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: `test-results/queue-balanced-${width}.png`,
      fullPage: true,
    });
  }
});

test("real engine case grouping preserves every original finding and payment output", async ({
  page,
  request,
}) => {
  const response = await request.get("/api/demo");
  const run = await response.json();
  expect(run.row_count).toBe(200);
  expect(run.review_cases.length).toBeLessThan(run.cases.length);
  const findingIds = new Set(
    run.review_cases.flatMap((c: any) =>
      c.findings.map((f: any) => f.finding_id),
    ),
  );
  const originals = run.transactions.flatMap((p: any) =>
    p.findings.map((f: any) => f.finding_id),
  );
  expect(findingIds.size).toBe(originals.length);
  await page.screenshot({
    path: "test-results/cases-overview.png",
    fullPage: true,
  });
  await page
    .getByRole("button", { name: /^Investigate / })
    .first()
    .click();
  await expect(
    page.getByRole("region", { name: "Case investigation" }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "What we observed" }),
  ).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
});

test("one invoice case opens both payments, exact calculation and original evidence", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await uploadCaseFile(page);
  await page.getByLabel("Search cases").fill("PAY-B");
  await expect(page.getByRole("button", { name: /^Investigate / })).toHaveCount(
    1,
  );
  await page.getByRole("button", { name: /^Investigate / }).click();
  const detail = page.getByRole("region", { name: "Case investigation" });
  await expect(
    detail.getByRole("heading", {
      name: "Possible invoice overpayment",
      exact: true,
    }),
  ).toBeVisible();
  await expect(detail.locator(".case-calculation")).toContainText("£2,480.00");
  await expect(
    detail
      .getByRole("table", { name: "Payments in this case" })
      .getByRole("row"),
  ).toHaveCount(3);
  await expect(
    detail
      .getByRole("table", { name: "Payments in this case" })
      .getByText("PAY-A", { exact: true }),
  ).toBeVisible();
  await expect(
    detail
      .getByRole("table", { name: "Payments in this case" })
      .getByText("PAY-B", { exact: true }),
  ).toBeVisible();
  await detail
    .getByRole("button", { name: "View imported row" })
    .first()
    .click();
  await expect(detail.locator(".member-source")).toContainText("Source row 2");
  await detail.getByText("All original audit findings (2)").click();
  await expect(detail.locator(".original-finding")).toHaveCount(2);
  await page.screenshot({
    path: "test-results/case-investigation.png",
    fullPage: true,
  });
  await detail.getByRole("button", { name: "Back to cases" }).click();
  await expect(page.getByLabel("Search cases")).toHaveValue("PAY-B");
  await expect(
    page.getByRole("button", { name: /^Investigate / }),
  ).toBeFocused();
  expect(errors).toEqual([]);
});

test("note drafts survive navigation; review and undo do not erase evidence", async ({
  page,
}) => {
  await uploadCaseFile(page);
  await page
    .getByRole("button", { name: /^Investigate / })
    .first()
    .click();
  const detail = page.getByRole("region", { name: "Case investigation" });
  await detail.getByLabel("Review note").fill("Request amended invoice.");
  await detail.getByRole("button", { name: "Next case", exact: true }).click();
  await expect(detail.getByLabel("Review note")).toHaveValue("");
  await detail
    .getByRole("button", { name: "Previous case", exact: true })
    .click();
  await expect(detail.getByLabel("Review note")).toHaveValue(
    "Request amended invoice.",
  );
  await detail.getByRole("button", { name: "Save note", exact: true }).click();
  await detail.getByLabel("Review decision").selectOption("reviewed");
  await expect(
    detail.getByText(/no longer matches your filters/),
  ).toBeVisible();
  await expect(
    detail.getByRole("heading", {
      name: "Possible invoice overpayment",
      exact: true,
    }),
  ).toBeVisible();
  await detail.getByRole("button", { name: "Undo last review action" }).click();
  await expect(detail.getByLabel("Review decision")).toHaveValue(
    "needs_review",
  );
  await expect(detail.getByLabel("Review note")).toHaveValue(
    "Request amended invoice.",
  );
});

test("filters select complete cases; export includes members, source dates and case identity", async ({
  page,
}) => {
  await uploadCaseFile(page);
  await page
    .getByLabel("Finding filter")
    .selectOption("possible_duplicate_payment");
  await expect(page.getByRole("button", { name: /^Investigate / })).toHaveCount(
    1,
  );
  await page.getByText("Export review", { exact: true }).click();
  const event = page.waitForEvent("download");
  await page.getByRole("button", { name: "Member payments (.csv)" }).click();
  const download = await event;
  const contents = await readFile((await download.path())!, "utf8");
  expect(contents).toContain('"case_id"');
  expect(contents).toContain('"transaction_date"');
  expect(contents).toContain("PAY-A");
  expect(contents).toContain("PAY-B");
  expect(contents).not.toContain("PAY-C");
  expect(contents.split("\r\n")).toHaveLength(3);
  const summaryEvent = page.waitForEvent("download");
  await page.getByRole("button", { name: "Case summaries (.csv)" }).click();
  const summaryFile = await summaryEvent;
  const summaryContents = await readFile((await summaryFile.path())!, "utf8");
  expect(summaryContents).toContain('"case_potential_excess"');
  expect(summaryContents).toContain('"2480"');
  expect(summaryContents.split("\r\n")).toHaveLength(2);
  await page.getByRole("button", { name: "Reset", exact: true }).click();
  await page.getByLabel("Currency filter").selectOption("USD");
  await expect(page.getByRole("button", { name: /^Investigate / })).toHaveCount(
    1,
  );
  await page.getByLabel("Search cases").fill("DOES-NOT-EXIST");
  await expect(
    page.getByRole("heading", { name: "No matching cases" }),
  ).toBeVisible();
});

test("focus mode, filtered navigation, keyboard opening and source context", async ({
  page,
}) => {
  await uploadCaseFile(page);
  const first = page.getByRole("button", { name: /^Investigate / }).first();
  await first.focus();
  await page.keyboard.press("Enter");
  const detail = page.getByRole("region", { name: "Case investigation" });
  await expect(
    detail.getByRole("heading", {
      name: "Possible invoice overpayment",
      exact: true,
    }),
  ).toBeFocused();
  await detail.getByRole("button", { name: "Focus on this case" }).click();
  await expect(page.locator(".review-queue-pane")).not.toBeVisible();
  await detail.getByRole("button", { name: "Show case navigator" }).click();
  await expect(page.locator(".review-queue-pane")).toBeVisible();
  await page.getByLabel("Search cases").fill("PAY-C");
  await expect(
    detail.getByText(/no longer matches your filters/),
  ).toBeVisible();
  await expect(
    detail.getByRole("button", { name: "Next case" }),
  ).toBeDisabled();
  await detail.getByRole("button", { name: "Back to cases" }).click();
  await page.getByRole("button", { name: /^Investigate / }).click();
  await expect(
    detail.getByText(/Large-payment comparison: at or above 10,000/),
  ).toBeVisible();
});

test("manual upload mapping remains available", async ({ page }) => {
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
  await expect(
    dialog
      .getByLabel("Currency")
      .getByRole("option", { name: "is_anomaly", exact: true }),
  ).toHaveCount(0);
  await dialog.getByLabel("Show all source columns").click();
  await expect(
    dialog
      .getByLabel("Currency")
      .getByRole("option", { name: "is_anomaly", exact: true }),
  ).toHaveCount(1);
  await dialog.getByLabel("Payee", { exact: true }).selectOption("description");
  await dialog
    .getByText("Invoice and optional columns", { exact: true })
    .click();
  await dialog.getByLabel("Description", { exact: true }).selectOption("");
  await dialog.getByRole("button", { name: "Start review" }).click();
  await expect(dialog).not.toBeVisible();
  await expect(page.getByText("Uploaded file", { exact: true })).toBeVisible();
});

test("reconciled instalments are low priority and missing currency is not invented", async ({
  page,
}) => {
  const contents =
    "transaction_id,transaction_date,account_id,counterparty,currency,amount,invoice_reference,invoice_amount\n" +
    "INST-A,2025-06-01,AP,Supplier A,GBP,1000,INV-1,2500\n" +
    "INST-B,2025-06-08,AP,Supplier A,GBP,1500,INV-1,2500\n" +
    "NO-CCY,2025-06-10,AP,Supplier B,,700,INV-2,700";
  await uploadCaseFile(page, contents, 3);
  await page.getByLabel("Search cases").fill("INST-B");
  await page.getByRole("button", { name: /^Investigate / }).click();
  const detail = page.getByRole("region", { name: "Case investigation" });
  await expect(detail.locator(".case-priority")).toContainText("low");
  await expect(
    detail.getByRole("heading", {
      name: "Invoice settled in instalments",
      exact: true,
    }),
  ).toBeVisible();
  await page.screenshot({
    path: "test-results/case-instalments-low.png",
    fullPage: true,
  });
  await detail.getByRole("button", { name: "Back to cases" }).click();
  await page.getByLabel("Search cases").fill("NO-CCY");
  await page.getByRole("button", { name: /^Investigate / }).click();
  await expect(
    detail.getByRole("table", { name: "Payments in this case" }),
  ).not.toContainText("£700");
  await expect(
    detail.getByRole("table", { name: "Payments in this case" }),
  ).toContainText("Currency not supplied");
});

test("large linked invoice keeps every payment instead of a first-six preview", async ({
  page,
}) => {
  const contents =
    "transaction_id,transaction_date,account_id,counterparty,currency,amount,invoice_reference,invoice_amount,vendor_id\n" +
    Array.from(
      { length: 100 },
      (_, i) => `PART-${i},2025-06-01,AP,Supplier A,GBP,1,INV-100,100,V1`,
    ).join("\n");
  await uploadCaseFile(page, contents, 100);
  await page.getByLabel("Search cases").fill("PART-99");
  await expect(page.getByRole("button", { name: /^Investigate / })).toHaveCount(
    1,
  );
  await page.getByRole("button", { name: /^Investigate / }).click();
  const table = page.getByRole("table", { name: "Payments in this case" });
  await expect(table.getByRole("row")).toHaveCount(101);
  await expect(table.getByText("PART-99", { exact: true })).toBeAttached();
  await expect(page.locator(".case-calculation")).toContainText("£100.00");
});

test("case headline and explanation follow the strongest finding", async ({
  page,
}) => {
  const contents =
    "transaction_id,transaction_date,account_id,counterparty,currency,amount,invoice_reference,invoice_amount\n" +
    "BAD-DATE,bad-date,AP,Supplier A,GBP,1000,INV-1,2500\n" +
    "OTHER,2025-06-08,AP,Supplier A,GBP,1500,INV-1,2500";
  await uploadCaseFile(page, contents, 2);
  await page.getByRole("button", { name: /^Investigate / }).click();
  const detail = page.getByRole("region", { name: "Case investigation" });
  await expect(
    detail.getByRole("heading", {
      name: "Transaction date could not be parsed",
      exact: true,
    }),
  ).toBeVisible();
  await expect(detail.locator(".case-priority")).toContainText("high");
  await expect(detail.locator(".explanation-grid")).toContainText(
    "correct audit period",
  );
  await expect(detail.locator(".case-heading-meta")).toContainText(
    "Data correction",
  );
});

test("synthetic 10k-case presentation stays virtualised and searchable", async ({
  page,
  request,
}) => {
  test.setTimeout(60000);
  const run = await (await request.get("/api/demo")).json();
  const template = run.review_cases.find((c: any) => c.members.length === 1);
  // A browser-only stress fixture, not a claim that the engine analysed these rows.
  run.filename = "Synthetic browser scale fixture";
  run.row_count = 10000;
  run.review_cases = Array.from({ length: 10000 }, (_, index) => {
    const member = {
      ...template.members[0],
      id: `load-row-${index}`,
      transaction: {
        ...template.members[0].transaction,
        transaction_id: `LOAD-${String(index).padStart(5, "0")}`,
      },
    };
    return {
      ...template,
      id: `load-case-${index}`,
      members: [member],
      transaction: member.transaction,
      related_case_ids: [],
    };
  });
  await page.route("**/api/demo", (route) => route.fulfill({ json: run }));
  await page.reload();
  await expect(
    page.getByRole("region", { name: "Review cases", exact: true }),
  ).toBeVisible();
  expect(
    await page.getByRole("button", { name: /^Investigate / }).count(),
  ).toBeLessThan(50);
  const timings: number[] = [];
  for (const id of ["09999", "01234", "00001", "05555", "09876"]) {
    const start = performance.now();
    await page.getByLabel("Search cases").fill(`LOAD-${id}`);
    await expect(
      page.getByRole("button", { name: /^Investigate / }),
    ).toHaveCount(1);
    timings.push(performance.now() - start);
  }
  console.log(
    "10k browser fixture search timings (ms)",
    timings.map((v) => Math.round(v)),
  );
  expect(Math.max(...timings)).toBeLessThan(1500);
});

test("accessibility and desktop layouts, dark appearance and narrow fallback", async ({
  page,
}) => {
  const scan = async () => {
    const results = await new AxeBuilder({ page })
      .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
      .analyze();
    expect(
      results.violations.map(({ id, nodes }) => ({
        id,
        targets: nodes.map((node) => node.target),
      })),
    ).toEqual([]);
  };
  await scan();
  await uploadCaseFile(page);
  await page
    .getByRole("button", { name: /^Investigate / })
    .first()
    .click();
  await scan();
  for (const [width, height] of [
    [1280, 800],
    [1440, 900],
    [1920, 1080],
  ]) {
    await page.setViewportSize({ width, height });
    await expect(
      page.getByRole("table", { name: "Payments in this case" }),
    ).toBeVisible();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: `test-results/case-${width}.png`,
      fullPage: true,
    });
  }
  await page.getByRole("button", { name: "Use dark appearance" }).click();
  await scan();
  await page.screenshot({ path: "test-results/case-dark.png", fullPage: true });
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(
    page.getByRole("button", { name: "Back to cases" }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
