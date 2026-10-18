import type { ReviewCase, ReviewStatus, Value } from "./types";

export const statusLabels: Record<ReviewStatus, string> = {
  needs_review: "Needs review",
  needs_attention: "Needs attention",
  reviewed: "Reviewed",
  dismissed: "Dismissed",
};
export const text = (value: Value | undefined) =>
  value === null || value === undefined || value === ""
    ? "\u2014"
    : String(value);
export const date = (
  value: Value | undefined,
  options?: Intl.DateTimeFormatOptions,
) => {
  if (!value) return "\u2014";
  const parsed = new Date(String(value));
  return Number.isNaN(parsed.getTime())
    ? String(value)
    : parsed.toLocaleDateString(
        "en-GB",
        options ?? { day: "2-digit", month: "short", year: "numeric" },
      );
};
export const money = (
  amount: Value | undefined,
  currency: Value | undefined,
) => {
  if (amount === null || amount === undefined || amount === "") return "\u2014";
  if (!Number.isFinite(Number(amount))) return String(amount);
  try {
    return new Intl.NumberFormat("en-GB", {
      style: "currency",
      currency: String(currency || "GBP"),
      currencyDisplay: "narrowSymbol",
    }).format(Number(amount));
  } catch {
    return `${Number(amount).toFixed(2)} ${text(currency)}`;
  }
};
export function caseTitle(item: ReviewCase) {
  return item.findings[0]?.finding ?? "Unusual payment pattern";
}
export const humanField = (field: string) =>
  ({
    transaction_id: "Transaction ID",
    transaction_date: "Payment date",
    account_id: "Account",
    amount: "Payment amount",
    counterparty: "Payee",
    invoice_reference: "Invoice reference",
    invoice_amount: "Invoice amount",
    vendor_id: "Vendor ID",
    source_row_number: "Source row",
  })[field] ?? field.replaceAll("_", " ").replace(/^./, (c) => c.toUpperCase());

export function exportCsv(
  rows: ReviewCase[],
  decisions: Record<string, { status: ReviewStatus; note: string }>,
) {
  const fields = [
    "transaction_id",
    "counterparty",
    "amount",
    "currency",
    "invoice_reference",
    "source_row",
    "audit_priority",
    "audit_score",
    "model_score",
    "model_flagged",
    "finding",
    "evidence",
    "review_status",
    "review_note",
  ];
  const escape = (value: unknown) => {
    let output = value === null || value === undefined ? "" : String(value);
    if (/^[\s]*[=+@-]/.test(output)) output = `'${output}`;
    return `"${output.replaceAll('"', '""')}"`;
  };
  const contents = [
    fields,
    ...rows.map((item) => [
      item.transaction.transaction_id,
      item.transaction.counterparty,
      item.transaction.amount,
      item.transaction.currency,
      item.transaction.invoice_reference,
      item.row_number,
      item.findings.length ? item.audit_band : "",
      item.findings.length ? item.audit_score : "",
      item.model.decision === null ? "" : item.model.score,
      item.model.flagged,
      item.findings.map((f) => f.finding).join("; ") ||
        "Unusual payment pattern",
      item.findings.map((f) => f.evidence).join("; ") || item.model.context,
      statusLabels[decisions[item.id]?.status ?? "needs_review"],
      decisions[item.id]?.note ?? "",
    ]),
  ]
    .map((row) => row.map(escape).join(","))
    .join("\r\n");
  const url = URL.createObjectURL(
    new Blob(["\ufeff", contents], { type: "text/csv;charset=utf-8;" }),
  );
  const link = document.createElement("a");
  link.href = url;
  link.download = "transaction-review.csv";
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
