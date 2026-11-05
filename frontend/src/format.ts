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
  if (!currency || currency === "Unknown currency")
    return `${new Intl.NumberFormat("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Number(amount))} (Currency not supplied)`;
  try {
    return new Intl.NumberFormat("en-GB", {
      style: "currency",
      currency: String(currency),
      currencyDisplay: "narrowSymbol",
    }).format(Number(amount));
  } catch {
    return `${Number(amount).toFixed(2)} ${text(currency)}`;
  }
};
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
  summary = false,
) {
  const fields = [
    "case_id",
    "case_title",
    "review_category",
    "payment_count",
    "case_invoice_amount",
    "case_paid_total",
    "case_potential_excess",
    "case_remaining",
    "case_invoice_currency",
    "unresolved_links",
    "transaction_id",
    "transaction_date",
    "account_id",
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
    ...rows.flatMap((item) =>
      (summary ? [null] : item.members.length ? item.members : [null]).map(
        (member) => [
          item.id,
          item.title,
          item.category,
          item.members.length,
          summary ? item.invoice_summary?.invoice : "",
          summary ? item.invoice_summary?.paid : "",
          summary ? item.invoice_summary?.excess : "",
          summary ? item.invoice_summary?.remaining : "",
          summary ? item.invoice_summary?.currency : "",
          item.unresolved_links.join("; "),
          member?.transaction.transaction_id ??
            item.members
              .map((m) => text(m.transaction.transaction_id))
              .join("; "),
          member?.transaction.transaction_date ?? "",
          member?.transaction.account_id ?? "",
          member?.transaction.counterparty ?? item.transaction.counterparty,
          member
            ? member.transaction.amount
            : item.totals.length === 1
              ? item.totals[0].amount
              : "",
          member
            ? member.transaction.currency
            : item.totals.map((total) => total.currency).join("; "),
          member?.transaction.invoice_reference ??
            item.transaction.invoice_reference,
          member?.row_number ?? "",
          item.findings.length ? item.audit_band : "",
          item.findings.length ? item.audit_score : "",
          member?.model.decision == null ? "" : member.model.score,
          member ? member.model.flagged : "",
          item.findings.map((f) => f.finding).join("; ") ||
            "Unusual payment pattern",
          item.findings.map((f) => f.evidence).join("; ") || item.model.context,
          statusLabels[decisions[item.id]?.status ?? "needs_review"],
          decisions[item.id]?.note ?? "",
        ],
      ),
    ),
  ]
    .map((row) => row.map(escape).join(","))
    .join("\r\n");
  const url = URL.createObjectURL(
    new Blob(["\ufeff", contents], { type: "text/csv;charset=utf-8;" }),
  );
  const link = document.createElement("a");
  link.href = url;
  link.download = summary ? "case-review.csv" : "transaction-review.csv";
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
