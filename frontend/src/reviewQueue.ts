import type { Decision, ReviewCase, Scope } from "./types";

export interface QueueFilters {
  scope: Scope;
  status: string;
  band: string;
  issue: string;
  currency: string;
  search: string;
  sort: string;
  modelCutoff: number;
}

export const hasVisibleModelSignal = (item: ReviewCase, cutoff: number) =>
  item.members.some(
    (member) => member.model.flagged && member.model.score >= cutoff,
  );

const bandOrder = { high: 3, medium: 2, low: 1 };
const priority = (item: ReviewCase) =>
  item.findings.length
    ? bandOrder[item.audit_band] * 1000 + item.audit_score
    : -1;
const amount = (item: ReviewCase) =>
  item.totals.length === 1 ? Number(item.totals[0].amount ?? 0) : 0;
const firstDate = (item: ReviewCase) =>
  item.members
    .map((m) => String(m.transaction.transaction_date ?? ""))
    .sort()[0] ?? "";

export function selectReviewCases(
  candidates: ReviewCase[],
  decisions: Record<string, Decision>,
  filters: QueueFilters,
): ReviewCase[] {
  const query = filters.search.trim().toLowerCase();
  return candidates
    .filter((item) => {
      const status = decisions[item.id]?.status ?? "needs_review";
      const closed = status === "reviewed" || status === "dismissed";
      return (
        (filters.scope === "all" ||
          (filters.scope === "audit"
            ? item.findings.length
            : hasVisibleModelSignal(item, filters.modelCutoff))) &&
        (filters.status === "all" ||
          (filters.status === "open"
            ? !closed
            : filters.status === "closed"
              ? closed
              : status === filters.status)) &&
        (filters.band === "all" ||
          (item.findings.length > 0 && item.audit_band === filters.band)) &&
        (filters.issue === "all" ||
          item.findings.some(
            (f) => f.technical_issue_type === filters.issue,
          )) &&
        (filters.currency === "all" ||
          item.totals.some((t) => t.currency === filters.currency)) &&
        (!query ||
          item.title.toLowerCase().includes(query) ||
          item.members.some((m) =>
            Object.values(m.transaction).some(
              (value) =>
                value != null && String(value).toLowerCase().includes(query),
            ),
          ))
      );
    })
    .sort((a, b) => {
      const difference =
        filters.sort === "amount"
          ? String(a.totals[0]?.currency ?? "").localeCompare(
              b.totals[0]?.currency ?? "",
            ) || amount(b) - amount(a)
          : filters.sort === "payee"
            ? String(a.transaction.counterparty ?? "").localeCompare(
                String(b.transaction.counterparty ?? ""),
              )
            : filters.sort === "date"
              ? firstDate(b).localeCompare(firstDate(a))
              : priority(b) - priority(a) || b.model.score - a.model.score;
      return difference || a.id.localeCompare(b.id);
    });
}
