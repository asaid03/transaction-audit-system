import { useEffect, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  ArrowUpRight,
  Check,
  Circle,
  Sparkles,
} from "lucide-react";
import { caseTitle, date, money, statusLabels, text } from "./format";
import type { Decision, ReviewCase, Scope } from "./types";

export type SortKey = "priority" | "date" | "amount" | "payee";
export function Priority({ item }: { item: ReviewCase }) {
  return item.findings.length ? (
    <span className={`priority ${item.audit_band}`}>
      <i aria-hidden="true" />
      <span>{item.audit_band}</span>
      <b>{item.audit_score}</b>
    </span>
  ) : (
    <span className="model-only">
      <Sparkles size={13} />
      Model only
    </span>
  );
}

export function Queue({
  rows,
  decisions,
  onSelect,
  selected,
  sort,
  direction,
  onSort,
  scope,
}: {
  rows: ReviewCase[];
  decisions: Record<string, Decision>;
  onSelect: (id: string) => void;
  selected: string | null;
  sort: SortKey;
  direction: number;
  onSort: (key: SortKey) => void;
  scope: Scope;
}) {
  const viewport = useRef<HTMLDivElement>(null);
  const [mobile, setMobile] = useState(window.innerWidth < 760);
  useEffect(() => {
    const query = window.matchMedia("(max-width: 759px)");
    const update = () => setMobile(query.matches);
    query.addEventListener("change", update);
    return () => query.removeEventListener("change", update);
  }, []);
  const virtual = useVirtualizer({
    count: rows.length,
    getScrollElement: () => viewport.current,
    estimateSize: () => 66,
    overscan: 10,
  });
  const items = virtual.getVirtualItems();
  const first = items[0]?.start ?? 0;
  const last = virtual.getTotalSize() - (items.at(-1)?.end ?? 0);
  const sortLabel = (key: SortKey, label: string) => (
    <button onClick={() => onSort(key)}>
      {label}
      {sort === key ? (
        direction === -1 ? (
          <ArrowDown size={12} />
        ) : (
          <ArrowUp size={12} />
        )
      ) : (
        <ArrowUpDown size={12} />
      )}
    </button>
  );

  return (
    <div className="table-shell">
      <div className="queue-viewport" ref={viewport}>
        <table className="queue-table" aria-label="Transactions needing review">
          <colgroup>
            <col className="col-priority" />
            <col className="col-payment" />
            {!mobile && (
              <>
                <col className="col-trigger" />
                <col className="col-date" />
              </>
            )}
            <col className="col-amount" />
            {!mobile && (
              <>
                <col className="col-status" />
                <col className="col-arrow" />
              </>
            )}
          </colgroup>
          <thead>
            <tr>
              <th
                scope="col"
                aria-sort={
                  sort === "priority"
                    ? direction === -1
                      ? "descending"
                      : "ascending"
                    : "none"
                }
              >
                {sortLabel(
                  "priority",
                  scope === "model" ? "Model score" : "Priority",
                )}
              </th>
              <th
                scope="col"
                aria-sort={
                  sort === "payee"
                    ? direction === -1
                      ? "descending"
                      : "ascending"
                    : "none"
                }
              >
                {sortLabel("payee", mobile ? "Payment" : "Payment / payee")}
              </th>
              {!mobile && (
                <>
                  <th scope="col">Reason for review</th>
                  <th
                    scope="col"
                    aria-sort={
                      sort === "date"
                        ? direction === -1
                          ? "descending"
                          : "ascending"
                        : "none"
                    }
                  >
                    {sortLabel("date", "Date")}
                  </th>
                </>
              )}
              <th
                className="align-right"
                scope="col"
                aria-sort={
                  sort === "amount"
                    ? direction === -1
                      ? "descending"
                      : "ascending"
                    : "none"
                }
              >
                {sortLabel("amount", "Amount")}
              </th>
              {!mobile && (
                <>
                  <th scope="col">Status</th>
                  <th aria-label="Open case" />
                </>
              )}
            </tr>
          </thead>
          <tbody>
            {first > 0 && (
              <tr aria-hidden="true">
                <td
                  colSpan={mobile ? 3 : 7}
                  style={{ height: first, padding: 0, border: 0 }}
                />
              </tr>
            )}
            {items.map((virtualRow) => {
              const item = rows[virtualRow.index];
              const status = decisions[item.id]?.status ?? "needs_review";
              const done = status === "reviewed" || status === "dismissed";
              return (
                <tr
                  key={item.id}
                  className={`${selected === item.id ? "selected" : ""} ${done ? "completed" : ""}`}
                  onClick={() => onSelect(item.id)}
                >
                  <td>
                    {scope === "model" ? (
                      <span className={`model-score ${item.model.band}`}>
                        <Sparkles size={13} />
                        <b>{item.model.score}</b>
                        <span>/100</span>
                      </span>
                    ) : (
                      <Priority item={item} />
                    )}
                  </td>
                  <td>
                    <button
                      className="payment-link"
                      title={text(item.transaction.counterparty)}
                      onClick={(event) => {
                        event.stopPropagation();
                        onSelect(item.id);
                      }}
                      aria-label={`Investigate ${text(item.transaction.transaction_id)}`}
                    >
                      <span>{text(item.transaction.counterparty)}</span>
                      <small className="mono">
                        {text(item.transaction.transaction_id)}
                      </small>
                      <small className="mobile-trigger">
                        {caseTitle(item)}
                      </small>
                    </button>
                  </td>
                  {!mobile && (
                    <>
                      <td>
                        <span className="trigger-title" title={caseTitle(item)}>
                          {scope === "model" && !item.findings.length
                            ? "Unusual payment pattern"
                            : caseTitle(item)}
                        </span>
                        <span className="trigger-sub">
                          {item.findings.length > 1
                            ? `${item.findings.length} audit findings`
                            : item.findings.length
                              ? "Audit check"
                              : "Model signal"}
                          {item.model.flagged && item.findings.length > 0
                            ? ` · Model ${item.model.score}`
                            : ""}
                        </span>
                      </td>
                      <td className="date-cell">
                        {date(item.transaction.transaction_date, {
                          day: "2-digit",
                          month: "short",
                        })}
                      </td>
                    </>
                  )}
                  <td className="amount-cell">
                    <strong
                      title={money(
                        item.transaction.amount,
                        item.transaction.currency,
                      )}
                    >
                      {money(
                        item.transaction.amount,
                        item.transaction.currency,
                      )}
                    </strong>
                    <small>{text(item.transaction.currency)}</small>
                  </td>
                  {!mobile && (
                    <>
                      <td>
                        <span className={`review-status ${status}`}>
                          {done ? <Check size={12} /> : <Circle size={9} />}{" "}
                          {statusLabels[status]}
                        </span>
                      </td>
                      <td className="row-arrow">
                        <ArrowUpRight size={16} />
                      </td>
                    </>
                  )}
                </tr>
              );
            })}
            {last > 0 && (
              <tr aria-hidden="true">
                <td
                  colSpan={mobile ? 3 : 7}
                  style={{ height: last, padding: 0, border: 0 }}
                />
              </tr>
            )}
          </tbody>
        </table>
        {!rows.length && (
          <div className="empty-results">
            <Check size={24} />
            <h3>No payments in this view</h3>
            <p>Change your filters to see other review cases.</p>
          </div>
        )}
      </div>
    </div>
  );
}
