import { useEffect, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ArrowUpRight, Check, Search, Sparkles } from "lucide-react";
import { date, money, statusLabels, text } from "./format";
import type { Decision, ReviewCase } from "./types";

export function Priority({ item }: { item: ReviewCase }) {
  return item.findings.length ? (
    <span className={`case-priority ${item.audit_band}`}>
      <i />
      {item.audit_band}
    </span>
  ) : (
    <span className="case-priority model">
      <Sparkles size={12} />
      Model only
    </span>
  );
}

export function Queue({
  rows,
  decisions,
  selected,
  onSelect,
  compact,
}: {
  rows: ReviewCase[];
  decisions: Record<string, Decision>;
  selected: string | null;
  onSelect: (id: string) => void;
  compact: boolean;
}) {
  const viewport = useRef<HTMLDivElement>(null);
  const virtual = useVirtualizer({
    count: rows.length,
    getScrollElement: () => viewport.current,
    estimateSize: () => (compact ? 136 : 72),
    overscan: 6,
    getItemKey: (index) => rows[index].id,
  });
  useEffect(() => {
    virtual.measure();
    const index = rows.findIndex((row) => row.id === selected);
    if (index >= 0) virtual.scrollToIndex(index, { align: "auto" });
  }, [compact, selected]);
  return (
    <section
      className={`case-queue ${compact ? "compact" : ""}`}
      aria-label="Review cases"
    >
      <div className="case-queue-heading">
        <h2>{compact ? "Cases" : "Review queue"}</h2>
        <span>{rows.length.toLocaleString()} cases</span>
      </div>
      {!compact && (
        <div className="queue-columns" aria-hidden="true">
          <span>Priority</span>
          <span>Payee / payment</span>
          <span>Reason for review</span>
          <span>Invoice / date</span>
          <span>Payments</span>
          <span>Value under review</span>
          <span>Status</span>
          <span />
        </div>
      )}
      <div
        ref={viewport}
        className="case-queue-scroll"
        tabIndex={0}
        aria-label="Scrollable case queue"
      >
        {rows.length ? (
          <div style={{ height: virtual.getTotalSize(), position: "relative" }}>
            {virtual.getVirtualItems().map((entry) => {
              const item = rows[entry.index];
              const status = decisions[item.id]?.status ?? "needs_review";
              const dates = item.members
                .map((member) => member.transaction.transaction_date)
                .filter(Boolean)
                .map(String)
                .sort();
              return (
                <button
                  key={item.id}
                  id={`open-${item.id}`}
                  className={`case-queue-row ${selected === item.id ? "selected" : ""}`}
                  style={{
                    position: "absolute",
                    top: 0,
                    left: 0,
                    width: "100%",
                    height: entry.size,
                    transform: `translateY(${entry.start}px)`,
                  }}
                  aria-label={`Investigate ${item.title} for ${text(item.transaction.counterparty)} ${text(item.transaction.invoice_reference)}, ${item.members.length} payments`}
                  aria-current={selected === item.id ? "true" : undefined}
                  onClick={() => onSelect(item.id)}
                >
                  <Priority item={item} />
                  <span className="case-row-title">
                    <strong>{text(item.transaction.counterparty)}</strong>
                    <span className="compact-case-reason">{item.title}</span>
                  </span>
                  <span className="case-row-reason">{item.title}</span>
                  <span className="case-row-context">
                    <span className="mono">
                      {item.transaction.invoice_reference
                        ? text(item.transaction.invoice_reference)
                        : item.members.length
                          ? text(item.members[0].transaction.transaction_id) +
                            (item.members.length > 1
                              ? ` +${item.members.length - 1}`
                              : "")
                          : "File-level issue"}
                    </span>
                    <small>
                      {dates.length ? date(dates[0]) : "No usable payment date"}
                      {dates.length > 1 && dates[dates.length - 1] !== dates[0]
                        ? ` to ${date(dates[dates.length - 1])}`
                        : ""}
                    </small>
                  </span>
                  <span className="case-payment-count">
                    {item.members.length}{" "}
                    {item.members.length === 1 ? "payment" : "payments"}
                  </span>
                  <span className="case-row-money">
                    {item.totals.length === 1
                      ? money(item.totals[0].amount, item.totals[0].currency)
                      : item.totals.length
                        ? "Multiple currencies"
                        : "File-level issue"}
                  </span>
                  <span className="case-row-status">
                    {["reviewed", "dismissed"].includes(status) && (
                      <Check size={12} />
                    )}
                    {statusLabels[status]}
                  </span>
                  <ArrowUpRight className="case-row-arrow" size={15} />
                </button>
              );
            })}
          </div>
        ) : (
          <div className="case-empty">
            <Search size={24} />
            <h3>No matching cases</h3>
            <p>Try a different search or clear the filters.</p>
          </div>
        )}
      </div>
    </section>
  );
}
