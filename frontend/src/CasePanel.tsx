import { useEffect, useRef, useState } from "react";
import {
  ArrowLeft,
  ArrowRight,
  Check,
  ChevronDown,
  Expand,
  FileText,
  Minimize,
  Sparkles,
  Undo2,
} from "lucide-react";
import { date, humanField, money, statusLabels, text } from "./format";
import { Priority } from "./Queue";
import type {
  AuditRun,
  Decision,
  PaymentRecord,
  ReviewCase,
  ReviewStatus,
} from "./types";

function PaymentTable({
  payments,
  context = false,
}: {
  payments: PaymentRecord[];
  context?: boolean;
}) {
  const [expanded, setExpanded] = useState<string | null>(null);
  return (
    <div className="case-payment-table">
      <table
        aria-label={context ? "Context payments" : "Payments in this case"}
      >
        <thead>
          <tr>
            <th>Transaction / source row</th>
            <th>Date</th>
            <th>Invoice reference</th>
            <th className="align-right">Payment</th>
            <th>Evidence</th>
          </tr>
        </thead>
        <tbody>
          {payments.map((payment) => (
            <PaymentRows
              key={payment.id}
              payment={payment}
              expanded={expanded === payment.id}
              onToggle={() =>
                setExpanded(expanded === payment.id ? null : payment.id)
              }
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function PaymentRows({
  payment,
  expanded,
  onToggle,
}: {
  payment: PaymentRecord;
  expanded: boolean;
  onToggle: () => void;
}) {
  const transaction = payment.transaction;
  return (
    <>
      <tr>
        <th scope="row">
          <span className="mono">{text(transaction.transaction_id)}</span>
          <small>Source row {payment.row_number}</small>
        </th>
        <td>{date(transaction.transaction_date)}</td>
        <td className="mono">{text(transaction.invoice_reference)}</td>
        <td className="align-right">
          {money(transaction.amount, transaction.currency)}
          <small>{text(transaction.currency)}</small>
        </td>
        <td>
          <button
            className="case-text-button"
            onClick={onToggle}
            aria-expanded={expanded}
            aria-controls={`source-${payment.id}`}
          >
            {expanded ? "Hide" : "View"} source
            <ChevronDown size={13} />
          </button>
        </td>
      </tr>
      {expanded && (
        <tr>
          <td colSpan={5}>
            <div id={`source-${payment.id}`} className="member-source">
              <h4>Source row {payment.row_number}</h4>
              <dl>
                {Object.entries(transaction).map(([field, value]) => (
                  <div key={field}>
                    <dt>{humanField(field)}</dt>
                    <dd>
                      {field.includes("amount")
                        ? money(value, transaction.currency)
                        : text(value)}
                    </dd>
                  </div>
                ))}
              </dl>
              <p>
                Original audit score: {payment.audit_score} / 100.{" "}
                {payment.audit_drivers}
              </p>
              <p>
                Model score:{" "}
                {payment.model.decision === null
                  ? "Not available"
                  : `${payment.model.score} / 100`}
                . {payment.model.context}
              </p>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

export default function CasePanel({
  item,
  run,
  decision,
  draft,
  onDraft,
  onNote,
  onDecision,
  onClose,
  onNavigate,
  position,
  count,
  outside,
  focusMode,
  onFocusMode,
  canUndo,
  onUndo,
  onOpenCase,
}: {
  item: ReviewCase;
  run: AuditRun;
  decision?: Decision;
  draft: string;
  onDraft: (value: string) => void;
  onNote: () => void;
  onDecision: (value: ReviewStatus) => void;
  onClose: () => void;
  onNavigate: (value: "previous" | "next") => void;
  position: number;
  count: number;
  outside: boolean;
  focusMode: boolean;
  onFocusMode: () => void;
  canUndo: boolean;
  onUndo: () => void;
  onOpenCase: (id: string) => void;
}) {
  const heading = useRef<HTMLHeadingElement>(null);
  const scroll = useRef<HTMLDivElement>(null);
  useEffect(() => {
    scroll.current?.scrollTo(0, 0);
    heading.current?.focus({ preventScroll: true });
  }, [item.id]);
  const summary = item.invoice_summary;
  const types = new Set(item.findings.map((f) => f.technical_issue_type));
  const split = types.has("possible_split_payment");
  const excess = summary && Number(summary.excess) > 0;
  const modelMembers = item.members.filter((member) => member.model.flagged);
  const status = decision?.status ?? "needs_review";
  const dates = item.members
    .map((m) => m.transaction.transaction_date)
    .filter(Boolean)
    .map(String)
    .sort();
  return (
    <section className="investigation" aria-label="Case investigation">
      <header className="investigation-toolbar">
        <button className="case-text-button" onClick={onClose}>
          <ArrowLeft size={15} />
          Back to cases
        </button>
        <label className="toolbar-review-decision">
          <span>Review decision</span>
          <select
            aria-label="Review decision"
            value={status}
            onChange={(event) => onDecision(event.target.value as ReviewStatus)}
          >
            {Object.entries(statusLabels).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <div className="investigation-navigation">
          <button
            className="icon-button"
            aria-label="Previous case"
            title="Previous case"
            disabled={position <= 0}
            onClick={() => onNavigate("previous")}
          >
            <ArrowLeft size={16} />
          </button>
          <span>
            {position < 0 ? "Outside results" : `${position + 1} of ${count}`}
          </span>
          <button
            className="icon-button"
            aria-label="Next case"
            title="Next case"
            disabled={position < 0 || position >= count - 1}
            onClick={() => onNavigate("next")}
          >
            <ArrowRight size={16} />
          </button>
          <button
            className="icon-button focus-case"
            aria-label={
              focusMode ? "Show case navigator" : "Focus on this case"
            }
            title={focusMode ? "Show case navigator" : "Focus on this case"}
            onClick={onFocusMode}
          >
            {focusMode ? <Minimize size={16} /> : <Expand size={16} />}
          </button>
        </div>
      </header>
      {outside && (
        <div className="case-context-message" role="status">
          This case no longer matches your filters. Your evidence stays open
          until you choose another case.
        </div>
      )}
      <div className="investigation-scroll" ref={scroll}>
        <div className="investigation-heading">
          <div className="case-heading-meta">
            <Priority item={item} />
            <span>{item.members.length} payments</span>
            <span>{item.findings.length} audit findings</span>
          </div>
          <h2 ref={heading} tabIndex={-1}>
            {item.title}
          </h2>
          <p>
            {text(item.transaction.counterparty)}
            {item.transaction.invoice_reference
              ? ` / ${item.transaction.invoice_reference}`
              : ""}
          </p>
          {dates.length > 0 && (
            <small>
              {date(dates[0])}
              {dates[dates.length - 1] !== dates[0]
                ? ` to ${date(dates[dates.length - 1])}`
                : ""}
            </small>
          )}
        </div>
        <section className="investigation-section observed-section">
          <h3>What we observed</h3>
          {summary ? (
            <>
              <p className="observed-fact">
                {item.members.length} recorded payments total{" "}
                <strong>{money(summary.paid, summary.currency)}</strong> against
                a supplied invoice amount of{" "}
                <strong>{money(summary.invoice, summary.currency)}</strong>.
              </p>
              <div className="case-calculation">
                <div>
                  <span>Invoice amount</span>
                  <strong>{money(summary.invoice, summary.currency)}</strong>
                </div>
                <div>
                  <span>Total paid</span>
                  <strong>{money(summary.paid, summary.currency)}</strong>
                </div>
                <div
                  className={
                    excess ? "calculation-excess" : "calculation-balanced"
                  }
                >
                  <span>
                    {excess
                      ? "Potential excess"
                      : Number(summary.remaining) > 0
                        ? "Remaining amount"
                        : "Difference"}
                  </span>
                  <strong>
                    {money(
                      excess ? summary.excess : summary.remaining,
                      summary.currency,
                    )}
                  </strong>
                </div>
              </div>
            </>
          ) : (
            <>
              <p className="observed-fact">
                {item.findings.length
                  ? item.findings[0].evidence
                  : "The anomaly model identified a payment pattern that differs from other transactions in this file. No audit rule established an error."}
              </p>
              {item.totals.map((total) => (
                <p className="case-value" key={total.currency}>
                  Recorded payment value{" "}
                  <strong>{money(total.amount, total.currency)}</strong>{" "}
                  <span>{total.currency}</span>
                </p>
              ))}
              {item.members.length > 1 &&
                types.has("possible_duplicate_payment") && (
                  <p className="case-muted">
                    A reliable invoice comparison is unavailable for this group.
                    Payment value is not an estimate of loss.
                  </p>
                )}
            </>
          )}
          {types.has("large_amount") && (
            <p className="case-muted">
              Large-payment comparison: at or above{" "}
              {run.large_payment_limit.toLocaleString("en-GB")} in the payment's
              currency. This is the engine's review limit, not a verified
              company approval policy.
            </p>
          )}
        </section>
        <section className="investigation-section">
          <div className="section-heading">
            <h3>Payments in this case</h3>
            <span>{item.members.length} source rows</span>
          </div>
          {item.members.length ? (
            <PaymentTable key={item.id} payments={item.members} />
          ) : (
            <p>
              This finding concerns the file rather than an identifiable payment
              row.
            </p>
          )}
          {item.unresolved_links.length > 0 && (
            <div className="case-warning">
              <strong>Some linked evidence could not be included safely</strong>
              <p>
                {item.unresolved_links.join(", ")}. These references are
                unavailable, ambiguous or incompatible with this group. They
                were not used to combine payments or calculate potential excess.
              </p>
            </div>
          )}
        </section>
        <section className="investigation-section explanation-grid">
          <div>
            <h3>Why it may matter</h3>
            <p>
              {excess
                ? "Recorded settlement exceeds the supplied invoice amount. If the records are complete and authoritative, the difference may require correction."
                : split
                  ? "An invoice was settled in parts. That can be legitimate; the pattern alone does not establish an approval breach."
                  : (item.findings[0]?.why_it_matters ??
                    "Unusual patterns can be legitimate. The model signal is a prompt for review, not evidence of fraud.")}
            </p>
          </div>
          <div>
            <h3>What remains uncertain</h3>
            <p>
              {summary
                ? "Invoice amendments, credits, reversals or an approved payment schedule may explain these records. Supporting documents have not been independently checked. This is not a confirmed loss."
                : types.has("missing_invoice_reference")
                  ? "A blank reference does not prove that no invoice or approved exception exists."
                  : "Source documents and approvals have not been independently verified. A finding does not establish that a payment is invalid."}
            </p>
          </div>
        </section>
        <section className="investigation-section">
          <h3>What to check next</h3>
          <ul className="case-next-actions">
            {[...new Set(item.findings.map((f) => f.recommended_action))].map(
              (action) => (
                <li key={action}>{action}</li>
              ),
            )}
            {!item.findings.length && (
              <li>
                Compare the source payment with its supporting documents and the
                surrounding business context.
              </li>
            )}
          </ul>
        </section>
        {item.context_members.length > 0 && (
          <section className="investigation-section">
            <h3>Context payments</h3>
            <p className="case-muted">
              Supporting comparison rows, excluded from this case's payment
              totals.
            </p>
            <PaymentTable payments={item.context_members} context />
          </section>
        )}
        {item.related_case_ids.length > 0 && (
          <section className="investigation-section">
            <h3>Other cases involving these payments</h3>
            <p className="case-muted">
              Different review questions. Decisions and notes are kept
              separately.
            </p>
            {item.related_case_ids.map((id) => (
              <div key={id}>
                <button
                  className="case-text-button"
                  onClick={() => onOpenCase(id)}
                >
                  {run.review_cases.find((c) => c.id === id)?.title ?? id}
                  <ArrowRight size={13} />
                </button>
              </div>
            ))}
          </section>
        )}
        <section className="investigation-section">
          <details>
            <summary>
              <FileText size={15} />
              All original audit findings ({item.findings.length})
            </summary>
            {item.findings.map((finding) => (
              <article className="original-finding" key={finding.finding_id}>
                <h4>{finding.finding}</h4>
                <p>{finding.evidence}</p>
                <small>
                  Source row {finding.row_number ?? "not available"} /{" "}
                  {finding.finding_id}
                </small>
              </article>
            ))}
          </details>
          <details className="model-disclosure">
            <summary>
              <Sparkles size={15} />
              Supporting model evidence{" "}
              <span>
                {modelMembers.length
                  ? `${modelMembers.length} flagged payments`
                  : "No flagged payments"}
              </span>
            </summary>
            <p>
              Scores describe relative unusualness within this file, not fraud
              probability. Each payment keeps its own model score; there is no
              combined case model score.
            </p>
            {item.members.map((member) => (
              <div className="member-model" key={member.id}>
                <strong>{text(member.transaction.transaction_id)}</strong>
                <span>
                  {member.model.decision === null
                    ? "Not run"
                    : `${member.model.score} / 100`}
                </span>
                <p>{member.model.context}</p>
              </div>
            ))}
          </details>
        </section>
        <section className="investigation-section case-review-section">
          <h3>Review this case</h3>
          <p className="case-muted">
            Reviewed records your work; it does not confirm fraud or loss.
            Decisions and notes last for this session only.
          </p>
          <p>
            Current status: <strong>{statusLabels[status]}</strong>
          </p>
          <label className="case-note-label">
            Review note
            <textarea
              aria-label="Review note"
              rows={4}
              value={draft}
              onChange={(event) => onDraft(event.target.value)}
              placeholder="Record your evidence, explanation or follow-up question."
            />
          </label>
          <div className="case-note-actions">
            <span role="status">
              {draft !== (decision?.note ?? "")
                ? "Unsaved note - kept while you navigate"
                : decision?.note
                  ? "Note saved for this session"
                  : "Included in review exports when saved"}
            </span>
            <button
              className="button small"
              onClick={onNote}
              disabled={draft === (decision?.note ?? "")}
            >
              <Check size={14} />
              Save note
            </button>
          </div>
          {canUndo && (
            <button className="case-text-button" onClick={onUndo}>
              <Undo2 size={14} />
              Undo last review action
            </button>
          )}
        </section>
      </div>
    </section>
  );
}
