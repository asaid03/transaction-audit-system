import { useEffect, useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import {
  AnimatePresence,
  motion,
  useDragControls,
  useReducedMotion,
} from "motion/react";
import {
  ArrowLeft,
  ArrowRight,
  ArrowUpRight,
  Check,
  ChevronDown,
  FileText,
  History,
  ListChecks,
  MessageSquare,
  Sparkles,
  X,
} from "lucide-react";
import { date, humanField, money, statusLabels, text } from "./format";
import { Priority } from "./Queue";
import type { AuditRun, Decision, ReviewCase, ReviewStatus } from "./types";

export default function CasePanel({
  item,
  run,
  decision,
  open,
  onClose,
  onDecision,
  onNote,
  onNavigate,
  position,
  count,
  onUndo,
  canUndo,
}: {
  item: ReviewCase | null;
  run: AuditRun;
  decision: Decision | undefined;
  open: boolean;
  onClose: () => void;
  onDecision: (status: ReviewStatus) => void;
  onNote: (note: string) => void;
  onNavigate: (id: string) => void;
  position: number;
  count: number;
  onUndo: () => void;
  canUndo: boolean;
}) {
  const reduced = useReducedMotion();
  const dragControls = useDragControls();
  const [mobile, setMobile] = useState(window.innerWidth < 760);
  const [note, setNote] = useState("");
  const [section, setSection] = useState<"evidence" | "activity">("evidence");
  useEffect(() => {
    const query = window.matchMedia("(max-width: 759px)");
    const update = () => setMobile(query.matches);
    query.addEventListener("change", update);
    return () => query.removeEventListener("change", update);
  }, []);
  useEffect(() => {
    setNote(decision?.note ?? "");
    setSection("evidence");
  }, [item?.id]);
  useEffect(() => {
    setNote(decision?.note ?? "");
  }, [decision?.note]);
  if (!item) return null;
  const transaction = item.transaction;
  const status = decision?.status ?? "needs_review";
  const explicitIds = new Set(
    item.findings
      .filter((f) => !f.related_dataset || f.related_dataset === "transactions")
      .flatMap((f) =>
        (f.related_record_id ?? "").split(",").map((id) => id.trim()),
      ),
  );
  const explicitRelated = run.transactions.filter(
    (record) =>
      record.id !== item.id &&
      explicitIds.has(String(record.transaction.transaction_id)),
  );
  const contextRelated = explicitRelated.length
    ? []
    : run.transactions
        .filter((record) => {
          if (
            record.id === item.id ||
            !transaction.counterparty ||
            record.transaction.counterparty !== transaction.counterparty ||
            record.transaction.currency !== transaction.currency
          )
            return false;
          const sameAmount =
            transaction.amount !== null &&
            Number(record.transaction.amount) === Number(transaction.amount);
          const days =
            Math.abs(
              new Date(String(record.transaction.transaction_date)).getTime() -
                new Date(String(transaction.transaction_date)).getTime(),
            ) / 86400000;
          return sameAmount || days <= 3;
        })
        .slice(0, 6);
  const related = explicitRelated.length ? explicitRelated : contextRelated;
  const findPrevious = () => onNavigate("previous");
  const findNext = () => onNavigate("next");
  const offscreen = reduced
    ? { opacity: 0 }
    : mobile
      ? { y: "100%", opacity: 1 }
      : { x: "100%", opacity: 1 };

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(value) => {
        if (!value) onClose();
      }}
    >
      <AnimatePresence>
        {open && (
          <Dialog.Portal forceMount>
            <Dialog.Overlay asChild>
              <motion.div
                className="panel-backdrop"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                transition={{ duration: reduced ? 0.12 : 0.2 }}
              />
            </Dialog.Overlay>
            <Dialog.Content
              asChild
              aria-describedby="case-description"
              onCloseAutoFocus={(event) => {
                event.preventDefault();
                const trigger = Array.from(
                  document.querySelectorAll<HTMLButtonElement>(
                    "button[aria-label]",
                  ),
                ).find(
                  (button) =>
                    button.getAttribute("aria-label") ===
                    `Investigate ${String(transaction.transaction_id)}`,
                );
                (
                  trigger ??
                  document.querySelector<HTMLInputElement>(
                    'input[aria-label="Search payments"]',
                  )
                )?.focus();
              }}
            >
              <motion.aside
                className="case-panel"
                initial={offscreen}
                animate={{ x: 0, y: 0, opacity: 1 }}
                exit={offscreen}
                transition={
                  reduced
                    ? { duration: 0.12 }
                    : { type: "spring", stiffness: 370, damping: 38, mass: 0.9 }
                }
                drag={mobile && !reduced ? "y" : false}
                dragControls={dragControls}
                dragListener={false}
                dragConstraints={{ top: 0, bottom: 0 }}
                dragElastic={{ top: 0.08, bottom: 0.8 }}
                onDragEnd={(_, info) => {
                  if (info.offset.y + info.velocity.y * 0.15 > 140) onClose();
                }}
              >
                <div
                  className="panel-drag-zone"
                  aria-hidden="true"
                  onPointerDown={(event) => {
                    if (mobile && !reduced) dragControls.start(event);
                  }}
                >
                  <div className="panel-drag-handle" />
                </div>
                <div className="panel-topbar">
                  <span>
                    <span className="mono">
                      {text(transaction.transaction_id)}
                    </span>
                    <span className="panel-row">
                      Source row {item.row_number}
                    </span>
                  </span>
                  <div className="panel-nav">
                    <button
                      className="icon-button"
                      title="Previous case"
                      aria-label="Previous case"
                      onClick={findPrevious}
                      disabled={position <= 0}
                    >
                      <ArrowLeft size={16} />
                    </button>
                    <span className="mono">
                      {position >= 0 ? position + 1 : "—"} / {count}
                    </span>
                    <button
                      className="icon-button"
                      title="Next case"
                      aria-label="Next case"
                      onClick={findNext}
                      disabled={position < 0 || position >= count - 1}
                    >
                      <ArrowRight size={16} />
                    </button>
                    <Dialog.Close asChild>
                      <button
                        className="icon-button close-panel"
                        aria-label="Close case"
                        title="Close case"
                      >
                        <X size={18} />
                      </button>
                    </Dialog.Close>
                  </div>
                </div>
                <div className="panel-heading">
                  <div className="panel-kicker">
                    <Priority item={item} />
                    <span>
                      {item.findings.length
                        ? `${item.findings.length} audit ${item.findings.length === 1 ? "finding" : "findings"}`
                        : "Supporting model signal"}
                    </span>
                  </div>
                  <Dialog.Title>{text(transaction.counterparty)}</Dialog.Title>
                  <div className="case-amount">
                    {money(transaction.amount, transaction.currency)}{" "}
                    <span>{text(transaction.currency)}</span>
                  </div>
                  <Dialog.Description id="case-description">
                    {date(transaction.transaction_date)}
                    <span>·</span>Account{" "}
                    <span className="mono">{text(transaction.account_id)}</span>
                  </Dialog.Description>
                  <div className="decision-control">
                    <label htmlFor="case-status">Review decision</label>
                    <div className="select-wrap">
                      <select
                        id="case-status"
                        value={status}
                        onChange={(event) =>
                          onDecision(event.target.value as ReviewStatus)
                        }
                      >
                        {Object.entries(statusLabels).map(([value, label]) => (
                          <option value={value} key={value}>
                            {label}
                          </option>
                        ))}
                      </select>
                      <ChevronDown size={14} />
                    </div>
                  </div>
                </div>
                <div
                  className="panel-tabs"
                  role="tablist"
                  aria-label="Case sections"
                >
                  <button
                    role="tab"
                    aria-selected={section === "evidence"}
                    aria-controls="case-evidence"
                    id="evidence-tab"
                    onClick={() => setSection("evidence")}
                  >
                    <ListChecks size={15} />
                    Evidence
                  </button>
                  <button
                    role="tab"
                    aria-selected={section === "activity"}
                    aria-controls="case-activity"
                    id="activity-tab"
                    onClick={() => setSection("activity")}
                  >
                    <History size={15} />
                    Activity
                    {Boolean(decision?.events.length) && (
                      <span>{decision!.events.length}</span>
                    )}
                  </button>
                </div>
                <div className="panel-scroll">
                  {section === "evidence" ? (
                    <div
                      role="tabpanel"
                      id="case-evidence"
                      aria-labelledby="evidence-tab"
                    >
                      <section className="case-section">
                        <h3>Why this needs review</h3>
                        {item.findings.map((finding) => (
                          <article
                            className="evidence-finding"
                            key={finding.finding_id}
                          >
                            <div className="finding-marker">
                              <span
                                className={`severity-dot ${item.audit_band}`}
                              />
                            </div>
                            <div>
                              <h4>{finding.finding}</h4>
                              <p>{finding.evidence}</p>
                              <div className="next-step">
                                <ArrowUpRight size={14} />
                                <span>{finding.recommended_action}</span>
                              </div>
                            </div>
                          </article>
                        ))}
                        {!item.findings.length && (
                          <p className="muted">
                            No audit checks flagged this payment. The model
                            identified an unusual pattern for review.
                          </p>
                        )}
                        <div className="model-evidence">
                          <div>
                            <Sparkles size={16} />
                            <h4>Model signal</h4>
                            {item.model.decision !== null && (
                              <span className="model-number">
                                {item.model.score}
                                <small>/100</small>
                              </span>
                            )}
                          </div>
                          <p>
                            {item.model.decision === null
                              ? "This file has fewer than 50 transactions, so the model did not run."
                              : item.model.flagged
                                ? item.model.context
                                : "This payment was not flagged by the anomaly model."}
                          </p>
                          {item.model.decision !== null && (
                            <details>
                              <summary>What does this score mean?</summary>
                              <p>
                                Relative unusualness within this file, not a
                                probability of fraud. Flagged payments score
                                50–100; higher values rank as more unusual.
                                Invoice and payee patterns also appear in audit
                                checks, so these signals can overlap.
                              </p>
                            </details>
                          )}
                        </div>
                      </section>
                      {related.length > 0 && (
                        <section className="case-section related-section">
                          <div className="section-heading">
                            <h3>
                              {explicitRelated.length
                                ? "Linked payments"
                                : "Similar payments"}
                            </h3>
                            <span>{related.length}</span>
                          </div>
                          <p className="section-caption">
                            {explicitRelated.length
                              ? "Linked by the audit check above."
                              : "Same payee and currency, with an equal amount or a payment date within three days. These are context, not confirmed duplicates."}
                          </p>
                          <div className="comparison-table">
                            <table>
                              <thead>
                                <tr>
                                  <th>Transaction</th>
                                  <th>Date</th>
                                  <th className="align-right">Paid</th>
                                </tr>
                              </thead>
                              <tbody>
                                <tr className="current-payment">
                                  <td>
                                    <span className="mono">
                                      {text(transaction.transaction_id)}
                                    </span>
                                    <small>Current payment</small>
                                  </td>
                                  <td>
                                    {date(transaction.transaction_date, {
                                      day: "2-digit",
                                      month: "short",
                                    })}
                                  </td>
                                  <td className="align-right">
                                    {money(
                                      transaction.amount,
                                      transaction.currency,
                                    )}
                                  </td>
                                </tr>
                                {related.map((record) => (
                                  <tr key={record.id}>
                                    <td>
                                      <button
                                        className="related-link mono"
                                        onClick={() => onNavigate(record.id)}
                                      >
                                        {text(
                                          record.transaction.transaction_id,
                                        )}
                                        <ArrowUpRight size={12} />
                                      </button>
                                      <small>
                                        {text(
                                          record.transaction.invoice_reference,
                                        )}
                                      </small>
                                    </td>
                                    <td>
                                      {date(
                                        record.transaction.transaction_date,
                                        { day: "2-digit", month: "short" },
                                      )}
                                    </td>
                                    <td className="align-right">
                                      {money(
                                        record.transaction.amount,
                                        record.transaction.currency,
                                      )}
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                          {transaction.invoice_amount !== undefined &&
                            transaction.invoice_amount !== null && (
                              <div className="invoice-total">
                                <span>Invoice total</span>
                                <strong>
                                  {money(
                                    transaction.invoice_amount,
                                    transaction.currency,
                                  )}
                                </strong>
                              </div>
                            )}
                        </section>
                      )}
                      <section className="case-section">
                        <h3>
                          <FileText size={15} />
                          Transaction details
                        </h3>
                        <dl className="transaction-details">
                          {Object.entries(transaction)
                            .filter(
                              ([field]) =>
                                !["is_anomaly", "anomaly_type"].includes(field),
                            )
                            .map(([field, value]) => (
                              <div key={field}>
                                <dt>{humanField(field)}</dt>
                                <dd
                                  className={
                                    field.endsWith("_id") ||
                                    field === "invoice_reference"
                                      ? "mono"
                                      : ""
                                  }
                                >
                                  {field === "amount" ||
                                  field === "invoice_amount"
                                    ? money(value, transaction.currency)
                                    : field === "transaction_date"
                                      ? date(value)
                                      : text(value)}
                                </dd>
                              </div>
                            ))}
                        </dl>
                      </section>
                      <section className="case-section notes-section">
                        <h3>
                          <MessageSquare size={15} />
                          Review note
                        </h3>
                        <textarea
                          aria-label="Review note"
                          value={note}
                          onChange={(event) => setNote(event.target.value)}
                          placeholder="Add context or a question for follow-up…"
                          rows={3}
                        />
                        <div className="note-actions">
                          <span>Session only · included in CSV export</span>
                          <button
                            className="button small"
                            disabled={note === (decision?.note ?? "")}
                            onClick={() => onNote(note)}
                          >
                            <Check size={14} />
                            Save note
                          </button>
                        </div>
                      </section>
                    </div>
                  ) : (
                    <div
                      role="tabpanel"
                      id="case-activity"
                      aria-labelledby="activity-tab"
                      className="case-section"
                    >
                      <h3>Review activity</h3>
                      <p className="section-caption">
                        This session only. Changes are recorded when you make a
                        decision or save a note.
                      </p>
                      {!decision?.events.length ? (
                        <div className="empty-activity">
                          <History size={23} />
                          <p>No review actions yet</p>
                        </div>
                      ) : (
                        <ol className="activity-list">
                          {[...decision.events]
                            .reverse()
                            .map((event, index) => (
                              <li key={`${event.at}-${index}`}>
                                <span className="activity-dot" />
                                <div>
                                  <p>{event.description}</p>
                                  <time dateTime={event.at}>
                                    {new Date(event.at).toLocaleTimeString(
                                      "en-GB",
                                      {
                                        hour: "2-digit",
                                        minute: "2-digit",
                                        second: "2-digit",
                                      },
                                    )}
                                  </time>
                                </div>
                              </li>
                            ))}
                        </ol>
                      )}
                    </div>
                  )}
                </div>
                <div className="panel-footer">
                  <span>
                    <i />
                    Source row {item.row_number}
                  </span>
                  {canUndo ? (
                    <button className="panel-undo" onClick={onUndo}>
                      Undo
                    </button>
                  ) : (
                    <span>Human review required</span>
                  )}
                </div>
              </motion.aside>
            </Dialog.Content>
          </Dialog.Portal>
        )}
      </AnimatePresence>
    </Dialog.Root>
  );
}
