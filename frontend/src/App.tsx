import { useEffect, useMemo, useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import {
  ArrowDownToLine,
  ChevronDown,
  FileSpreadsheet,
  ListFilter,
  LoaderCircle,
  Moon,
  Plus,
  Search,
  Settings2,
  ShieldCheck,
  Sun,
  Undo2,
  X,
} from "lucide-react";
import CasePanel from "./CasePanel";
import FileDialog from "./FileDialog";
import { Queue } from "./Queue";
import { date, exportCsv, statusLabels } from "./format";
import { hasVisibleModelSignal, selectReviewCases } from "./reviewQueue";
import type {
  AuditRun,
  Decision,
  ReviewCase,
  ReviewStatus,
  Scope,
} from "./types";
import "./review-workspace.css";

const emptyDecision: Decision = { status: "needs_review", note: "" };
const isClosed = (value: ReviewStatus) =>
  value === "reviewed" || value === "dismissed";

export default function App() {
  const [run, setRun] = useState<AuditRun | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(true);
  const [scope, setScope] = useState<Scope>("all");
  const [search, setSearch] = useState("");
  const [band, setBand] = useState("all");
  const [issue, setIssue] = useState("all");
  const [currency, setCurrency] = useState("all");
  const [status, setStatus] = useState("open");
  const [sort, setSort] = useState("priority");
  const [selected, setSelected] = useState<string | null>(null);
  const [navigation, setNavigation] = useState<string[]>([]);
  const [focusMode, setFocusMode] = useState(false);
  const [decisions, setDecisions] = useState<Record<string, Decision>>({});
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [undo, setUndo] = useState<{
    id: string;
    before: Decision;
    message: string;
  } | null>(null);
  const [uploadOpen, setUploadOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [theme, setTheme] = useState(
    () => localStorage.getItem("audit-theme") ?? "light",
  );
  const [modelCutoff, setModelCutoff] = useState(50);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("audit-theme", theme);
  }, [theme]);
  useEffect(() => {
    const controller = new AbortController();
    fetch("/api/demo", { signal: controller.signal })
      .then(async (response) => {
        if (!response.ok)
          throw new Error("The review engine is unavailable. Please retry.");
        return response.json();
      })
      .then(setRun)
      .catch((error) => {
        if (error.name !== "AbortError") setError(error.message);
      })
      .finally(() => {
        if (!controller.signal.aborted) setBusy(false);
      });
    return () => controller.abort();
  }, []);
  const hasWork =
    Object.keys(decisions).length > 0 || Object.values(drafts).some(Boolean);
  useEffect(() => {
    const preventLoss = (event: BeforeUnloadEvent) => {
      if (hasWork) {
        event.preventDefault();
        event.returnValue = "";
      }
    };
    window.addEventListener("beforeunload", preventLoss);
    return () => window.removeEventListener("beforeunload", preventLoss);
  }, [hasWork]);

  const allCases = run?.review_cases ?? [];
  const statusOf = (item: ReviewCase) =>
    decisions[item.id]?.status ?? "needs_review";
  const candidates = useMemo(
    () =>
      (run?.review_cases ?? []).filter(
        (item) =>
          item.findings.length || hasVisibleModelSignal(item, modelCutoff),
      ),
    [run, modelCutoff],
  );
  const rows = useMemo(() => {
    return selectReviewCases(candidates, decisions, {
      scope,
      status,
      band,
      issue,
      currency,
      search,
      sort,
      modelCutoff,
    });
  }, [
    candidates,
    decisions,
    scope,
    search,
    band,
    issue,
    currency,
    status,
    sort,
    modelCutoff,
  ]);

  // Navigation snapshots survive a status action removing the current case from Open.
  // Explicit filter changes replace the snapshot so Next follows the new results.
  useEffect(() => {
    setNavigation(rows.map((r) => r.id));
  }, [run, scope, search, band, issue, currency, status, sort, modelCutoff]);
  const current = allCases.find((item) => item.id === selected) ?? null;
  const position = selected ? navigation.indexOf(selected) : -1;
  const types = [
    ...new Map(
      allCases.flatMap((item) =>
        item.findings.map((f) => [f.technical_issue_type, f.finding] as const),
      ),
    ),
  ];
  const currencies = [
    ...new Set(allCases.flatMap((item) => item.totals.map((t) => t.currency))),
  ].sort();
  const openCount = candidates.filter(
    (item) => !isClosed(statusOf(item)),
  ).length;
  const memberCount = new Set(
    candidates
      .filter((item) => !isClosed(statusOf(item)))
      .flatMap((item) => item.members.map((m) => m.id)),
  ).size;
  const hasFilters =
    search ||
    band !== "all" ||
    issue !== "all" ||
    currency !== "all" ||
    status !== "open" ||
    scope !== "all";

  function resetFilters() {
    setSearch("");
    setBand("all");
    setIssue("all");
    setCurrency("all");
    setStatus("open");
    setScope("all");
  }
  function choose(id: string) {
    setNavigation(rows.map((row) => row.id));
    setSelected(id);
  }
  function closeCase() {
    const id = selected;
    setSelected(null);
    setFocusMode(false);
    requestAnimationFrame(() =>
      (
        document.getElementById(`open-${id}`) ??
        document.querySelector<HTMLInputElement>(
          'input[aria-label="Search cases"]',
        )
      )?.focus(),
    );
  }
  function navigate(value: "previous" | "next") {
    const target = navigation[position + (value === "next" ? 1 : -1)];
    if (target) setSelected(target);
  }
  function updateDecision(value: Partial<Decision>, message: string) {
    if (!current) return;
    const before = decisions[current.id] ?? emptyDecision;
    setDecisions((previous) => ({
      ...previous,
      [current.id]: { ...before, ...value },
    }));
    setUndo({
      id: current.id,
      before,
      message: `${message} - ${current.title}`,
    });
  }
  function undoLast() {
    if (!undo) return;
    setDecisions((previous) => ({ ...previous, [undo.id]: undo.before }));
    setUndo(null);
  }
  function loadRun(value: AuditRun) {
    if (
      hasWork &&
      !window.confirm(
        "Starting a new review replaces this session's decisions and notes. Continue?",
      )
    )
      return;
    setRun(value);
    setDecisions({});
    setDrafts({});
    setSelected(null);
    setUndo(null);
    setFocusMode(false);
    resetFilters();
  }

  return (
    <div className="review-app">
      <header className="review-topbar">
        <div className="review-brand">
          <img src="/audit-mark.svg" alt="" />
          <strong>Transaction Audit</strong>
          <span>Payment review</span>
        </div>
        <div className="review-top-actions">
          <span className="session-label">Session only</span>
          <button
            className="icon-button"
            title={
              theme === "light" ? "Use dark appearance" : "Use light appearance"
            }
            aria-label={
              theme === "light" ? "Use dark appearance" : "Use light appearance"
            }
            onClick={() => setTheme(theme === "light" ? "dark" : "light")}
          >
            {theme === "light" ? <Moon size={16} /> : <Sun size={16} />}
          </button>
          <button
            className="icon-button"
            title="Review settings"
            aria-label="Review settings"
            onClick={() => setSettingsOpen(true)}
          >
            <Settings2 size={17} />
          </button>
          <button className="button small" onClick={() => setUploadOpen(true)}>
            <Plus size={15} />
            New review
          </button>
        </div>
      </header>
      <main className="review-main">
        <div className="review-page-heading">
          <div>
            <h1>Cases to review</h1>
            <p>
              {run
                ? `${openCount} open cases across ${memberCount} payments`
                : "Payment investigation workspace"}
            </p>
          </div>
          <details className="case-export">
            <summary className="button small">
              <ArrowDownToLine size={15} />
              Export review
              <ChevronDown size={13} />
            </summary>
            <div>
              <button
                disabled={!rows.length}
                onClick={() => exportCsv(rows, decisions, true)}
              >
                Case summaries (.csv)
              </button>
              <button
                disabled={!rows.length}
                onClick={() => exportCsv(rows, decisions)}
              >
                Member payments (.csv)
              </button>
              <small>Current filters / saved session decisions</small>
            </div>
          </details>
        </div>
        {busy ? (
          <div className="case-empty" role="status">
            <LoaderCircle size={24} className="spin" />
            <h2>Preparing your review</h2>
          </div>
        ) : error ? (
          <div className="case-empty" role="alert">
            <h2>Review unavailable</h2>
            <p>{error}</p>
            <button className="button" onClick={() => window.location.reload()}>
              Try again
            </button>
          </div>
        ) : (
          run && (
            <>
              <div className="review-source">
                <FileSpreadsheet size={16} />
                <strong title={run.filename}>{run.filename}</strong>
                <span>{run.row_count.toLocaleString()} transactions</span>
                <span>
                  {date(run.date_start)} to {date(run.date_end)}
                </span>
                <span className="source-tag">
                  {run.demo ? "Synthetic sample" : "Uploaded file"}
                </span>
              </div>
              <div className="case-controls">
                <div
                  className="case-source-filter"
                  role="group"
                  aria-label="Evidence source"
                >
                  {(
                    [
                      ["all", "All cases"],
                      ["audit", "Audit findings"],
                      ["model", "Model signals"],
                    ] as const
                  ).map(([value, label]) => (
                    <button
                      key={value}
                      aria-pressed={scope === value}
                      onClick={() => setScope(value)}
                    >
                      {label}
                    </button>
                  ))}
                </div>
                <label className="case-search">
                  <Search size={16} />
                  <input
                    aria-label="Search cases"
                    placeholder="Search payee, invoice or any linked transaction"
                    value={search}
                    onChange={(event) => setSearch(event.target.value)}
                  />
                  {search && (
                    <button
                      className="icon-button"
                      aria-label="Clear search"
                      onClick={() => setSearch("")}
                    >
                      <X size={13} />
                    </button>
                  )}
                </label>
              </div>
              <div className="case-filters">
                <ListFilter size={15} />
                <select
                  aria-label="Review status filter"
                  value={status}
                  onChange={(e) => setStatus(e.target.value)}
                >
                  <option value="open">Open cases</option>
                  <option value="all">All statuses</option>
                  <option value="closed">Reviewed or dismissed</option>
                  {Object.entries(statusLabels).map(([value, label]) => (
                    <option key={value} value={value}>
                      {label}
                    </option>
                  ))}
                </select>
                <select
                  aria-label="Priority filter"
                  value={band}
                  onChange={(e) => setBand(e.target.value)}
                >
                  <option value="all">All priorities</option>
                  <option value="high">High priority</option>
                  <option value="medium">Medium priority</option>
                  <option value="low">Low priority</option>
                </select>
                <select
                  aria-label="Finding filter"
                  value={issue}
                  onChange={(e) => setIssue(e.target.value)}
                >
                  <option value="all">All findings</option>
                  {types.map(([key, label]) => (
                    <option key={key} value={key}>
                      {label}
                    </option>
                  ))}
                </select>
                {currencies.length > 1 && (
                  <select
                    aria-label="Currency filter"
                    value={currency}
                    onChange={(e) => setCurrency(e.target.value)}
                  >
                    <option value="all">All currencies</option>
                    {currencies.map((c) => (
                      <option key={c}>{c}</option>
                    ))}
                  </select>
                )}
                {hasFilters && (
                  <button className="case-text-button" onClick={resetFilters}>
                    Reset
                    <X size={12} />
                  </button>
                )}
                <label className="case-sort-label">
                  Sort
                  <select
                    aria-label="Sort cases"
                    value={sort}
                    onChange={(e) => setSort(e.target.value)}
                  >
                    <option value="priority">Priority first</option>
                    <option value="amount">Value: largest first</option>
                    <option value="date">Date: newest first</option>
                    <option value="payee">Payee: A to Z</option>
                  </select>
                </label>
              </div>
              <div
                className={`review-split ${current ? "has-investigation" : ""} ${focusMode ? "focus-mode" : ""}`}
              >
                <div className="review-queue-pane">
                  <Queue
                    rows={rows}
                    selected={selected}
                    decisions={decisions}
                    onSelect={choose}
                    compact={!!current}
                  />
                </div>
                {current && (
                  <CasePanel
                    item={current}
                    run={run}
                    decision={decisions[current.id]}
                    draft={
                      drafts[current.id] ?? decisions[current.id]?.note ?? ""
                    }
                    onDraft={(value) =>
                      setDrafts((previous) => ({
                        ...previous,
                        [current.id]: value,
                      }))
                    }
                    onNote={() =>
                      updateDecision(
                        {
                          note:
                            drafts[current.id] ??
                            decisions[current.id]?.note ??
                            "",
                        },
                        "Review note saved",
                      )
                    }
                    onDecision={(value) =>
                      updateDecision(
                        { status: value },
                        `Marked ${statusLabels[value].toLowerCase()}`,
                      )
                    }
                    onClose={closeCase}
                    onNavigate={navigate}
                    onOpenCase={choose}
                    position={position}
                    count={navigation.length}
                    outside={!rows.some((row) => row.id === current.id)}
                    focusMode={focusMode}
                    onFocusMode={() => setFocusMode(!focusMode)}
                    canUndo={!!undo}
                    onUndo={undoLast}
                  />
                )}
              </div>
              <footer className="review-bottom">
                <span>
                  {rows.length} cases in this view / {run.finding_count}{" "}
                  original audit findings
                </span>
                <span>
                  <ShieldCheck size={13} />
                  Review prompts, not conclusions
                </span>
              </footer>
            </>
          )
        )}
      </main>
      {undo && (
        <div className="case-feedback" role="status">
          <ShieldCheck size={15} />
          <span>{undo.message}</span>
          <button onClick={undoLast}>
            <Undo2 size={14} />
            Undo
          </button>
          <button
            className="icon-button"
            aria-label="Dismiss notification"
            onClick={() => setUndo(null)}
          >
            <X size={14} />
          </button>
        </div>
      )}
      <FileDialog
        open={uploadOpen}
        onOpenChange={setUploadOpen}
        onRun={loadRun}
      />
      <Dialog.Root open={settingsOpen} onOpenChange={setSettingsOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="modal-backdrop" />
          <Dialog.Content className="settings-dialog">
            <div className="dialog-heading">
              <div>
                <Dialog.Title>Review settings</Dialog.Title>
                <Dialog.Description>
                  Supporting model signals and source information.
                </Dialog.Description>
              </div>
              <Dialog.Close asChild>
                <button className="icon-button" aria-label="Close settings">
                  <X size={18} />
                </button>
              </Dialog.Close>
            </div>
            <div className="setting-block">
              <div>
                <label htmlFor="model-cutoff">Minimum model score</label>
                <output htmlFor="model-cutoff">{modelCutoff} / 100</output>
              </div>
              <input
                id="model-cutoff"
                type="range"
                min="50"
                max="100"
                value={modelCutoff}
                onChange={(e) => setModelCutoff(Number(e.target.value))}
              />
              <p>
                Only filters existing model flags. Audit findings are unchanged.
                Individual model scores remain available in each case.
              </p>
            </div>
            <div className="setting-block">
              <h3>Session review</h3>
              <p>
                Notes and decisions are kept in this browser tab until you
                refresh or replace the review. Export saved work before leaving.
              </p>
            </div>
            <details className="source-details">
              <summary>Source column mapping</summary>
              <dl>
                {Object.entries(run?.mapping ?? {}).map(([source, field]) => (
                  <div key={source}>
                    <dt>{source}</dt>
                    <dd>{field}</dd>
                  </div>
                ))}
              </dl>
            </details>
            <Dialog.Close asChild>
              <button className="button primary full-width">Done</button>
            </Dialog.Close>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </div>
  );
}
