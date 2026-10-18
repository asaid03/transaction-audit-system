import { useEffect, useMemo, useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import * as Dialog from "@radix-ui/react-dialog";
import {
  Activity,
  ArrowDownToLine,
  ArrowRight,
  ArrowUpRight,
  Check,
  ChevronDown,
  CircleHelp,
  FileSpreadsheet,
  FolderOpen,
  Layers,
  ListFilter,
  ListTodo,
  LoaderCircle,
  Moon,
  Plus,
  Search,
  Settings2,
  ShieldCheck,
  Sparkles,
  Sun,
  Undo2,
  X,
} from "lucide-react";
import CasePanel from "./CasePanel";
import FileDialog from "./FileDialog";
import { Queue, type SortKey } from "./Queue";
import { date, exportCsv, money, statusLabels, text } from "./format";
import type {
  AuditRun,
  Band,
  Decision,
  ReviewCase,
  ReviewStatus,
  Scope,
} from "./types";

const emptyDecision: Decision = {
  status: "needs_review",
  note: "",
  events: [],
};
const bandOrder = { high: 3, medium: 2, low: 1 };

export default function App() {
  const [run, setRun] = useState<AuditRun | null>(null);
  const [busy, setBusy] = useState(true);
  const [error, setError] = useState("");
  const [scope, setScope] = useState<Scope>("all");
  const [search, setSearch] = useState("");
  const [band, setBand] = useState("all");
  const [issueType, setIssueType] = useState("all");
  const [currency, setCurrency] = useState("all");
  const [status, setStatus] = useState("open");
  const [sort, setSort] = useState<SortKey>("priority");
  const [direction, setDirection] = useState(-1);
  const [selected, setSelected] = useState<string | null>(null);
  const [panelOpen, setPanelOpen] = useState(false);
  const [decisions, setDecisions] = useState<Record<string, Decision>>({});
  const [undo, setUndo] = useState<{
    id: string;
    before: Decision;
    message: string;
  } | null>(null);
  const [uploadOpen, setUploadOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [aboutOpen, setAboutOpen] = useState(false);
  const [theme, setTheme] = useState(
    () => localStorage.getItem("audit-theme") ?? "light",
  );
  const [modelCutoff, setModelCutoff] = useState(50);
  const [filtersOpen, setFiltersOpen] = useState(false);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("audit-theme", theme);
  }, [theme]);
  useEffect(() => {
    let active = true;
    fetch("/api/demo")
      .then(async (response) => {
        if (!response.ok)
          throw new Error(
            "The review engine is unavailable. Start the Python API and retry.",
          );
        return response.json();
      })
      .then((data) => {
        if (active) setRun(data);
      })
      .catch((error) => {
        if (active) setError(String(error.message));
      })
      .finally(() => {
        if (active) setBusy(false);
      });
    return () => {
      active = false;
    };
  }, []);

  const visibleModel = (item: ReviewCase) =>
    item.model.flagged && item.model.score >= modelCutoff;
  const statusOf = (item: ReviewCase) =>
    decisions[item.id]?.status ?? "needs_review";
  const openCase = (item: ReviewCase) =>
    !["reviewed", "dismissed"].includes(statusOf(item));
  const candidates =
    run?.cases.filter((item) => item.findings.length || visibleModel(item)) ??
    [];
  const scoped = candidates.filter(
    (item) =>
      scope === "all" ||
      (scope === "audit" ? item.findings.length : visibleModel(item)),
  );
  const openCases = scoped.filter(openCase);
  const auditCases = candidates.filter((item) => item.findings.length);
  const modelCases = candidates.filter(visibleModel);
  const highCount = openCases.filter(
    (item) => item.findings.length && item.audit_band === "high",
  ).length;
  const mediumCount = openCases.filter(
    (item) => item.findings.length && item.audit_band === "medium",
  ).length;
  const lowCount = openCases.filter(
    (item) => item.findings.length && item.audit_band === "low",
  ).length;
  const modelOnlyCount = openCases.filter(
    (item) => !item.findings.length,
  ).length;
  const closedCount = candidates.filter((item) => !openCase(item)).length;

  const rows = useMemo(() => {
    const result = scoped.filter((item) => {
      const matchesStatus =
        status === "all" ||
        (status === "open"
          ? openCase(item)
          : status === "closed"
            ? !openCase(item)
            : statusOf(item) === status);
      const matchesBand =
        band === "all" ||
        (band === "model"
          ? !item.findings.length
          : item.findings.length > 0 && item.audit_band === band);
      const matchesIssue =
        issueType === "all" ||
        item.findings.some(
          (finding) => finding.technical_issue_type === issueType,
        );
      const matchesCurrency =
        currency === "all" || item.transaction.currency === currency;
      const query = search.trim().toLowerCase();
      const matchesSearch =
        !query ||
        Object.values(item.transaction).some(
          (value) =>
            value !== null && String(value).toLowerCase().includes(query),
        ) ||
        item.findings.some((finding) =>
          finding.finding.toLowerCase().includes(query),
        );
      return (
        matchesStatus &&
        matchesBand &&
        matchesIssue &&
        matchesCurrency &&
        matchesSearch
      );
    });
    return result.sort((a, b) => {
      let difference = 0;
      if (sort === "priority")
        difference =
          scope === "model"
            ? a.model.score - b.model.score
            : (a.findings.length
                ? bandOrder[a.audit_band] * 1000 + a.audit_score
                : -1) -
                (b.findings.length
                  ? bandOrder[b.audit_band] * 1000 + b.audit_score
                  : -1) || a.model.score - b.model.score;
      if (sort === "amount")
        difference =
          String(a.transaction.currency).localeCompare(
            String(b.transaction.currency),
          ) ||
          Number(a.transaction.amount ?? 0) - Number(b.transaction.amount ?? 0);
      if (sort === "date")
        difference = String(a.transaction.transaction_date ?? "").localeCompare(
          String(b.transaction.transaction_date ?? ""),
        );
      if (sort === "payee")
        difference = text(a.transaction.counterparty).localeCompare(
          text(b.transaction.counterparty),
        );
      return difference * direction || a.row_number - b.row_number;
    });
  }, [
    run,
    scope,
    decisions,
    status,
    band,
    issueType,
    currency,
    search,
    sort,
    direction,
    modelCutoff,
  ]);

  const current =
    run?.transactions.find((item) => item.id === selected) ?? null;
  const position = rows.findIndex((item) => item.id === selected);
  const types = new Map(
    candidates.flatMap((item) =>
      item.findings.map(
        (finding) => [finding.technical_issue_type, finding.finding] as const,
      ),
    ),
  );
  const currencies = [
    ...new Set(
      run?.transactions
        .map((item) => String(item.transaction.currency))
        .filter(Boolean),
    ),
  ].sort();
  const hasFilters =
    band !== "all" ||
    issueType !== "all" ||
    currency !== "all" ||
    search !== "" ||
    status !== "open";

  function resetFilters() {
    setBand("all");
    setIssueType("all");
    setCurrency("all");
    setSearch("");
    setStatus("open");
  }
  function changeScope(value: Scope) {
    setScope(value);
    resetFilters();
  }
  function focusBand(value: string) {
    resetFilters();
    setBand(value);
  }
  function setRunData(value: AuditRun) {
    setRun(value);
    setDecisions({});
    setSelected(null);
    setPanelOpen(false);
    setUndo(null);
    resetFilters();
  }
  function selectCase(id: string) {
    setSelected(id);
    setPanelOpen(true);
  }
  function decision(status: ReviewStatus) {
    if (!current) return;
    const before = decisions[current.id] ?? emptyDecision;
    if (before.status === status) return;
    setDecisions((previous) => ({
      ...previous,
      [current.id]: {
        ...before,
        status,
        events: [
          ...before.events,
          {
            at: new Date().toISOString(),
            description: `Status changed from ${statusLabels[before.status].toLowerCase()} to ${statusLabels[status].toLowerCase()}.`,
          },
        ],
      },
    }));
    setUndo({
      id: current.id,
      before,
      message: `Marked ${statusLabels[status].toLowerCase()}`,
    });
  }
  function note(value: string) {
    if (!current) return;
    const before = decisions[current.id] ?? emptyDecision;
    setDecisions((previous) => ({
      ...previous,
      [current.id]: {
        ...before,
        note: value,
        events: [
          ...before.events,
          {
            at: new Date().toISOString(),
            description: value ? "Review note saved." : "Review note cleared.",
          },
        ],
      },
    }));
    setUndo({ id: current.id, before, message: "Review note saved" });
  }
  function undoLast() {
    if (!undo) return;
    setDecisions((previous) => ({
      ...previous,
      [undo.id]: {
        ...undo.before,
        events: [
          ...(previous[undo.id]?.events ?? []),
          {
            at: new Date().toISOString(),
            description: "Previous review action undone.",
          },
        ],
      },
    }));
    setUndo(null);
  }
  const navigate = (target: string) => {
    if (target === "previous" || target === "next") {
      const next = rows[position + (target === "previous" ? -1 : 1)];
      if (next) setSelected(next.id);
    } else setSelected(target);
  };

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a className="brand" href="/" aria-label="Transaction Audit home">
          <img src="/audit-mark.svg" alt="" />
          <span>
            Transaction Audit<small>REVIEW WORKSPACE</small>
          </span>
        </a>
        <div className="workspace-label">WORKSPACE</div>
        <nav aria-label="Main navigation">
          <button
            aria-label="Investigation"
            title="Investigation"
            className={scope !== "model" ? "nav-item active" : "nav-item"}
            onClick={() => changeScope("all")}
          >
            <ListTodo size={18} />
            <span>Investigation</span>
            <b>{candidates.filter(openCase).length || "—"}</b>
          </button>
          <button
            aria-label="Model signals"
            title="Model signals"
            className={scope === "model" ? "nav-item active" : "nav-item"}
            onClick={() => changeScope("model")}
          >
            <Sparkles size={18} />
            <span>Model signals</span>
            <b>{modelCases.filter(openCase).length || "—"}</b>
          </button>
          <button
            className="nav-item"
            aria-label="Source file"
            title="Source file"
            onClick={() => setAboutOpen(true)}
          >
            <FolderOpen size={18} />
            <span>Source file</span>
          </button>
        </nav>
        {run && (
          <div className="sidebar-file">
            <div className="workspace-label">CURRENT FILE</div>
            <FileSpreadsheet size={18} />
            <strong title={run.filename}>{run.filename}</strong>
            <span>{run.row_count.toLocaleString()} transactions</span>
            <span className="source-tag">
              {run.demo ? "Synthetic sample" : "Uploaded file"}
            </span>
          </div>
        )}
        <div className="sidebar-bottom">
          <button
            className="nav-item"
            aria-label="Review settings"
            title="Review settings"
            onClick={() => setSettingsOpen(true)}
          >
            <Settings2 size={17} />
            <span>Review settings</span>
          </button>
          <button
            className="nav-item"
            aria-label="About this review"
            title="About this review"
            onClick={() => setAboutOpen(true)}
          >
            <CircleHelp size={17} />
            <span>About this review</span>
          </button>
          <div className="sidebar-bottom-line">
            <span>
              <i />
              Session only
            </span>
            <button
              className="icon-button"
              title={
                theme === "light"
                  ? "Use dark appearance"
                  : "Use light appearance"
              }
              aria-label={
                theme === "light"
                  ? "Use dark appearance"
                  : "Use light appearance"
              }
              onClick={() => setTheme(theme === "light" ? "dark" : "light")}
            >
              {theme === "light" ? <Moon size={16} /> : <Sun size={16} />}
            </button>
          </div>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <div className="breadcrumb">
            <span>Workspace</span>
            <span>/</span>
            <strong>Investigation</strong>
          </div>
          <div className="topbar-actions">
            <span className="sample-indicator">
              <i />
              {run?.demo ? "Synthetic sample" : "Local review"}
            </span>
            <button
              className="button small"
              onClick={() => setUploadOpen(true)}
            >
              <Plus size={15} />
              <span>New review</span>
            </button>
          </div>
        </header>
        <main>
          <div className="page-heading">
            <div className="heading-copy">
              <div className="eyebrow">
                <span />
                TRANSACTION REVIEW
              </div>
              <h1>Payments to investigate.</h1>
              <p>Prioritise unusual payments. Understand every finding.</p>
            </div>
            <button
              className="button export-button"
              onClick={() => exportCsv(rows, decisions)}
              disabled={!run || !rows.length}
            >
              <ArrowDownToLine size={15} />
              Export review
              <ChevronDown size={13} />
            </button>
          </div>
          {busy ? (
            <div className="loading-state" role="status">
              <LoaderCircle size={24} className="spin" />
              <h2>Preparing your investigation</h2>
              <p>
                Running audit checks and the anomaly model on the sample file.
              </p>
              <div className="loading-lines">
                <i />
                <i />
                <i />
              </div>
            </div>
          ) : error ? (
            <div className="error-state" role="alert">
              <Activity size={28} />
              <h2>We couldn’t prepare the review</h2>
              <p>{error}</p>
              <button
                className="button"
                onClick={() => window.location.reload()}
              >
                Try again
              </button>
            </div>
          ) : (
            run && (
              <>
                <div className="source-strip">
                  <FileSpreadsheet size={17} />
                  <strong title={run.filename}>{run.filename}</strong>
                  <span className="source-divider" />
                  <span>{run.row_count.toLocaleString()} transactions</span>
                  <span className="source-dates">
                    {date(run.date_start)} – {date(run.date_end)}
                  </span>
                  <button
                    title="Inspect source and mapping"
                    aria-label="Inspect source and mapping"
                    className="icon-button"
                    onClick={() => setAboutOpen(true)}
                  >
                    <ArrowRight size={15} />
                  </button>
                </div>
                <section className="summary-strip" aria-label="Review summary">
                  <button
                    className={`summary-total ${status === "open" && band === "all" ? "chosen" : ""}`}
                    onClick={() => focusBand("all")}
                  >
                    <span>NEEDS REVIEW</span>
                    <strong>
                      {openCases.length}
                      <small>payments</small>
                    </strong>
                  </button>
                  <div className="severity-breakdown">
                    {(["high", "medium", "low"] as Band[]).map((value) => (
                      <button
                        key={value}
                        className={`summary-priority ${band === value ? "chosen" : ""}`}
                        onClick={() => {
                          focusBand(band === value ? "all" : value);
                        }}
                      >
                        <span>
                          <i className={`severity-dot ${value}`} />
                          {value} priority
                        </span>
                        <strong>
                          {value === "high"
                            ? highCount
                            : value === "medium"
                              ? mediumCount
                              : lowCount}
                          <ArrowUpRight size={14} />
                        </strong>
                      </button>
                    ))}
                  </div>
                  <button
                    className={`summary-model ${band === "model" ? "chosen" : ""}`}
                    onClick={() => {
                      focusBand(band === "model" ? "all" : "model");
                    }}
                  >
                    <span>
                      <Sparkles size={13} />
                      MODEL ONLY
                    </span>
                    <strong>
                      {modelOnlyCount}
                      <small>additional payments</small>
                    </strong>
                  </button>
                </section>
                <div className="queue-heading">
                  <div
                    className="view-tabs"
                    role="tablist"
                    aria-label="Evidence source"
                  >
                    {(
                      [
                        ["all", "All cases", candidates.length],
                        ["audit", "Audit findings", auditCases.length],
                        ["model", "Model signals", modelCases.length],
                      ] as const
                    ).map(([value, label, count]) => (
                      <button
                        role="tab"
                        key={value}
                        aria-selected={scope === value}
                        onClick={() => changeScope(value)}
                      >
                        {value === "model" && <Sparkles size={14} />}
                        <span>{label}</span>
                        <b>{count}</b>
                      </button>
                    ))}
                  </div>
                  <button
                    className="review-progress"
                    onClick={() => {
                      resetFilters();
                      setStatus("closed");
                    }}
                  >
                    <Check size={13} />
                    {closedCount} reviewed
                  </button>
                </div>
                <div className={`filter-bar ${filtersOpen ? "expanded" : ""}`}>
                  <label className="search-field">
                    <Search size={16} />
                    <input
                      aria-label="Search payments"
                      placeholder="Search payee, transaction or invoice…"
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
                  <button
                    className="button small mobile-filter-toggle"
                    onClick={() => setFiltersOpen(!filtersOpen)}
                    aria-expanded={filtersOpen}
                  >
                    <ListFilter size={15} />
                    Filters
                  </button>
                  <div className="filter-selects">
                    <select
                      aria-label="Priority filter"
                      value={band}
                      onChange={(event) => setBand(event.target.value)}
                    >
                      <option value="all">All priorities</option>
                      <option value="high">High priority</option>
                      <option value="medium">Medium priority</option>
                      <option value="low">Low priority</option>
                      <option value="model">Model only</option>
                    </select>
                    <select
                      aria-label="Finding filter"
                      value={issueType}
                      onChange={(event) => setIssueType(event.target.value)}
                    >
                      <option value="all">All findings</option>
                      {[...types].map(([key, label]) => (
                        <option key={key} value={key}>
                          {label}
                        </option>
                      ))}
                    </select>
                    {currencies.length > 1 && (
                      <select
                        aria-label="Currency filter"
                        value={currency}
                        onChange={(event) => setCurrency(event.target.value)}
                      >
                        <option value="all">All currencies</option>
                        {currencies.map((code) => (
                          <option key={code}>{code}</option>
                        ))}
                      </select>
                    )}
                    <select
                      aria-label="Review status filter"
                      value={status}
                      onChange={(event) => setStatus(event.target.value)}
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
                  </div>
                  {hasFilters && (
                    <button className="reset-filters" onClick={resetFilters}>
                      Reset
                      <X size={12} />
                    </button>
                  )}
                </div>
                <Queue
                  rows={rows}
                  decisions={decisions}
                  selected={panelOpen ? selected : null}
                  onSelect={selectCase}
                  sort={sort}
                  direction={direction}
                  scope={scope}
                  onSort={(value) => {
                    if (sort === value) setDirection(-direction);
                    else {
                      setSort(value);
                      setDirection(value === "payee" ? 1 : -1);
                    }
                  }}
                />
                <div className="queue-footer">
                  <span>
                    {rows.length} {rows.length === 1 ? "payment" : "payments"}
                    {hasFilters ? " matching your filters" : " in this view"}
                    <span className="footer-separator">·</span>
                    {run.finding_count} audit findings across the file
                  </span>
                  <span>
                    <ShieldCheck size={13} />
                    Flags guide review, not conclusions
                  </span>
                </div>
                <div className="engine-note">
                  <div>
                    <ListFilter size={15} />
                    <p>
                      <strong>Audit findings</strong> identify payments that
                      meet a defined check. <strong>Model signals</strong>{" "}
                      highlight patterns that differ from the rest of this file.
                    </p>
                  </div>
                  <button onClick={() => setAboutOpen(true)}>
                    How this review works
                    <ArrowRight size={13} />
                  </button>
                </div>
              </>
            )
          )}
          <footer className="workspace-footer">
            <span>TRANSACTION AUDIT</span>
            <span>Evidence first. Human judgement always.</span>
          </footer>
        </main>
      </div>
      {run && (
        <CasePanel
          item={current}
          run={run}
          decision={selected ? decisions[selected] : undefined}
          open={panelOpen}
          onClose={() => setPanelOpen(false)}
          onDecision={decision}
          onNote={note}
          onNavigate={navigate}
          position={position}
          count={rows.length}
          onUndo={undoLast}
          canUndo={!!undo}
        />
      )}
      <FileDialog
        open={uploadOpen}
        onOpenChange={setUploadOpen}
        onRun={setRunData}
      />
      <Dialog.Root open={settingsOpen} onOpenChange={setSettingsOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="modal-backdrop" />
          <Dialog.Content className="settings-dialog">
            <div className="dialog-heading">
              <div>
                <Dialog.Title>Review settings</Dialog.Title>
                <Dialog.Description>
                  Choose how much model evidence appears in your queue.
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
                step="1"
                value={modelCutoff}
                onChange={(event) => setModelCutoff(Number(event.target.value))}
              />
              <div className="range-labels">
                <span>More signals</span>
                <span>Most unusual</span>
              </div>
              <p>
                Shows model-flagged payments at or above this score. Audit
                findings always stay in the queue.
              </p>
              <div className="live-preview">
                <Sparkles size={19} />
                <strong>
                  {run?.cases.filter(
                    (item) =>
                      item.model.flagged && item.model.score >= modelCutoff,
                  ).length ?? 0}
                </strong>
                <span>model signals at this cutoff</span>
              </div>
            </div>
            <div className="setting-block">
              <h3>Large-payment review limit</h3>
              <strong className="limit-value">
                {run
                  ? new Intl.NumberFormat("en-GB").format(
                      run.large_payment_limit,
                    )
                  : "—"}{" "}
                <small>in each payment’s currency</small>
              </strong>
              <p>
                Payments at or above the engine’s current limit receive a
                finding. This review uses the existing detection settings.
              </p>
            </div>
            <Dialog.Close asChild>
              <button className="button primary full-width">
                Done
                <Check size={15} />
              </button>
            </Dialog.Close>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
      <Dialog.Root open={aboutOpen} onOpenChange={setAboutOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="modal-backdrop" />
          <Dialog.Content className="settings-dialog about-dialog">
            <div className="dialog-heading">
              <div>
                <Dialog.Title>About this review</Dialog.Title>
                <Dialog.Description>
                  Source, scope and evidence for this investigation.
                </Dialog.Description>
              </div>
              <Dialog.Close asChild>
                <button
                  className="icon-button"
                  aria-label="Close review information"
                >
                  <X size={18} />
                </button>
              </Dialog.Close>
            </div>
            <div className="about-block">
              <ShieldCheck size={20} />
              <h3>Audit checks</h3>
              <p>
                Defined checks identify issues such as missing values, possible
                duplicate payments, invoice inconsistencies and large payments.
                Priority helps you decide what to inspect first.
              </p>
            </div>
            <div className="about-block">
              <Sparkles size={20} />
              <h3>Model signals</h3>
              <p>
                Isolation Forest compares payment patterns within the file. The
                score describes relative unusualness and can overlap with the
                audit checks. It does not estimate the probability of fraud.
              </p>
            </div>
            <div className="about-block">
              <Layers size={20} />
              <h3>Session review</h3>
              <p>
                Decisions and notes last until this page reloads or you start
                another review. Export your review to retain them. Theme
                preference is the only setting stored locally.
              </p>
            </div>
            {run && (
              <details className="source-details">
                <summary>Source and column mapping</summary>
                <p>
                  {run.demo
                    ? "Committed synthetic benchmark, evaluated by the audit engine. Ground-truth labels excluded."
                    : run.filename}
                </p>
                <dl>
                  {Object.entries(run.mapping).map(([source, field]) => (
                    <div key={source}>
                      <dt className="mono">{source}</dt>
                      <dd>{field}</dd>
                    </div>
                  ))}
                </dl>
                {run.cleaning.map((line) => (
                  <p key={line}>{line}</p>
                ))}
              </details>
            )}
            <p className="scope-note">
              This first design slice covers transaction review. Reference-file
              upload and a dedicated upload screen follow after design feedback.
            </p>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
      <AnimatePresence>
        {undo && !panelOpen && (
          <motion.div
            className="undo-toast"
            role="status"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 12 }}
          >
            <Check size={16} />
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
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
