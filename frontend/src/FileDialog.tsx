import { useRef, useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import {
  ArrowRight,
  Check,
  FileSpreadsheet,
  LoaderCircle,
  Upload,
  X,
} from "lucide-react";
import { humanField } from "./format";
import type { AuditRun, FileInspection } from "./types";

async function request<T>(path: string, body: FormData): Promise<T> {
  const response = await fetch(path, { method: "POST", body });
  if (!response.ok) {
    const data = await response.json();
    throw new Error(
      typeof data.detail === "string"
        ? data.detail
        : "Check the selected file and try again.",
    );
  }
  return response.json();
}

export default function FileDialog({
  open,
  onOpenChange,
  onRun,
}: {
  open: boolean;
  onOpenChange: (value: boolean) => void;
  onRun: (run: AuditRun) => void;
}) {
  const input = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [inspection, setInspection] = useState<FileInspection | null>(null);
  const [mapping, setMapping] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [dragging, setDragging] = useState(false);
  const required = [
    "transaction_id",
    "transaction_date",
    "account_id",
    "amount",
    "currency",
    "counterparty",
  ];

  function changeMapping(field: string, source: string) {
    setMapping((previous) =>
      Object.fromEntries([
        ...Object.entries(previous).filter(
          ([key, value]) => key !== field && (!source || value !== source),
        ),
        [field, source],
      ]),
    );
  }

  async function inspect(file: File) {
    setFile(file);
    setBusy("Reading file");
    setError("");
    setInspection(null);
    try {
      const data = new FormData();
      data.append("file", file);
      const result = await request<FileInspection>("/api/inspect", data);
      setInspection(result);
      setMapping(result.mapping);
    } catch (error) {
      setError(error instanceof Error ? error.message : "Could not read file.");
    } finally {
      setBusy("");
    }
  }
  async function analyse() {
    if (!file) return;
    setBusy("Running audit checks and model");
    setError("");
    try {
      const data = new FormData();
      data.append("file", file);
      data.append("mapping", JSON.stringify(mapping));
      onRun(await request<AuditRun>("/api/analyse", data));
      onOpenChange(false);
    } catch (error) {
      setError(
        error instanceof Error ? error.message : "Could not analyse file.",
      );
    } finally {
      setBusy("");
    }
  }

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(value) => {
        if (!busy) onOpenChange(value);
      }}
    >
      <Dialog.Portal>
        <Dialog.Overlay className="modal-backdrop" />
        <Dialog.Content
          className="file-dialog"
          aria-describedby="file-description"
        >
          <div className="dialog-heading">
            <div>
              <Dialog.Title>Start a transaction review</Dialog.Title>
              <Dialog.Description id="file-description">
                Choose a CSV or Excel transaction file, then confirm its
                columns.
              </Dialog.Description>
            </div>
            <Dialog.Close asChild>
              <button
                className="icon-button"
                aria-label="Close upload"
                disabled={!!busy}
              >
                <X size={19} />
              </button>
            </Dialog.Close>
          </div>
          <button
            className={`dropzone ${dragging ? "dragging" : ""}`}
            onClick={() => input.current?.click()}
            onDragOver={(event) => {
              event.preventDefault();
              setDragging(true);
            }}
            onDragLeave={() => setDragging(false)}
            onDrop={(event) => {
              event.preventDefault();
              setDragging(false);
              const uploaded = event.dataTransfer.files[0];
              if (uploaded && !busy) void inspect(uploaded);
            }}
            disabled={!!busy}
          >
            {busy ? (
              <LoaderCircle className="spin" size={24} />
            ) : file ? (
              <FileSpreadsheet size={25} />
            ) : (
              <Upload size={25} />
            )}
            <strong>
              {busy || file?.name || "Drop a transaction file here"}
            </strong>
            <span>
              {busy
                ? "Preparing your review…"
                : file
                  ? "Choose a different file"
                  : "or choose a file · CSV / XLSX · up to 4 MB"}
            </span>
          </button>
          <input
            ref={input}
            hidden
            type="file"
            accept=".csv,.xlsx"
            aria-label="Transaction file"
            onChange={(event) => {
              const uploaded = event.target.files?.[0];
              if (uploaded) void inspect(uploaded);
            }}
          />
          {inspection && (
            <div className="mapping-area">
              <div className="mapping-heading">
                <Check size={15} />
                <span>{inspection.row_count.toLocaleString()} source rows</span>
              </div>
              <div className="mapping-grid">
                {required.map((field) => (
                  <label key={field}>
                    {humanField(field)}
                    <select
                      aria-label={humanField(field)}
                      value={mapping[field] ?? ""}
                      onChange={(event) =>
                        changeMapping(field, event.target.value)
                      }
                    >
                      <option value="">Choose column</option>
                      {inspection.columns.map((column) => (
                        <option key={column}>{column}</option>
                      ))}
                    </select>
                  </label>
                ))}
              </div>
              <details>
                <summary>Invoice and optional columns</summary>
                <div className="mapping-grid">
                  {inspection.fields
                    .filter((field) => !required.includes(field))
                    .map((field) => (
                      <label key={field}>
                        {humanField(field)}
                        <select
                          aria-label={humanField(field)}
                          value={mapping[field] ?? ""}
                          onChange={(event) =>
                            changeMapping(field, event.target.value)
                          }
                        >
                          <option value="">Not included</option>
                          {inspection.columns.map((column) => (
                            <option key={column}>{column}</option>
                          ))}
                        </select>
                      </label>
                    ))}
                </div>
              </details>
            </div>
          )}
          {error && (
            <p className="error-message" role="alert">
              {error}
            </p>
          )}
          <div className="dialog-footer">
            <span>Review decisions stay in this browser session.</span>
            <button
              className="button primary"
              onClick={() => void analyse()}
              disabled={
                !!busy ||
                !inspection ||
                required.some((field) => !mapping[field])
              }
            >
              Start review
              <ArrowRight size={15} />
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
