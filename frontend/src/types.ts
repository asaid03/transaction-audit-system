export type Band = "high" | "medium" | "low";
export type ReviewStatus =
  "needs_review" | "needs_attention" | "reviewed" | "dismissed";
export type Scope = "all" | "audit" | "model";
export type Value = string | number | boolean | null;
export interface Finding {
  finding_id: string;
  finding: string;
  evidence: string;
  why_it_matters: string;
  recommended_action: string;
  technical_issue_type: string;
  related_record_id: string;
  related_dataset: string;
  review_priority_score: number;
  row_number: number;
  review_priority_band: string;
}
export interface ReviewCase {
  id: string;
  row_number: number;
  transaction: Record<string, Value>;
  audit_score: number;
  audit_band: Band;
  audit_drivers: string;
  findings: Finding[];
  model: {
    score: number;
    band: Band;
    flagged: boolean;
    decision: number | null;
    context: string;
  };
}
export interface AuditRun {
  filename: string;
  demo: boolean;
  row_count: number;
  date_start: string | null;
  date_end: string | null;
  mapping: Record<string, string>;
  columns: string[];
  cases: ReviewCase[];
  transactions: ReviewCase[];
  cleaning: string[];
  finding_count: number;
  large_payment_limit: number;
  model_available: boolean;
  unlinked_findings: Finding[];
}
export interface FileInspection {
  filename: string;
  row_count: number;
  columns: string[];
  fields: string[];
  mapping: Record<string, string>;
  missing_required: string[];
  sample: Record<string, Value>[];
  cleaning: string[];
}
export interface ReviewEvent {
  at: string;
  description: string;
}
export interface Decision {
  status: ReviewStatus;
  note: string;
  events: ReviewEvent[];
}
