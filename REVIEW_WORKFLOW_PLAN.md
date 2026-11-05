# Case-Based Review Workflow

Planning date: 2026-11-01. Branch: `review-cases`.
Status: initial implementation built and engineering checks passed;
representative-user usability validation not yet performed.
Baseline inspected: `260016d`. This plan describes intended behavior, not shipped features.

## 1. Outcome And Boundaries

A reviewer should be able to choose a case, understand the observed issue,
compare all relevant payments, identify the next evidence to request, and move
on without reconstructing relationships or losing their place.

Desktop is the design priority. Keep React, FastAPI, the existing audit engine,
and session-only decisions. No new framework, database, model, detection rule,
population-assurance module, or multi-file upload feature in this branch.
Changing how findings are presented must not silently change detection results.
Do not commit, merge, or deploy without the user's approval.

## 2. Research To Design Decisions

These sources inform the design; they do not establish that this particular
workflow is usable. The layout and targets below are project decisions to test.

| Source | Relevant guidance | Application here |
| --- | --- | --- |
| [Apple: Split views](https://developer.apple.com/design/human-interface-guidelines/split-views) | Adjacent selection and detail panes; persistent selected state; appropriate pane widths. | A compact case navigator beside a dominant investigation area, with a focus mode for wide payment comparisons. |
| [Apple: Modality](https://developer.apple.com/design/human-interface-guidelines/modality) | Modals interrupt context and need a clear benefit. | Investigating is the main task, not a modal. Keep dialogs for upload/settings and exceptional confirmations. |
| [Apple: Search fields](https://developer.apple.com/design/human-interface-guidelines/search-fields) | Toolbar search can preserve selection and detail context. | One stable search field; no new search screen or mandatory search wizard. |
| [Apple: Feedback](https://developer.apple.com/design/human-interface-guidelines/feedback) | Feedback should be relevant, accessible, and proportionate to the action. | Inline saved/status feedback and reversible decisions rather than confirmation pop-ups for routine actions. |
| [Apple: Undo and redo](https://developer.apple.com/design/human-interface-guidelines/undo-and-redo) | Make the affected action and result understandable. | Name the case affected by Undo and restore its state without changing another case. No general history framework in this slice. |
| [Nielsen Norman Group: Recognition and recall](https://www.nngroup.com/articles/recognition-and-recall/) | Visible context reduces memory demands. | Show the related payments and calculation together, rather than asking people to remember transaction IDs across views. |
| [Nielsen Norman Group: Progressive disclosure](https://www.nngroup.com/articles/progressive-disclosure/) | Secondary complexity can be revealed when needed. | Facts, comparison, uncertainty and next action are visible; raw fields and technical rule details are expandable. |
| [Shneiderman: The Eyes Have It, 1996](https://www.cs.umd.edu/users/ben/papers/Shneiderman1996eyes.pdf) | Overview, filtering, details, relationships, history and extraction support exploration. | Queue to investigation to review action, with preserved filters and usable export. Not a requirement for a dashboard full of charts. |
| [Microsoft Research: Human-AI interaction guidelines](https://www.microsoft.com/en-us/research/blog/guidelines-for-human-ai-interaction-design/) | Explain system behavior, uncertainty, and allow correction or dismissal. | ML remains supporting context, with its own score and limitations; dismissal is not proof that a transaction is valid. |
| [W3C: Table pattern](https://www.w3.org/WAI/ARIA/apg/patterns/table/) and [Grid pattern](https://www.w3.org/WAI/ARIA/apg/patterns/grid/) | Native tables and interactive grids have different keyboard contracts. | Use a native comparison table and explicit open-case controls. Do not add grid roles without implementing the required focus behavior. |
| [W3C: Focus not obscured](https://www.w3.org/WAI/WCAG22/Understanding/focus-not-obscured-minimum.html) | Sticky content must not hide keyboard focus. | Test sticky case controls, virtual rows, restoration and zoom; automated accessibility scans alone are insufficient. |

Apple inspiration means calm hierarchy, predictable selection, continuity and
forgiving actions. It does not mean oversized typography, glass effects,
decorative animation or a copy of a consumer app.

## 3. Current Implementation Constraints

- `transaction_audit/api.py` calls individual flagged transaction records
  `cases`; there is not yet an underlying-case entity.
- `frontend/src/App.tsx` filters, sorts and stores decisions by transaction-row ID.
- `CasePanel.tsx` is a modal side panel. It resolves linked IDs while rendering,
  and otherwise suggests up to six similar payments. Similarity is not evidence
  that records belong to one case.
- The current queue is virtualised. Keep that existing capability; virtualising
  the DOM does not solve expensive grouping, large JSON payloads or slow filtering.
- `related_record_id` is not a universal transaction link: a concentration
  finding can contain a payee name, while a balance finding refers to a preceding
  transaction. Resolve relationships by issue type, never by blindly parsing all
  related fields as payment IDs.
- Current browser tests assume a modal dialog and payment-level rows. Replace
  those assertions with case-workflow tests; preserve accessibility, upload,
  undo and export coverage rather than deleting inconvenient tests.
- Baseline already includes vendor-ID-aware invoice matching and improved mapping
  suggestions. Do not rebuild those fixes based on the older benchmark report.

## 4. Goals And Acceptance Measures

| Goal | Completion evidence |
| --- | --- |
| No duplicate work | A linked duplicate/split group appears once in the queue, even if every member has a finding. |
| Understandable explanations | In a formative test, reviewers can describe the observation, uncertainty and next step without a developer translating. |
| Evidence at hand | All rule-linked payments are accessible in one comparison; source rows and original invoice variants remain available. |
| Continuous navigation | Search, filters, sort, selection and list position survive opening/closing a case and moving between cases. |
| Honest totals | Case count, payment count and finding count are distinctly labelled; currencies never silently combined. |
| Safe review | Notes do not disappear during navigation; decisions and Undo affect only the intended case. |
| Detection parity | Original validation issues, row scores and ML outputs match the baseline on the same input. |
| Scale | Measure grouping, API response, filtering and browser interaction on 200-, 10,000- and 50,000-row synthetic fixtures within current upload limits. Do not infer large-scale support from a 200-row demo. |

Proposed usability targets, not research claims: at least 4 of 5 representative
reviewers complete the core tasks without assistance; median time to correctly
explain a seeded duplicate case at most 30 seconds and at least 25% better than
the measured baseline. Treat a single dangerous misunderstanding as a redesign
trigger even if the time target passes. A smaller participant sample is formative,
not statistically conclusive; report the actual sample and results.

## 5. Case Identity And Grouping Contract

### Units

- Payment: a source transaction row, addressed by its row identity, not only its
  transaction ID (transaction IDs can themselves be duplicated).
- Finding: one original rule observation with its source and related evidence.
- Review case: a coherent investigation containing payments and findings.
- Context payment: useful evidence, not automatically an implicated payment or a
  member of a case, and never silently included in its totals.

### Grouping Rules

1. An existing duplicate/split rule relationship is the initial authority for
   combining payment rows. Do not create new matching conclusions in the UI.
2. Resolve duplicate-ID groups to all applicable source rows. Preserve ambiguous
   relationships explicitly instead of choosing the first transaction-ID match.
3. Deduplicate reciprocal links. Allow transitive A-B-C membership only within
   the same compatible relationship family and vendor/invoice/currency context.
   Do not use unrestricted connected components over every finding type.
4. Shared payee, same amount, nearby dates, or an invoice number alone do not
   justify merging. Different vendor IDs or currencies remain separate. Use the
   engine's matching semantics, including its invoice normalisation, rather than
   implementing a second normaliser in JavaScript.
5. An invoice case can contain both a payment relationship and other findings on
   those same members. Present the different findings as separate reasons inside
   the case. Do not lose individual-row findings when removing queue duplicates.
6. If a row bridges incompatible groups, retain separate investigations with an
   explicit related-case link. A transaction may legitimately appear in multiple
   different cases. Never mark all of them reviewed because they share a row.
7. Population findings such as concentration are separate investigations with
   their own context; they must not combine a supplier's invoices into one invoice
   case. Balance evidence must not turn a statement into one transaction chain.
8. Findings with no resolvable row remain visible as file-level cases. Broken or
   out-of-file references are reported as unavailable evidence, not discarded.
9. Unrelated row findings become singleton cases. Model-only rows remain
   singleton cases unless already included in an audit case. Preserve each
   member's original ML score and flag; do not average them into a case ML score.
10. Stable case IDs depend on grouping family and sorted source-row membership
    within the run. They must not depend on queue order, selection or filters.
    Case IDs are not persistent cross-upload identities in this branch.

Case priority: highest existing audit band, then highest existing finding score
for sorting, with a stable tie-breaker. Display this as inherited priority, not
a newly calculated case risk score. ML must not inflate it.

Coverage invariant: every original finding is traceable from at least one case;
every payment membership has an explainable relationship; no original finding is
deleted or rewritten merely to reduce queue length.

## 6. Reviewer Workflow

### A. Orient And Choose

- Start with one case queue: priority, case title, payee, invoice/context,
  number of payments, period, currency, value under review and review status.
- A small summary distinguishes open cases, involved payments and source rows.
  Avoid a wall of metrics. Do not label summed payment value as money lost.
- Default order is highest audit priority first. Keep model-only cases clearly
  labelled and secondary. One source filter is enough; avoid duplicate navigation
  controls for the same audit/model distinction.
- Search matches any member's transaction ID, payee or original invoice variant,
  as well as the case title. A match reveals the complete case, not just the
  matching payment. Filters likewise select cases, not partial evidence sets.
- Initially expose priority, finding category, status and currency. Put date/payee
  refinements behind a filter control if testing establishes a need.

### B. Investigate

At wide desktop widths, selection opens a nonmodal investigation region that
occupies most of the available workspace. A compact case navigator remains at
the left with an unmistakable selected state. The existing global sidebar should
collapse or simplify when necessary so it does not create three cramped columns.
Provide a focus action to hide the case navigator and a clear way to restore it.
At narrow desktop widths, use a full-workspace detail view with Back to cases.
No bespoke mobile redesign in this branch; do not knowingly break basic access.

Recommended first comparison: fixed, comfortable navigator width of roughly
320-380 px and detail minimum around 680 px, validated against actual text and
browser zoom. Collapse based on available content width, not assumed device type.
No draggable-pane framework unless fixed widths demonstrably fail testing.

First visible investigation content:

1. A specific title, payee/invoice context, priority and payment count.
2. Observed facts: a short plain-language statement, not a raw rule message.
3. A calculation or exact comparison when one is supported by the source data.
4. All implicated payments together, with ID, date, invoice variant, paid amount,
   currency, source row and any row-specific finding indicator.
5. Why it may matter, what remains unknown, and the concrete next review action.

Raw source fields, full rule evidence and ML details are secondary disclosures.
Selecting a payment expands its evidence within this case; it does not jump to
another investigation or open nested modals. Large groups use a scrollable or
paged comparison with a visible total count; no silent first-six truncation.

### C. Record And Continue

- Reuse current session statuses: Needs review, Needs attention, Reviewed,
  Dismissed. Reviewed is not confirmed fraud, validity or recovery.
- Apply decisions at case level. Notes and decisions remain session-only, with
  that limitation visible near the review action and export.
- Keep editable note drafts by case during navigation; Save remains explicit.
  Never reset a draft just because the selected case changes. Warn before
  replacing a run or leaving with unsaved work where browser behavior permits.
- After a status action, keep the case visible with clear feedback, even if it
  no longer matches the Open filter. Offer explicit Next case; do not unexpectedly
  advance or blank the evidence before the reviewer can confirm the action.
- Previous/Next follows the active filtered and sorted queue. On a disappearing
  item, compute the next target from the pre-action ordering. Disable boundaries;
  do not wrap silently. Undo must restore the correct case and counts.
- Back restores filters, search, ordering and scroll anchor. Keep the selected
  case visible if filters change; identify when it is outside the current results
  instead of silently switching to a different investigation.

## 7. Explanation And Calculation Contract

Example structure (illustrative amounts, not benchmark results):

> Possible invoice overpayment - Supplier A - INV-88001
>
> Observed: two recorded payments total GBP 4,980 against a supplied invoice
> amount of GBP 2,500. Potential excess: GBP 2,480.
>
> Uncertain: the invoice amount may be incomplete, or a credit, reversal or
> amendment may explain the difference. This is not a confirmed loss.
>
> Next: compare both payment records with the invoice and settlement evidence.

- Generate explanations from typed values, not by extracting numbers from prose.
- Use deterministic templates, not an LLM. Preserve the original rule evidence.
- Only show invoice excess where members have compatible invoice identity,
  one currency and consistent usable invoice totals. Count the invoice once;
  deduplicate payments by source row. Use exact monetary arithmetic and the
  engine's configured tolerance. Unknown or inconsistent values stay unavailable.
- Split payments show the reconciled total and the amount remaining, without
  claiming avoidance of approval or asserting an approved schedule was checked.
- Large-payment findings name the actual configured limit and the row currency,
  and state the comparison is inclusive where the engine uses >=. Do not invent
  a company policy, vendor baseline or currency conversion.
- Missing-invoice findings distinguish an absent reference from proof that no
  invoice exists. Similar payments and ML patterns are not confirmed duplicates.
- If a case includes different currencies, display separate totals, not one
  monetary summary. Potential excess remains unavailable unless justified.
- Do not produce a global exposure total by summing overlapping cases. Define
  value under review as unique included payment rows within each currency.
- Long names, invoice references and IDs stay accessible without relying solely
  on hover; wrap or expand them. All linked members remain inspectable.

## 8. Small Implementation Slices

### Slice 1: Case Model And Tests

Add one focused pure-Python case builder (proposed `transaction_audit/cases.py`)
and tests. Accept existing payment/finding results, produce structured case
records, grouping reasons and membership. Add structured monetary facts only
where supported. Keep rules, reconciliation, risk and anomaly logic unchanged.
If an essential engine key or tolerance must be exposed, use a narrowly scoped
shared helper; do not build a registry, matching framework or import dependency
from detection into the UI. Freeze the case contract before styling.

Gate: grouping safety, source traceability, deterministic IDs, monetary checks
and baseline detection parity pass.

### Slice 2: API And One End-To-End Case

Add an explicit `review_cases` response rather than silently changing the meaning
of existing payment records. Keep `transactions` and old `cases` temporarily for
compatibility. Add API tests, then update frontend types and render one genuine
duplicate case with all payments and its calculation. No duplicate case-building
logic in React. Remove transitional payload fields only after all consumers and
tests migrate, and only if doing so is safe for the deployed API.

Gate: a real upload maps, analyses and opens the grouped case with no lost finding.

### Slice 3: Queue And Investigation Layout

Replace the main-task modal with the nonmodal case workspace. Reuse the existing
queue virtualiser and accessible controls where appropriate. Move payment detail
and calculation to the primary area; preserve reduced-motion support. Compare
1280x800, 1440x900 and 1920x1080 captures before polishing visual details.

Gate: reviewer can find the reason and compare every payment without opening a
secondary dialog or copying IDs elsewhere; no truncation or overlapping controls.

### Slice 4: Navigation, Review State And Export

Switch decisions to case IDs, implement draft preservation, filtered navigation,
focus restoration and case-aware counts. Export a case summary CSV and a member
payment CSV with stable case IDs, source rows, dates, findings and case decisions.
Provide them through a small export menu rather than a new workbook system.
Retain formula-injection protection and explicitly label review exports as reports,
not full-population source imports. Do not imply export/import resume support.

Gate: a reviewer can investigate, record a decision, undo it, continue, return and
export without lost notes, duplicate amounts or mismatched decisions.

### Slice 5: Usability And Scale Iteration

Run the test matrix below, measure performance, and observe representative users.
Fix the largest task failures first. Update README/frontend documentation to match
shipped behavior; mark remaining plan items honestly. Commit only after approval.

## 9. Test Matrix

Write focused failing tests before each behavioral slice, implement the minimum,
then rerun focused and full regressions. Do not rewrite detection expectations to
make the presentation tests pass.

### Pure-Python And Contract Tests

| Scenario | Expected outcome |
| --- | --- |
| Reciprocal two-payment duplicate alerts | One case; two members; every original finding traceable. |
| Compatible A-B and B-C duplicate relationships | One three-member investigation; no repeated payment in totals. |
| Same invoice text, different vendors or currencies | No accidental merge. |
| Leading-zero/punctuation variants already linked by engine | One case; original references preserved for display and search. |
| Vendor ID/name variation | Use current engine-established identity; no frontend name inference. |
| Repeated transaction IDs on different source rows | Every source row retained; ambiguous links never arbitrarily resolved. |
| Duplicate plus large-payment finding on one member | One coherent case with both reasons and no summed row-risk score. |
| Shared row across incompatible relationship families | Separate related cases; decisions isolated. |
| Concentration/balance related fields | No bogus transaction-ID matches or giant connected component. |
| Missing external peer or file-level finding | Visible unresolved evidence/file case, never silent loss. |
| Invoice 2,500; payments 2,500 and 2,480 | Paid 4,980; potential excess 2,480, not a confirmed loss. |
| Invoice 9,800; payments 4,850 and 4,950 | Paid 9,800; remaining zero; no invented overpayment. |
| Inconsistent totals, invalid amounts, mixed currencies, refunds | No unsupported invoice-excess claim; uncertainty visible. |
| Many findings reference the same payments | Amount counted once per source row; findings retained. |
| Same inputs twice / row-order variation with stable provenance | Same case membership and IDs within the run. |
| Threshold/model-cutoff change in UI | Existing audit results and case identity unchanged; supporting model visibility only changes. |
| Model-only payment and model flag inside an audit case | No duplicated case for a member; original individual scores available. |

### Browser And Accessibility Tests

- Open one case with a single click or Enter on its explicit control; no required
  double-click, hover-only action or row checkbox.
- Search each member ID and original invoice variant; find one complete case.
- Combine status, currency, priority and finding filters; clear them predictably.
- Sort with stable ties; preserve order when opening and returning from detail.
- Next/Previous follows filtered results; first/last and empty states are correct.
- Mark Reviewed under Open filter, inspect feedback, continue, then Undo.
- Type an unsaved note, change case, return: draft intact. Saved note and export
  agree. A second case sharing a payment must not inherit the first case decision.
- Open a member's source fields without leaving the case; inspect a group of
  at least 100 linked rows without a silent display cap.
- Keyboard-only review, visible focus, correctly named controls, screen-reader
  spot checks and no focus hidden under sticky bars. Distinguish selected state
  from keyboard focus. Prefer native semantics over a custom grid.
- At browser zoom 200%, content reflows and important values remain accessible.
- Test light/dark appearances, reduced motion and long/blank/non-ASCII values.
- Axe checks on queue, case and dialogs; screenshots of actual engine results;
  no console errors; no inline HTML leakage or horizontal page overflow at target
  desktop widths. Mobile is a basic non-regression check only.
- Export filtered cases with all their members, stable case IDs, correct currencies
  and review state. Check formula-leading user text is safely escaped.
- Loading, malformed upload, empty results, unavailable model and failed request
  states must offer a clear recovery, without presenting stale data as new results.

### Performance And Data Benchmarks

- Existing committed 200-row sample and invoice/partial-duplicate tests establish
  detection parity. Level 4 archive is an external evaluation input, not a source
  of feature labels and not committed without separate approval.
- Generate label-free synthetic populations at 10k and 50k rows, constrained to
  the existing 4 MB upload limit, with both many singleton cases and a large linked
  group. Keep ground truth outside detection input. Record hardware, browser,
  row/case/finding counts, payload size and cold/warm runs.
- Measure grouping separately from model fitting and total API latency. Proposed
  local targets: 10k-row grouping under 1 s, 50k under 5 s; p95 search/filter-to-paint
  under 200 ms across repeated interactions on the recorded test machine. These
  are test targets, not promises or production capacity claims.
- Avoid pairwise all-payment scans in rendering/grouping. Build indexes once per
  run and do not refit ML or regroup when selecting/filtering cases.
- If serialization, payload size or browser memory becomes the bottleneck, record
  the evidence and scope a separate pagination/server-filtering change. Do not
  introduce that infrastructure pre-emptively or increase upload limits silently.

### Commands At Implementation Gates

```powershell
.\.venv\Scripts\python.exe -B -m pytest tests/test_cases.py -q
.\.venv\Scripts\python.exe -B -m pytest -q
.\.venv\Scripts\python.exe -B -c "from transaction_audit.api import app; print('API import ok')"
npm --prefix frontend run build
npm --prefix frontend test
```

`test_cases.py` now covers the implemented case builder. Browser tests require the local API
and frontend servers described in `frontend/README.md`. Test execution may require
local access outside the restricted sandbox. Do not call a sandbox hang a product
failure or report tests as passed if they did not finish.

## 10. Formative Review And Iteration

Record the current UI first using the same tasks and fixture as the new design.
Use a finance reviewer and non-technical participants where available. Testers
must not first receive a developer explanation of the layout. Have the answer
key independently checked; synthetic labels are not proof of real-world validity.

Tasks:

1. Find a named invoice and explain why it is in the queue.
2. Identify every linked payment and the potential excess, if supported.
3. Explain why a split-payment pattern is not automatically improper.
4. Identify the actual large-payment review limit and whether it is a proven policy.
5. Locate a second transaction ID that belongs to the same case.
6. Record a note, mark Reviewed, undo it, move to the next case and return.
7. Filter to a currency/finding and export the cases being examined.
8. Explain the difference between an audit finding and a model-only suggestion.

Capture completion, time, assistance, navigation errors, lost context and incorrect
conclusions; do not measure attractiveness as a substitute for task success.
No actual participant results are available at planning time. Agent walkthroughs
and automated checks must be labelled as such, not presented as user research.

Iteration order: correctness/confusing financial conclusions, then task/navigation
failures, then reading hierarchy, then density/performance, then visual polish.
Each cycle records the problem, smallest change, retest and remaining limitation.

## 11. Delivery Checklist

- [x] Inspect the existing UI/API boundaries and browser tests.
- [x] Research primary platform/accessibility guidance and relevant HCI sources.
- [x] Define scope, grouping safeguards, acceptance targets and test approach.
- [x] Build one real duplicate-case vertical slice and capture desktop screenshots; user design feedback remains pending.
- [x] Case builder and contract tests pass.
- [x] Queue and investigation workspace implemented and visually checked.
- [x] Navigation, decisions, note drafts and exports verified.
- [x] Full regressions, automated accessibility, external benchmark and scoped scale checks completed; limits noted below.
- [ ] Formative user feedback gathered, or absence explicitly documented.
- [x] README and frontend documentation updated for implemented branch behavior.
- [ ] User approval obtained before commit, merge or deployment.

## 12. Initial Implementation Verification

- Python: 182 tests and 13 subtests passed. One existing Starlette/httpx test-client
  deprecation warning remains. API import and production TypeScript/Vite build pass.
- Browser: nine scenarios passed, including keyboard opening/focus restoration,
  note drafts, review/Undo, filters, uploads, focus mode, exports, and automated
  WCAG A/AA scans in light and dark appearances. A subsequent focused export test
  also verifies supported invoice calculations occur once in the case summary.
- Screenshots inspected at 1280x800, 1440x900 and 1920x1080, plus a basic narrow
  fallback check. These are engineering checks, not representative-user testing.
- External Level 4 input: 200 rows, 70 existing findings/flagged payment rows,
  45 review cases, 25 multi-payment cases. Every original finding and member's
  existing audit/model output is retained. The 70-row baseline already includes
  the earlier vendor-matching fix; do not compare it to the older 68-row report as
  though this presentation branch changed detection. User data was not committed.
- Exact invoice-calculation tests include excess settlement, exact reconciliation,
  decimal arithmetic, missing values, conflicting invoice totals and currencies.
- A 100-payment synthetic invoice exposes all 100 members in one case, with no
  six-payment truncation. Its complete comparison remains accessible by scrolling.
- Case builder, synthetic all-singleton workload, Python 3.11.1 on this Windows
  Intel64 machine: 200 rows 0.003 s; 10k rows 0.199 s; 50k rows 1.246 s. These are
  one-shot grouping measurements, not end-to-end upload/model/serialization times.
- A separate 10k-case browser-only fixture remains virtualised (fewer than 50
  rendered case buttons). Five measured search fill-to-result assertions took
  135-162 ms in the final full run, including Playwright round trips; this is not
  a statistically robust p95 or a direct browser-paint measurement.
- No full 50k-row browser upload capacity claim, independent screen-reader audit,
  or participant-based usability claim is made. Session work is still lost on
  reload unless exported. Manual review by the user is the next feedback gate.

### Desktop Readability Iteration

Following user feedback, the full queue now separates payee/payment, reason and
invoice/date into balanced columns instead of one oversized title column. Main
queue rows are 72px, primary text is 14px, and search/filter controls are 40px.
Search, source filters and sorting are grouped together. The compact navigator
keeps larger text and uses taller rows. Detection and priority settings are unchanged.
Ten browser tests and the production build pass after this iteration, including
layout assertions and inspected screenshots at 1280px and 1920px widths.

### Pre-Commit ZIP Regression

The Level 4 transaction CSV was uploaded through `/api/inspect` and `/api/analyse`
twice. Both runs were identical: 200 transactions, 70 original findings, 45 review
cases, 25 multi-payment cases and 10 model-flagged payments. Every original finding
and member audit/model output was preserved. Separate ground truth was used only
after analysis: all 30 targeted review cases had a deterministic finding. This is
review-case coverage, not proof of invalid payments or perfect precision: the
previously identified flags on approved advances, high-value controls and scheduled
instalments remain. The automatic High priority for reconciled instalments and the
other wording/scoring concerns identified in review are deliberately not changed
by this presentation branch. They need a separately tested correction.

### Corrective Pass - 2026-11-05

The uncommitted follow-up now addresses the focused code/clarity review:

- Instalment observations contribute 10 low-priority points, rather than 75 high
  points. Higher-priority coexisting findings still take precedence. No rule
  observations are suppressed and no supplied approval flag is treated as verified.
- Cases expose a typed primary finding shared by their title and explanation,
  plus a presentation category such as Data correction or Settlement information.
- Blank-reference, uploaded-vendor and configured-currency wording no longer
  implies missing documents, unapproved vendors or a verified audit policy.
- Imported-row labels explain normalisation; unknown currency no longer falls
  back to GBP. Exports include the presentation category.
- Queue filtering/sorting moved into a pure frontend helper; an unused title
  helper and about 1,850 obsolete stylesheet lines were removed. Shared modal,
  upload and accessibility styles remain. Case records have TypedDict contracts
  and share a public invoice-key helper with the detection code.
- ML training/features are unchanged. Context-threshold wording no longer
  promises an exact top-5% classification, and the UI explicitly states that
  context notes are not feature-attribution measurements or currency conversions.
- Verification: 184 Python tests and 13 subtests; 12 browser scenarios; production
  build. External ZIP still produces 70 findings and 45 cases, including 10
  instalment-only cases now Low, with all findings retained and 10 model flags.

Approval/schedule-aware evaluation, currency-aware ML and representative-user
testing remain separate work. This correction is not a claim that every earlier
product limitation has been solved. Nothing is merged or deployed by this pass.
