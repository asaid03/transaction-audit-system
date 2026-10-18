# Investigation Workspace

The first React design slice: a working investigation queue and case panel using
the existing Python audit engine. The separate Streamlit app still runs as before.

## Run locally

From the repository root, install Python dependencies into the existing environment:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m uvicorn transaction_audit.api:app --host 127.0.0.1 --port 8001
```

In a second terminal:

```powershell
cd frontend
npm ci
npm run dev
```

Open http://127.0.0.1:5173. The queue initially runs the committed 200-row synthetic
benchmark through validation and Isolation Forest. Evaluation labels are removed.
Use **New review** to upload a CSV or XLSX and confirm required/optional mappings.
Mapping a column twice is rejected. Files are processed in memory.

## Design

- Neutral light/dark surfaces, blue for actions, semantic priority colours.
- System text, monospace IDs, tabular money, compact sortable virtualised table.
- One row per flagged transaction. Audit priority and model score stay separate.
- Actual findings link to original payments and source row numbers.
- Radix provides modal focus handling, Escape, and accessible dialog names.
- Motion provides spring panels; mobile drag projects release velocity and uses
  elastic bounds. Reduced motion crossfades panels instead of sliding them.
- Review decisions, notes and timestamped actions are session-only. Undo reverses
  the last action. CSV export contains the current filtered review with decisions.
- The model cutoff filters existing flagged model scores; it does not retrain or
  alter deterministic checks. Scores are not fraud probabilities.

No relationship graph: identifiers in one transaction file do not establish a
verified entity network. No invented supplier names, review history, or findings.
Reference-file upload and dedicated remaining screens await design feedback.

## API contract

| Endpoint | Input | Output |
| --- | --- | --- |
| `GET /api/health` | none | `{status}` |
| `GET /api/demo` | none | `AuditRun` from the committed synthetic sample |
| `POST /api/inspect` | multipart `file` | source columns, sample, auto-mapping, missing required fields |
| `POST /api/analyse` | multipart `file`, JSON `mapping` | `AuditRun` from existing engine |

`mapping` maps canonical field names to source column names. `AuditRun` includes
filename, source row count, applied mapping, cleaning steps, all transactions and
flagged cases. Each case contains a source row key, source fields, audit score,
audit band, findings with evidence/related IDs, and an independent `model` object
with score, flag, decision and contextual drivers. No score is recomputed in JS.

`GET /docs` on port 8001 provides the generated endpoint documentation. Review
storage and authentication are not added in this slice. Development servers bind
to localhost; the production service uses the hosting platform's assigned port.

## Deploy a shareable version

The project deploys to Vercel as a static React build backed by one FastAPI Python
Function. Relative `/api` requests stay on the same public domain, so no separate
backend service or CORS configuration is required.

On Vercel:

1. Import `asaid03/transaction-audit-system` from GitHub and select `review-ui`.
2. Keep the project root at the repository root. `vercel.json` builds the React
   application, `requirements.txt` supplies Python dependencies, and
   `api/index.py` exposes the existing FastAPI application.
3. Select the free Hobby plan and deploy. No environment variables are required.
4. Use the generated `*.vercel.app` domain as the shareable demonstration link.

The app runs in the cloud independently of your computer. It opens the committed
synthetic sample and accepts transaction uploads. Uploaded data is processed on
the server; it is not stored in an application database. Review decisions/notes
remain in browser memory and are lost on refresh unless exported.

This deployment has no sign-in. A public domain is suitable for synthetic-data
demonstrations; access control is a separate step before private client use.
Uploads are limited to 4 MB because Vercel Functions cap request bodies at 4.5 MB.

To check the combined service locally after `npm run build`:

```powershell
# From the repository root.
.\.venv\Scripts\python.exe -m uvicorn transaction_audit.api:app --host 127.0.0.1 --port 8000
```

Open http://localhost:8000. This uses built React assets directly from FastAPI,
without the Vite development server.

## Verify

```powershell
npm run build
npx playwright install chromium
npm test
```

The browser tests require both local servers. Screenshots and traces go to the
ignored `test-results/` directory. Python integration tests are in `tests/test_api.py`.
