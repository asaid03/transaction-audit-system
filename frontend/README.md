# Investigation Workspace

The first React design slice: a working investigation queue and case panel using
the existing Python audit engine. The separate Streamlit app still runs as before.

## Run locally

From the repository root, install API dependencies into the existing environment:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-web.txt
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

The root `Dockerfile` builds React and bundles its compiled assets with the Python
API in one service. Relative `/api` requests work on the same public domain, with
no separate frontend hosting or CORS configuration required.

On Railway:

1. Publish the `review-ui` branch containing this work to GitHub.
2. Create a service from `asaid03/transaction-audit-system`, using `review-ui`.
3. Keep the service root at the repository root, where `Dockerfile` and
   `railway.json` live. Railway uses the Docker build and `/api/health` check.
4. Generate a domain in the service's Networking settings to get the shareable
   HTTPS link. Use the port supplied by Railway; the server reads `PORT`.
5. Leave Serverless disabled when you want the demonstration to remain running.

The app runs in the cloud independently of your computer. It opens the committed
synthetic sample and accepts transaction uploads. Uploaded data is processed on
the server; it is not stored in an application database. Review decisions/notes
remain in browser memory and are lost on refresh unless exported.

This deployment has no sign-in. A public domain is suitable for synthetic-data
demonstrations; access control is a separate step before private client use.

To check the combined production service locally after `npm run build`:

```powershell
# From the repository root; defaults to port 8000.
.\.venv\Scripts\python.exe -m transaction_audit.serve
```

Open http://localhost:8000. This uses built React assets directly from FastAPI,
without the Vite development server. Docker is optional for this local check.

## Verify

```powershell
npm run build
npx playwright install chromium
npm test
```

The browser tests require both local servers. Screenshots and traces go to the
ignored `test-results/` directory. Python integration tests are in `tests/test_api.py`.
