# Transaction Testing Phase 1

Small Phase 1 starter for automated financial transaction testing.

## What It Does

- Loads transactions from CSV or Excel files.
- Infers common column mappings and allows schema overrides.
- Validates required fields and basic data quality.
- Flags duplicate transaction IDs.
- Produces an audit-ready validation summary.
- Provides a basic Streamlit UI for upload and review.

## Project Structure

```text
transaction_audit/
  config.py       Validation settings
  ingestion.py    File loading without schema mutation
  profiles.py     Saved import profile management
  schema.py       Flexible source-to-canonical schema mapping
  validation.py   Phase 1 validation checks
  audit.py        Audit summary generation
app.py            Streamlit frontend
import_profiles/  Saved source-column mapping profiles
sample_data/      Example transaction file
requirements.txt  Python dependencies
```

## Core Boundary

Streamlit is only a thin UI layer. The reusable core flow is:

```python
from transaction_audit.ingestion import load_transactions
from transaction_audit.schema import apply_schema
from transaction_audit.validation import validate_transactions

raw_transactions = load_transactions("transactions.csv")
schema_result = apply_schema(raw_transactions)
validation_result = validate_transactions(schema_result.transactions)
```

That means the validation logic can move behind an API, React frontend, batch worker, or notebook without rewriting the rules.

## Import Profiles

Financial exports do not share one column convention, so the system uses reusable import profiles.

Profiles are JSON files in:

```text
import_profiles/
```

Each profile stores a `source_to_canonical` mapping:

```json
{
  "txn_ref": "transaction_id",
  "date": "transaction_date",
  "acc_id": "account_id",
  "amt": "amount",
  "ccy": "currency",
  "counterparty_name": "counterparty"
}
```

The validation engine still only receives the canonical dataframe. Profiles are just the import layer.

## Required Transaction Fields

The system expects these normalized fields:

- `transaction_id`
- `transaction_date`
- `account_id`
- `amount`
- `currency`
- `counterparty`

Common alternatives such as `id`, `date`, `acct`, `value`, `ccy`, and `vendor` are normalized automatically.

## Run Locally

Clone the repo and enter the project folder:

```powershell
git clone https://github.com/asaid03/transaction-audit-system.git
cd transaction-audit-system
```

Create and activate a local virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Run the app:

```powershell
streamlit run app.py
```

If your shell does not have the virtual environment activated, run commands through the environment Python:

```powershell
.\.venv\Scripts\python.exe -m streamlit run app.py
```

Or test the core validator with the sample file:

```powershell
.\.venv\Scripts\python.exe -m transaction_audit.validation sample_data/sample_transactions.csv
```

Run the regression tests:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -v
```
