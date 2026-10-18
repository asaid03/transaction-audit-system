"""Start the combined API and built React application on the host's assigned port."""

import os

import uvicorn


def main() -> None:
    uvicorn.run(
        "transaction_audit.api:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
    )


if __name__ == "__main__":
    main()
