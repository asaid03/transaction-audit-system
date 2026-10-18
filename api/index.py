"""Expose the audit API through Vercel's Python runtime."""

from transaction_audit.api import app

__all__ = ["app"]
