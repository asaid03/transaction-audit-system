FROM node:22-bookworm-slim AS frontend
WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim-bookworm AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000
WORKDIR /app
COPY requirements.txt requirements-web.txt ./
RUN pip install --no-cache-dir -r requirements-web.txt
COPY transaction_audit/ ./transaction_audit/
COPY sample_data/reduced_transactions_200_with_anomalies.csv ./sample_data/reduced_transactions_200_with_anomalies.csv
COPY --from=frontend /build/frontend/dist ./frontend/dist
RUN useradd --create-home --uid 10001 audit
USER audit
EXPOSE 8000
CMD ["python", "-m", "transaction_audit.serve"]
