"""Minimal mock of a Customer REST API for local development.

Serves paginated customer records from a static JSON file, so the ingestion
layer exercises real HTTP pagination/retry logic against something that
behaves like a genuine third-party API — not a local file read.
"""

import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="OpenLake Mock Customer API")

DATA_PATH = Path(os.environ.get("CUSTOMER_DATA_PATH", "/data/customers.json"))
_customers: list[dict] = json.loads(DATA_PATH.read_text())


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "record_count": len(_customers)}


@app.get("/customers")
def list_customers(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
) -> dict:
    if page < 1:
        raise HTTPException(status_code=400, detail="page must be >= 1")

    start = (page - 1) * page_size
    end = start + page_size
    page_data = _customers[start:end]

    return {
        "data": page_data,
        "page": page,
        "page_size": page_size,
        "total": len(_customers),
        "has_next": end < len(_customers),
    }
