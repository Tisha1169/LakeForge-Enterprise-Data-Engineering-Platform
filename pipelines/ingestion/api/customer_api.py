"""Ingests customer records from the Customer API (paginated), with retry
on transient failures (timeouts, connection errors, 5xx)."""

from __future__ import annotations

from collections.abc import Iterator

import requests
from config.settings import settings
from config.sources import SourceConfig
from monitoring.logging_config import get_logger
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from pipelines.ingestion.base import BaseIngestion

logger = get_logger(__name__)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, requests.exceptions.ConnectionError | requests.exceptions.Timeout):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        response = exc.response
        return response is not None and response.status_code >= 500
    return False


class CustomerApiIngestion(BaseIngestion):
    def __init__(self, config: SourceConfig, batch_date=None):
        super().__init__(config, batch_date)
        self._base_url = settings.customer_api_base_url.rstrip("/")

    def extract(self) -> Iterator[dict]:
        page = 1
        while True:
            payload = self._fetch_page(page)
            yield from payload["data"]
            if not payload["has_next"]:
                break
            page += 1

    @retry(
        retry=retry_if_exception(_is_transient),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=15),
        reraise=True,
    )
    def _fetch_page(self, page: int) -> dict:
        url = f"{self._base_url}{self.config.endpoint}"
        response = requests.get(
            url,
            params={"page": page, "page_size": self.config.page_size},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
