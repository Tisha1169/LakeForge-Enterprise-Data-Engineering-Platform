from unittest.mock import MagicMock, patch

import pytest
import requests
from config.sources import SourceConfig
from pipelines.ingestion.api.customer_api import CustomerApiIngestion


def _config() -> SourceConfig:
    return SourceConfig(name="customers", source_type="api", endpoint="/customers", page_size=2)


def _response(data, has_next, status=200):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = {
        "data": data,
        "page": 1,
        "page_size": 2,
        "total": len(data),
        "has_next": has_next,
    }
    resp.raise_for_status = MagicMock()
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    return resp


def test_extract_paginates_until_has_next_false():
    page1 = _response([{"customer_id": 1}, {"customer_id": 2}], has_next=True)
    page2 = _response([{"customer_id": 3}], has_next=False)

    with patch(
        "pipelines.ingestion.api.customer_api.requests.get", side_effect=[page1, page2]
    ) as mock_get:
        ingestion = CustomerApiIngestion(_config())
        records = list(ingestion.extract())

    assert records == [{"customer_id": 1}, {"customer_id": 2}, {"customer_id": 3}]
    assert mock_get.call_count == 2


def test_extract_retries_on_transient_connection_error_then_succeeds():
    ok_response = _response([{"customer_id": 1}], has_next=False)

    with patch(
        "pipelines.ingestion.api.customer_api.requests.get",
        side_effect=[requests.exceptions.ConnectionError("refused"), ok_response],
    ) as mock_get:
        ingestion = CustomerApiIngestion(_config())
        records = list(ingestion.extract())

    assert records == [{"customer_id": 1}]
    assert mock_get.call_count == 2


def test_extract_does_not_retry_on_client_error():
    bad_response = _response([], has_next=False, status=404)

    with patch(
        "pipelines.ingestion.api.customer_api.requests.get", return_value=bad_response
    ) as mock_get:
        ingestion = CustomerApiIngestion(_config())
        with pytest.raises(requests.exceptions.HTTPError):
            list(ingestion.extract())

    assert mock_get.call_count == 1
