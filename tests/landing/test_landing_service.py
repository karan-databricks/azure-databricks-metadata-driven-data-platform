# tests/landing/test_landing_service.py

from unittest.mock import MagicMock

from enterprise_api_ingestion_platform.landing.landing_service import (
    LandingService,
)
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata
from enterprise_api_ingestion_platform.models.landing_result import LandingResult
from enterprise_api_ingestion_platform.utils.http_client import HttpClient


def create_metadata() -> ApiMetadata:
    return ApiMetadata(
        api_id="orders-api",
        api_name="orders",
        endpoint="https://example.com/orders",
        pagination_type="NONE",
        load_type="FULL",
    )


def test_ingest_without_pagination_writes_payload() -> None:
    http_client = MagicMock(spec=HttpClient)
    landing_writer = MagicMock()

    payload = [
        {"id": 1, "status": "COMPLETE"},
        {"id": 2, "status": "PENDING"},
    ]

    http_client.request.return_value = payload
    landing_writer.write.return_value = "/landing/orders"
    landing_writer.get_file_size.return_value = 1234

    service = LandingService(
        http_client=http_client,
        landing_writer=landing_writer,
    )

    result = service.ingest(create_metadata())

    assert isinstance(result, LandingResult)
    assert result.landing_file == "/landing/orders"
    assert result.checkpoint_state is None
    assert result.records_read == 2
    assert result.pages_read == 1
    assert result.landing_file_size_bytes == 1234

    http_client.request.assert_called_once_with(
        method="GET",
        url="https://example.com/orders",
        headers={},
        params={},
    )

    landing_writer.write.assert_called_once_with(
        metadata=create_metadata(),
        payload=payload,
    )

    landing_writer.get_file_size.assert_called_once_with(
        file_path="/landing/orders",
    )


def test_ingest_with_cursor_pagination_follows_pages() -> None:
    http_client = MagicMock(spec=HttpClient)
    landing_writer = MagicMock()

    http_client.request.side_effect = [
        {
            "data": [{"id": 1}],
            "next_cursor": "cursor-a",
        },
        {
            "data": [{"id": 2}],
            "next_cursor": "cursor-b",
        },
        {
            "data": [{"id": 3}],
        },
    ]

    landing_writer.write.return_value = "/landing/orders"
    landing_writer.get_file_size.return_value = 1234

    metadata = ApiMetadata(
        api_id="orders-api",
        api_name="orders",
        endpoint="https://example.com/orders",
        pagination_type="CURSOR",
        cursor_field="next_cursor",
        cursor_parameter="cursor",
        load_type="INCREMENTAL",
        query_parameters={
            "status": "active",
        },
    )

    service = LandingService(
        http_client=http_client,
        landing_writer=landing_writer,
    )

    result = service.ingest(metadata)

    assert result == LandingResult(
        landing_file="/landing/orders",
        checkpoint_state="cursor-b",
        records_read=3,
        pages_read=3,
        landing_file_size_bytes=1234,
    )

    assert http_client.request.call_count == 3

    assert http_client.request.call_args_list[0].kwargs["params"] == {
        "status": "active",
    }

    assert http_client.request.call_args_list[1].kwargs["params"] == {
        "status": "active",
        "cursor": "cursor-a",
    }

    assert http_client.request.call_args_list[2].kwargs["params"] == {
        "status": "active",
        "cursor": "cursor-b",
    }

    landing_writer.write.assert_called_once_with(
        metadata=metadata,
        payload=[
            {
                "data": [{"id": 1}],
                "next_cursor": "cursor-a",
            },
            {
                "data": [{"id": 2}],
                "next_cursor": "cursor-b",
            },
            {
                "data": [{"id": 3}],
            },
        ],
    )

    landing_writer.get_file_size.assert_called_once_with(
        file_path="/landing/orders",
    )


def test_ingest_with_initial_cursor_uses_cursor_on_first_request() -> None:
    http_client = MagicMock(spec=HttpClient)
    landing_writer = MagicMock()

    http_client.request.return_value = {
        "data": [{"id": 1}],
    }

    landing_writer.write.return_value = "/landing/orders"
    landing_writer.get_file_size.return_value = 1234

    metadata = ApiMetadata(
        api_id="orders-api",
        api_name="orders",
        endpoint="https://example.com/orders",
        pagination_type="CURSOR",
        cursor_field="next_cursor",
        cursor_parameter="cursor",
        load_type="INCREMENTAL",
        query_parameters={
            "status": "active",
        },
    )

    service = LandingService(
        http_client=http_client,
        landing_writer=landing_writer,
    )

    result = service.ingest(
        metadata,
        initial_cursor="cursor-previous",
    )

    assert result == LandingResult(
        landing_file="/landing/orders",
        checkpoint_state=None,
        records_read=1,
        pages_read=1,
        landing_file_size_bytes=1234,
    )

    http_client.request.assert_called_once_with(
        method="GET",
        url="https://example.com/orders",
        headers={},
        params={
            "status": "active",
            "cursor": "cursor-previous",
        },
    )

    landing_writer.write.assert_called_once_with(
        metadata=metadata,
        payload={
            "data": [{"id": 1}],
        },
    )

    landing_writer.get_file_size.assert_called_once_with(
        file_path="/landing/orders",
    )