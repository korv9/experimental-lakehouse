from __future__ import annotations

import json

import pytest

from lakehouse_platform.tools.api_explorer import (
    ApiRequest,
    build_parser,
    execute_request,
    format_preview,
    load_request,
    mask_headers,
    mask_parameters,
    request_from_args,
)


class FakeResponse:
    status_code = 200
    headers = {"Content-Type": "application/json", "X-Request-Id": "abc"}
    content = b'{"items": [1, 2]}'
    text = content.decode()

    def json(self):
        return json.loads(self.text)


class FakeSession:
    def __init__(self):
        self.arguments = None

    def request(self, **arguments):
        self.arguments = arguments
        return FakeResponse()


def test_execute_request_supports_methods_bodies_and_masks_progress(capsys):
    session = FakeSession()
    request = ApiRequest(
        name="create",
        method="POST",
        url="https://api.example.test/items",
        headers={"Authorization": "Bearer secret", "Accept": "application/json"},
        json_body={"name": "demo"},
    )

    response = execute_request(request, session=session)

    assert response.ok
    assert response.body == {"items": [1, 2]}
    assert session.arguments["method"] == "POST"
    assert session.arguments["json"] == {"name": "demo"}
    output = capsys.readouterr().out
    assert "Bearer secret" not in output
    assert "'Authorization': '***'" in output


def test_loads_named_endpoint_and_expands_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("API_TEST_TOKEN", "secret-value")
    config = tmp_path / "endpoints.yaml"
    config.write_text(
        """
endpoints:
  items:
    url: https://api.example.test/items
    headers:
      X-API-Key: ${API_TEST_TOKEN}
    params:
      page: 2
""",
        encoding="utf-8",
    )

    request = load_request(config, "items")

    assert request.headers == {"X-API-Key": "secret-value"}
    assert request.params == {"page": 2}


def test_missing_environment_variable_is_an_actionable_error(tmp_path, monkeypatch):
    monkeypatch.delenv("MISSING_API_TOKEN", raising=False)
    config = tmp_path / "endpoints.yaml"
    config.write_text(
        """
endpoints:
  secured:
    url: https://api.example.test
    headers:
      Authorization: Bearer ${MISSING_API_TOKEN}
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="MISSING_API_TOKEN"):
        load_request(config, "secured")


def test_cli_values_override_config_values(tmp_path):
    config = tmp_path / "endpoints.yaml"
    config.write_text(
        """
endpoints:
  items:
    url: https://api.example.test/items
    method: GET
    params:
      page: 1
""",
        encoding="utf-8",
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "--config",
            str(config),
            "--endpoint",
            "items",
            "--method",
            "POST",
            "--param",
            "page=3",
            "--json",
            '{"name": "demo"}',
        ]
    )

    request = request_from_args(args)

    assert request.method == "POST"
    assert request.params == {"page": "3"}
    assert request.json_body == {"name": "demo"}


def test_preview_is_limited_and_sensitive_headers_are_masked():
    assert "omitted" in format_preview({"value": "abcdefghij"}, max_characters=8)
    assert mask_headers(
        {"Authorization": "secret", "X-API-Key": "key", "Accept": "json"}
    ) == {"Authorization": "***", "X-API-Key": "***", "Accept": "json"}
    assert mask_parameters({"api_key": "secret", "page": 2}) == {
        "api_key": "***",
        "page": 2,
    }
