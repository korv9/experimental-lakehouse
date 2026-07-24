"""CLI and Python API for exploring HTTP endpoints without starting Spark."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import requests
import yaml

from lakehouse_platform.observability.progress import progress

SENSITIVE_HEADERS = {
    "authorization",
    "cookie",
    "proxy-authorization",
    "set-cookie",
    "x-api-key",
    "x-auth-token",
}
SENSITIVE_PARAMETERS = {
    "access_token",
    "api_key",
    "apikey",
    "key",
    "password",
    "secret",
    "token",
}
ENVIRONMENT_VARIABLE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass(frozen=True)
class ApiRequest:
    """Complete description of one exploratory HTTP request."""

    name: str
    url: str
    method: str = "GET"
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    json_body: Any = None
    data: str | None = None
    timeout: float = 30.0
    verify_ssl: bool = True


@dataclass(frozen=True)
class ApiResponse:
    """Response metadata plus parsed and raw response bodies."""

    status_code: int
    elapsed_ms: int
    headers: dict[str, str]
    body: Any
    content: bytes

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 400


def mask_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return headers safe to print in a terminal or notebook."""
    return {
        key: "***" if key.lower() in SENSITIVE_HEADERS else value
        for key, value in headers.items()
    }


def mask_parameters(parameters: Mapping[str, Any]) -> dict[str, Any]:
    """Mask common credentials when APIs place them in query parameters."""
    return {
        key: "***" if key.lower() in SENSITIVE_PARAMETERS else value
        for key, value in parameters.items()
    }


def _expand_environment(value: Any) -> Any:
    if isinstance(value, str):

        def replace_variable(match: re.Match[str]) -> str:
            variable = match.group(1)
            if variable not in os.environ:
                raise ValueError(f"Environment variable {variable!r} is not set")
            return os.environ[variable]

        return ENVIRONMENT_VARIABLE.sub(replace_variable, value)
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_environment(item) for key, item in value.items()}
    return value


def load_request(path: str | Path, endpoint: str | None = None) -> ApiRequest:
    """Load one named endpoint from YAML and expand ``${ENV_VAR}`` values."""
    config_path = Path(path)
    document = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    endpoints = document.get("endpoints")
    if not isinstance(endpoints, dict) or not endpoints:
        raise ValueError(f"{config_path} must contain a non-empty 'endpoints' mapping")

    selected = endpoint
    if selected is None:
        if len(endpoints) != 1:
            names = ", ".join(sorted(endpoints))
            raise ValueError(f"Choose an endpoint with --endpoint. Available: {names}")
        selected = next(iter(endpoints))
    if selected not in endpoints:
        names = ", ".join(sorted(endpoints))
        raise ValueError(f"Unknown endpoint {selected!r}. Available: {names}")

    values = _expand_environment(endpoints[selected] or {})
    if not isinstance(values, dict) or "url" not in values:
        raise ValueError(f"Endpoint {selected!r} must define a URL")
    return ApiRequest(
        name=selected,
        url=str(values["url"]),
        method=str(values.get("method", "GET")).upper(),
        params=dict(values.get("params", {})),
        headers={str(key): str(value) for key, value in values.get("headers", {}).items()},
        json_body=values.get("json"),
        data=values.get("data"),
        timeout=float(values.get("timeout", 30)),
        verify_ssl=bool(values.get("verify_ssl", True)),
    )


def execute_request(
    request: ApiRequest,
    *,
    session: requests.Session | None = None,
) -> ApiResponse:
    """Execute a request and print compact, secret-safe progress events."""
    client = session or requests.Session()
    progress(
        "api",
        "Sending request",
        name=request.name,
        method=request.method,
        url=request.url,
    )
    if request.params:
        progress("api", "Query parameters", values=mask_parameters(request.params))
    if request.headers:
        progress("api", "Request headers", values=mask_headers(request.headers))
    if not request.verify_ssl:
        progress("api", "Warning: TLS certificate verification is disabled")

    started = time.perf_counter()
    response = client.request(
        method=request.method,
        url=request.url,
        params=request.params or None,
        headers=request.headers or None,
        json=request.json_body,
        data=request.data,
        timeout=request.timeout,
        verify=request.verify_ssl,
    )
    elapsed_ms = round((time.perf_counter() - started) * 1000)

    try:
        body = response.json()
    except (requests.exceptions.JSONDecodeError, json.JSONDecodeError, ValueError):
        body = response.text

    result = ApiResponse(
        status_code=response.status_code,
        elapsed_ms=elapsed_ms,
        headers=dict(response.headers),
        body=body,
        content=response.content,
    )
    progress(
        "api",
        "Response received",
        status=result.status_code,
        ok=result.ok,
        elapsed_ms=result.elapsed_ms,
        bytes=len(result.content),
    )
    return result


def format_preview(body: Any, max_characters: int = 2_000) -> str:
    """Format JSON readably and cap terminal output without changing saved data."""
    rendered = (
        body
        if isinstance(body, str)
        else json.dumps(body, indent=2, ensure_ascii=False, default=str)
    )
    if len(rendered) <= max_characters:
        return rendered
    omitted = len(rendered) - max_characters
    return f"{rendered[:max_characters]}\n... ({omitted} characters omitted)"


def save_response(response: ApiResponse, path: str | Path) -> Path:
    """Save the exact response bytes, creating the requested parent folders."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(response.content)
    progress("api", "Raw response saved", path=target, bytes=len(response.content))
    return target


def _key_value(values: Sequence[str], option: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"{option} expects KEY=VALUE, received {value!r}")
        key, item = value.split("=", 1)
        if not key:
            raise ValueError(f"{option} requires a non-empty key")
        parsed[key] = item
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lakehouse-api",
        description="Test API endpoints without starting Spark.",
    )
    parser.add_argument("url", nargs="?", help="URL for an ad-hoc request")
    parser.add_argument("--config", type=Path, help="YAML file with named endpoints")
    parser.add_argument("--endpoint", help="Endpoint name inside --config")
    parser.add_argument("--method", help="HTTP method; defaults to GET")
    parser.add_argument("--param", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--header", action="append", default=[], metavar="KEY=VALUE")
    body = parser.add_mutually_exclusive_group()
    body.add_argument("--json", dest="json_text", help="JSON request body")
    body.add_argument("--data", help="Raw string request body")
    parser.add_argument("--timeout", type=float, help="Timeout in seconds")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification")
    parser.add_argument("--save", type=Path, help="Save the exact response body")
    parser.add_argument("--max-preview", type=int, default=2_000)
    parser.add_argument(
        "--show-response-headers",
        action="store_true",
        help="Print response headers",
    )
    return parser


def request_from_args(args: argparse.Namespace) -> ApiRequest:
    if args.config:
        request = load_request(args.config, args.endpoint)
        if args.url:
            raise ValueError("Provide either a URL or --config, not both")
    elif args.url:
        request = ApiRequest(name="ad-hoc", url=args.url)
    else:
        raise ValueError("Provide a URL or --config")

    json_body = request.json_body
    if args.json_text is not None:
        try:
            json_body = json.loads(args.json_text)
        except json.JSONDecodeError as error:
            raise ValueError(f"--json is not valid JSON: {error.msg}") from error

    return replace(
        request,
        method=(args.method or request.method).upper(),
        params={**request.params, **_key_value(args.param, "--param")},
        headers={**request.headers, **_key_value(args.header, "--header")},
        json_body=json_body,
        data=args.data if args.data is not None else request.data,
        timeout=args.timeout if args.timeout is not None else request.timeout,
        verify_ssl=False if args.insecure else request.verify_ssl,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        request = request_from_args(args)
        response = execute_request(request)
    except (OSError, ValueError, requests.RequestException) as error:
        print(f"API request failed: {error}", file=sys.stderr)
        return 2

    if args.show_response_headers:
        print(json.dumps(mask_headers(response.headers), indent=2, ensure_ascii=False))
    print(format_preview(response.body, args.max_preview))
    if args.save:
        save_response(response, args.save)
    return 0 if response.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
