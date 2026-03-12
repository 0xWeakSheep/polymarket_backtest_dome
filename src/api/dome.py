import json
import os
import time
import urllib.parse
import urllib.request
from typing import Dict, Generator, Iterable, Optional


class DomeAPIError(RuntimeError):
    pass


class DomeClient:
    BASE_URL = "https://api.domeapi.io/v1"

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 5,
        retry_backoff_seconds: float = 1.5,
    ) -> None:
        self.api_key = api_key or os.getenv("DOME_API_KEY")
        if not self.api_key:
            raise DomeAPIError(
                "Missing DOME_API_KEY. Export it first, for example: export DOME_API_KEY=your_key"
            )

        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

    def _request_json(self, path: str, params: Optional[Dict[str, object]] = None) -> Dict[str, object]:
        query = urllib.parse.urlencode(
            [(key, value) for key, value in (params or {}).items() if value is not None],
            doseq=True,
        )
        url = f"{self.BASE_URL}{path}"
        if query:
            url = f"{url}?{query}"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "x-api-key": self.api_key,
            "Accept": "application/json",
            "User-Agent": "polymarket-backtest/0.1",
        }

        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                request = urllib.request.Request(url, headers=headers, method="GET")
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = response.read().decode("utf-8")
                return json.loads(payload)
            except Exception as exc:  # pragma: no cover - network behavior
                last_error = exc
                if attempt == self.max_retries:
                    break
                time.sleep(self.retry_backoff_seconds * attempt)

        raise DomeAPIError(f"Failed request to {url}: {last_error}")

    def paginate(
        self,
        path: str,
        items_key: str,
        params: Optional[Dict[str, object]] = None,
        limit: int = 100,
    ) -> Generator[Dict[str, object], None, None]:
        next_key: Optional[str] = None
        while True:
            page_params = dict(params or {})
            page_params["limit"] = limit
            if next_key:
                page_params["pagination_key"] = next_key

            payload = self._request_json(path, page_params)
            items = payload.get(items_key, [])
            if not isinstance(items, list):
                raise DomeAPIError(f"Unexpected payload shape for {path}: missing list '{items_key}'")

            for item in items:
                if isinstance(item, dict):
                    yield item

            pagination = payload.get("pagination", {})
            if not isinstance(pagination, dict) or not pagination.get("has_more"):
                break

            next_key = pagination.get("pagination_key")
            if not next_key:
                break

    def iter_closed_markets(self) -> Iterable[Dict[str, object]]:
        return self.paginate("/polymarket/markets", "markets", params={"status": "closed"}, limit=100)

    def iter_orders_for_condition(self, condition_id: str) -> Iterable[Dict[str, object]]:
        return self.paginate(
            "/polymarket/orders",
            "orders",
            params={"condition_id": condition_id},
            limit=1000,
        )
