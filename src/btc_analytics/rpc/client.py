from __future__ import annotations

import itertools
import time
from typing import Any


class BitcoinRPCError(RuntimeError):
    pass


class BitcoinRPCClient:
    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        timeout: int = 30,
        max_retries: int = 3,
        retry_backoff_sec: float = 1.0,
    ) -> None:
        self._url = url
        self._auth = (user, password)
        self._timeout = timeout
        self._id_counter = itertools.count(1)
        self._max_retries = max_retries
        self._retry_backoff_sec = retry_backoff_sec

    def call(self, method: str, *params: Any) -> Any:
        payload = {
            "jsonrpc": "1.0",
            "id": next(self._id_counter),
            "method": method,
            "params": list(params),
        }
        try:
            import requests
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise ModuleNotFoundError("requests is required for Bitcoin RPC access") from exc

        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                response = requests.post(self._url, json=payload, auth=self._auth, timeout=self._timeout)
                response.raise_for_status()
                body = response.json()
                if body.get("error"):
                    raise BitcoinRPCError(f"RPC error {method}: {body['error']}")
                return body["result"]
            except (requests.RequestException, ValueError, BitcoinRPCError) as exc:
                last_error = exc
                if attempt >= self._max_retries:
                    break
                sleep_for = self._retry_backoff_sec * (2**attempt)
                time.sleep(sleep_for)

        raise BitcoinRPCError(f"RPC call failed after {self._max_retries + 1} attempts: {method}: {last_error}")

    def get_block_count(self) -> int:
        return int(self.call("getblockcount"))

    def get_block_hash(self, height: int) -> str:
        return str(self.call("getblockhash", height))

    def get_block(self, block_hash: str, verbosity: int = 2) -> dict[str, Any]:
        result = self.call("getblock", block_hash, verbosity)
        if not isinstance(result, dict):
            raise BitcoinRPCError("Unexpected getblock response")
        return result
