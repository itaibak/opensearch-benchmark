# SPDX-License-Identifier: Apache-2.0
"""Client helpers for Hyperspace compatible REST API."""
import json
from typing import Any, List, Dict, Optional

import msgpack
import requests
import aiohttp


class _BaseClient:
    def __init__(self, host: Dict[str, Any], timeout: int = 60, token: Optional[str] = None):
        self.host = {
            "host": host.get("host"),
            "port": host.get("port", 80),
            "scheme": host.get("scheme", "http"),
        }
        self.base_url = f"{self.host['scheme']}://{self.host['host']}:{self.host['port']}/api/v1"
        self.timeout = timeout
        self.headers = {}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path}"


class _SyncTransport:
    def __init__(self, base_url: str, host: Dict[str, Any], headers: Dict[str, Any], timeout: int):
        self.hosts = [host]
        self._base_url = base_url.rstrip("/")
        self._headers = headers
        self._timeout = timeout

    def perform_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                        body: Any = None, headers: Optional[Dict[str, Any]] = None):
        url = f"{self._base_url}/{path.lstrip('/')}"
        hdrs = {**self._headers, **(headers or {})}
        resp = requests.request(method, url, params=params, json=body, headers=hdrs, timeout=self._timeout)
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return {}

    def close(self):
        pass


class _AsyncTransport:
    def __init__(self, base_url: str, host: Dict[str, Any], headers: Dict[str, Any], timeout: int, session: aiohttp.ClientSession):
        self.hosts = [host]
        self._base_url = base_url.rstrip("/")
        self._headers = headers
        self._timeout = timeout
        self._session = session

    async def perform_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                              body: Any = None, headers: Optional[Dict[str, Any]] = None):
        url = f"{self._base_url}/{path.lstrip('/')}"
        hdrs = {**self._headers, **(headers or {})}
        async with self._session.request(method, url, params=params, json=body, headers=hdrs) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def close(self):
        await self._session.close()


class HyperspaceClient(_BaseClient):
    """Synchronous client for the Hyperspace service."""

    def __init__(self, host: Dict[str, Any], timeout: int = 60, token: Optional[str] = None):
        super().__init__(host, timeout, token)
        self.transport = _SyncTransport(self.base_url, self.host, self.headers, timeout)

    def bulk(self, index: str, body: Any) -> Dict[str, Any]:
        docs = self._parse_bulk_body(body)
        data = msgpack.packb(docs)
        resp = requests.post(self._url(f"{index}/batch"), data=data,
                             headers={**self.headers, "Content-Type": "application/msgpack"},
                             timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def index(self, index: str, document: Dict[str, Any]) -> Dict[str, Any]:
        data = msgpack.packb(document)
        resp = requests.put(self._url(f"{index}/document/add"), data=data,
                            headers={**self.headers, "Content-Type": "application/msgpack"},
                            timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def search(self, index: str, body: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        resp = requests.post(self._url(f"{index}/dsl_search"), json=body, params=params,
                             headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def indices_create(self, index: str, body: Any = None) -> Dict[str, Any]:
        resp = requests.put(self._url(f"collection/{index}"), json=body, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def indices_delete(self, index: str) -> Dict[str, Any]:
        resp = requests.get(self._url(f"collection/{index}"), headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def commit(self, index: str) -> Dict[str, Any]:
        resp = requests.get(self._url(f"{index}/commit"), headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def close(self):
        self.transport.close()

    @staticmethod
    def _parse_bulk_body(body: Any) -> List[Dict[str, Any]]:
        if isinstance(body, list):
            return body
        docs = []
        lines = [l for l in str(body).splitlines() if l]
        skip_next = False
        for line in lines:
            if skip_next:
                docs.append(json.loads(line))
                skip_next = False
                continue
            if line.startswith("{") and ("\"index\"" in line or "\"create\"" in line):
                skip_next = True
            else:
                docs.append(json.loads(line))
        return docs


class AsyncHyperspaceClient(_BaseClient):
    """Asynchronous client for the Hyperspace service."""

    def __init__(self, host: Dict[str, Any], timeout: int = 60, token: Optional[str] = None):
        super().__init__(host, timeout, token)
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout))
        self.transport = _AsyncTransport(self.base_url, self.host, self.headers, timeout, self._session)

    async def bulk(self, index: str, body: Any, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        docs = HyperspaceClient._parse_bulk_body(body)
        data = msgpack.packb(docs)
        async with self._session.post(self._url(f"{index}/batch"), data=data,
                                       params=params,
                                       headers={**self.headers, "Content-Type": "application/msgpack"}) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def index(self, index: str, document: Dict[str, Any]) -> Dict[str, Any]:
        data = msgpack.packb(document)
        async with self._session.put(self._url(f"{index}/document/add"), data=data,
                                     headers={**self.headers, "Content-Type": "application/msgpack"}) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def search(self, index: str, body: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        async with self._session.post(self._url(f"{index}/dsl_search"), json=body, params=params,
                                      headers=self.headers) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def indices_create(self, index: str, body: Any = None) -> Dict[str, Any]:
        async with self._session.put(self._url(f"collection/{index}"), json=body, headers=self.headers) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def indices_delete(self, index: str) -> Dict[str, Any]:
        async with self._session.get(self._url(f"collection/{index}"), headers=self.headers) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def commit(self, index: str) -> Dict[str, Any]:
        async with self._session.get(self._url(f"{index}/commit"), headers=self.headers) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def close(self):
        await self.transport.close()
