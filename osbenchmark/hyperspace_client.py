# SPDX-License-Identifier: Apache-2.0
"""Client helpers for Hyperspace compatible REST API."""
import json
from typing import Any, List, Dict, Optional
import os

import msgpack
import requests
import aiohttp

from osbenchmark.context import RequestContextHolder


class _BaseClient(RequestContextHolder):
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

        self.is_hyperspace = True

        # initialize helpers; subclasses may overwrite
        self.cluster = None
        self.indices = None
        self.nodes = None

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"


class _Cluster:
    """Simple placeholder cluster client."""

    def health(self, *args, **kwargs):
        return {"status": "green", "relocating_shards": 0}


class _AsyncCluster:
    """Async variant of :class:`_Cluster`."""

    async def health(self, *args, **kwargs):
        return {"status": "green", "relocating_shards": 0}


class _Nodes:
    """Minimal nodes helper providing static info and stats."""

    def __init__(self, client: "HyperspaceClient"):
        self._client = client

    def info(self, node_id: str = "_all") -> Dict[str, Any]:
        name = self._client.host["host"]
        return {
            "nodes": {
                name: {
                    "name": name,
                    "host": name,
                    "ip": name,
                    "os": {"name": "", "version": "", "available_processors": 1},
                    "jvm": {"vm_vendor": "", "version": ""},
                }
            }
        }

    def stats(self, metric: str = "_all", level: str = None) -> Dict[str, Any]:
        name = self._client.host["host"]
        return {
            "nodes": {
                name: {
                    "name": name,
                    "indices": {},
                    "thread_pool": {},
                    "breakers": {},
                    "jvm": {
                        "gc": {
                            "collectors": {
                                "old": {"collection_time_in_millis": 0, "collection_count": 0},
                                "young": {"collection_time_in_millis": 0, "collection_count": 0},
                            }
                        },
                        "mem": {"pools": {}},
                        "buffer_pools": {},
                    },
                    "transport": {},
                    "process": {"cpu": {}},
                    "indexing_pressure": {},
                }
            }
        }


class _AsyncNodes:
    """Async variant of :class:`_Nodes`."""

    def __init__(self, client: "AsyncHyperspaceClient"):
        self._client = client

    async def info(self, node_id: str = "_all") -> Dict[str, Any]:
        name = self._client.host["host"]
        return {
            "nodes": {
                name: {
                    "name": name,
                    "host": name,
                    "ip": name,
                    "os": {"name": "", "version": "", "available_processors": 1},
                    "jvm": {"vm_vendor": "", "version": ""},
                }
            }
        }

    async def stats(self, metric: str = "_all", level: str = None) -> Dict[str, Any]:
        name = self._client.host["host"]
        return {
            "nodes": {
                name: {
                    "name": name,
                    "indices": {},
                    "thread_pool": {},
                    "breakers": {},
                    "jvm": {
                        "gc": {
                            "collectors": {
                                "old": {"collection_time_in_millis": 0, "collection_count": 0},
                                "young": {"collection_time_in_millis": 0, "collection_count": 0},
                            }
                        },
                        "mem": {"pools": {}},
                        "buffer_pools": {},
                    },
                    "transport": {},
                    "process": {"cpu": {}},
                    "indexing_pressure": {},
                }
            }
        }


class _Indices:
    def __init__(self, client: "HyperspaceClient"):
        self._client = client

    def create(self, index: str, body: Any = None, **kwargs):
        return self._client.indices_create(index=index, body=body)

    def delete(self, index: str, params: Optional[Dict[str, Any]] = None, **kwargs):
        return self._client.indices_delete(index=index)

    def exists(self, index: str, **kwargs) -> bool:
        resp = self._client.transport.perform_request(
            "GET",
            "collectionsInfo",
            headers=self._client.headers,
        )
        collections = resp or []
        if isinstance(collections, dict):
            collections = collections.get("collections", collections)
        for c in collections:
            if isinstance(c, dict):
                name = c.get("name")
            else:
                name = c
            if name == index:
                return True
        return False

    def refresh(self, index: str, **kwargs):
        return self._client.commit(index)

    def stats(self, metric: str = "_all", level: str = None, **kwargs):
        return {}


class _AsyncIndices:
    def __init__(self, client: "AsyncHyperspaceClient"):
        self._client = client

    async def create(self, index: str, body: Any = None, **kwargs):
        return await self._client.indices_create(index=index, body=body)

    async def delete(self, index: str, params: Optional[Dict[str, Any]] = None, **kwargs):
        return await self._client.indices_delete(index=index)

    async def exists(self, index: str, **kwargs) -> bool:
        resp = await self._client.transport.perform_request(
            "GET",
            "collectionsInfo",
            headers=self._client.headers,
        )
        collections = resp or []
        if isinstance(collections, dict):
            collections = collections.get("collections", collections)
        for c in collections:
            if isinstance(c, dict):
                name = c.get("name")
            else:
                name = c
            if name == index:
                return True
        return False

    async def refresh(self, index: str, **kwargs):
        return await self._client.commit(index)

    async def stats(self, metric: str = "_all", level: str = None, **kwargs):
        return {}


class _SyncTransport:
    def __init__(self, base_url: str, host: Dict[str, Any], headers: Dict[str, Any], timeout: int, ctx_holder: RequestContextHolder):
        self.hosts = [host]
        self._base_url = base_url.rstrip("/")
        self._headers = headers
        self._timeout = timeout
        self._ctx_holder = ctx_holder

    def perform_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                        body: Any = None, headers: Optional[Dict[str, Any]] = None):
        url = f"{self._base_url}/{path.lstrip('/')}"
        hdrs = {**self._headers, **(headers or {})}
        self._ctx_holder.on_client_request_start()
        self._ctx_holder.on_request_start()
        try:
            if isinstance(body, (bytes, bytearray)):
                resp = requests.request(method, url, params=params, data=body, headers=hdrs, timeout=self._timeout)
            else:
                resp = requests.request(method, url, params=params, json=body, headers=hdrs, timeout=self._timeout)
            resp.raise_for_status()
            if resp.content:
                return resp.json()
            return {}
        finally:
            self._ctx_holder.on_request_end()
            self._ctx_holder.on_client_request_end()

    def close(self):
        pass


class _AsyncTransport:
    def __init__(self, base_url: str, host: Dict[str, Any], headers: Dict[str, Any], timeout: int, session: aiohttp.ClientSession, ctx_holder: RequestContextHolder):
        self.hosts = [host]
        self._base_url = base_url.rstrip("/")
        self._headers = headers
        self._timeout = timeout
        self._session = session
        self._ctx_holder = ctx_holder

    async def perform_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                              body: Any = None, headers: Optional[Dict[str, Any]] = None):
        url = f"{self._base_url}/{path.lstrip('/')}"
        hdrs = {**self._headers, **(headers or {})}
        self._ctx_holder.on_client_request_start()
        self._ctx_holder.on_request_start()
        try:
            async with self._session.request(
                method,
                url,
                params=params,
                json=None if isinstance(body, (bytes, bytearray)) else body,
                data=body if isinstance(body, (bytes, bytearray)) else None,
                headers=hdrs,
            ) as resp:
                resp.raise_for_status()
                result = await resp.json()
            return result
        finally:
            self._ctx_holder.on_request_end()
            self._ctx_holder.on_client_request_end()

    async def close(self):
        await self._session.close()


class HyperspaceClient(_BaseClient):
    """Synchronous client for the Hyperspace service."""

    def __init__(self, host: Dict[str, Any], timeout: int = 60, token: Optional[str] = None):
        super().__init__(host, timeout, token)
        self.transport = _SyncTransport(self.base_url, self.host, self.headers, timeout, self)
        self.cluster = _Cluster()
        self.indices = _Indices(self)
        self.nodes = _Nodes(self)

    def info(self) -> Dict[str, Any]:
        # provide a minimal version response for compatibility
        return {"version": {"number": "1.0.0", "build_hash": ""}}

    def bulk(self, index: str, body: Any) -> Dict[str, Any]:
        docs = self._parse_bulk_body(body)
        data = msgpack.packb(docs)
        return self.transport.perform_request(
            "POST",
            f"{index}/batch",
            body=data,
            headers={**self.headers, "Content-Type": "application/msgpack"},
        )

    def index(self, index: str, document: Dict[str, Any]) -> Dict[str, Any]:
        data = msgpack.packb(document)
        return self.transport.perform_request(
            "PUT",
            f"{index}/document/add",
            body=data,
            headers={**self.headers, "Content-Type": "application/msgpack"},
        )

    def search(self, index: str, body: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.transport.perform_request(
            "POST",
            f"{index}/dsl_search",
            params=params,
            body=body,
            headers=self.headers,
        )

    def indices_create(self, index: str, body: Any = None) -> Dict[str, Any]:
        return self.transport.perform_request(
            "PUT",
            f"collection/{index}",
            body=body,
            headers=self.headers,
        )

    def indices_delete(self, index: str) -> Dict[str, Any]:
        return self.transport.perform_request(
            "GET",
            f"collection/{index}",
            headers=self.headers,
        )

    def commit(self, index: str) -> Dict[str, Any]:
        return self.transport.perform_request(
            "GET",
            f"{index}/commit",
            headers=self.headers,
        )

    def close(self):
        self.transport.close()

    @staticmethod
    def _parse_bulk_body(body: Any) -> List[Dict[str, Any]]:
        if isinstance(body, list):
            return body

        text = None
        if isinstance(body, (bytes, bytearray)):
            text = body.decode("utf-8")
        elif hasattr(body, "read"):
            raw = body.read()
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        elif isinstance(body, str):
            if os.path.isfile(body):
                with open(body, "r", encoding="utf-8") as f:
                    text = f.read()
            else:
                text = body
        else:
            text = str(body)

        lines = [l.strip() for l in text.splitlines() if l.strip()]
        docs = []
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
        self.transport = _AsyncTransport(self.base_url, self.host, self.headers, timeout, self._session, self)
        self.cluster = _AsyncCluster()
        self.indices = _AsyncIndices(self)
        self.nodes = _AsyncNodes(self)

    async def info(self) -> Dict[str, Any]:
        return {"version": {"number": "1.0.0", "build_hash": ""}}

    async def bulk(self, index: str, body: Any, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        docs = HyperspaceClient._parse_bulk_body(body)
        data = msgpack.packb(docs)
        return await self.transport.perform_request(
            "POST",
            f"{index}/batch",
            params=params,
            body=data,
            headers={**self.headers, "Content-Type": "application/msgpack"},
        )

    async def index(self, index: str, document: Dict[str, Any]) -> Dict[str, Any]:
        data = msgpack.packb(document)
        return await self.transport.perform_request(
            "PUT",
            f"{index}/document/add",
            body=data,
            headers={**self.headers, "Content-Type": "application/msgpack"},
        )

    async def search(self, index: str, body: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.transport.perform_request(
            "POST",
            f"{index}/dsl_search",
            params=params,
            body=body,
            headers=self.headers,
        )

    async def indices_create(self, index: str, body: Any = None) -> Dict[str, Any]:
        return await self.transport.perform_request(
            "PUT",
            f"collection/{index}",
            body=body,
            headers=self.headers,
        )

    async def indices_delete(self, index: str) -> Dict[str, Any]:
        return await self.transport.perform_request(
            "GET",
            f"collection/{index}",
            headers=self.headers,
        )

    async def commit(self, index: str) -> Dict[str, Any]:
        return await self.transport.perform_request(
            "GET",
            f"{index}/commit",
            headers=self.headers,
        )

    async def close(self):
        await self.transport.close()
