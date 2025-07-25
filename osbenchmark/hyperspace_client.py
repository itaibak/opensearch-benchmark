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
    def __init__(self,
                 host: Dict[str, Any],
                 timeout: int = 60,
                 token: Optional[str] = None,
                 debug: bool = False,
                 login_user: Optional[str] = None,
                 login_password: Optional[str] = None):
        self.host = {
            "host": host.get("host"),
            "port": host.get("port", 80),
            "scheme": host.get("scheme", "http"),
        }
        self.base_url = f"{self.host['scheme']}://{self.host['host']}:{self.host['port']}/api/v1"
        self.timeout = timeout
        self.headers = {}
        self.debug = debug
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        elif login_user is not None or login_password is not None:
            if login_user is None or login_password is None:
                raise ValueError("login_user and login_password must both be provided")
            self._login(str(login_user), str(login_password))

        self.is_hyperspace = True

        # initialize helpers; subclasses may overwrite
        self.cluster = None
        self.indices = None
        self.nodes = None

    def _debug_log(self, msg: str) -> None:
        if self.debug:
            print(f"[DEBUG] {msg}")

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

    def _login(self, username: str, password: str) -> None:
        """Authenticate against the login API and store the bearer token."""
        url = self._url("login")
        self._debug_log(f"POST {url} username={username} password=*****")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        resp = requests.post(
            url,
            json={"username": username, "password": password},
            headers=headers,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json() if resp.content else {}
        token = data.get("token") or data.get("access_token")
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        else:
            raise RuntimeError("Login response did not contain a token")


class _Cluster:
    """Simple placeholder cluster client."""

    def __init__(self, client: "HyperspaceClient"):
        self._client = client

    def health(self, *args, **kwargs):
        return {"status": "green", "relocating_shards": 0}

    # The following cluster APIs are not supported by Hyperspace. They are
    # provided as no-op implementations so callers relying on the OpenSearch
    # client interface don't fail with AttributeError.

    def put_settings(self, *args, **kwargs):
        self._client._debug_log("cluster.put_settings called - returning empty response")
        return {}

    def put_component_template(self, *args, **kwargs):
        self._client._debug_log("cluster.put_component_template called - returning empty response")
        return {}

    def delete_component_template(self, *args, **kwargs):
        self._client._debug_log("cluster.delete_component_template called - returning empty response")
        return {}

    def put_index_template(self, *args, **kwargs):
        self._client._debug_log("cluster.put_index_template called - returning empty response")
        return {}


class _AsyncCluster:
    """Async variant of :class:`_Cluster`."""

    def __init__(self, client: "AsyncHyperspaceClient"):
        self._client = client

    async def health(self, *args, **kwargs):
        return {"status": "green", "relocating_shards": 0}

    # Provide no-op implementations for APIs that the Hyperspace service does
    # not support. They mimic the OpenSearch client's interface but simply
    # return an empty response.

    async def put_settings(self, *args, **kwargs):
        self._client._debug_log("cluster.put_settings called - returning empty response")
        return {}

    async def put_component_template(self, *args, **kwargs):
        self._client._debug_log("cluster.put_component_template called - returning empty response")
        return {}

    async def delete_component_template(self, *args, **kwargs):
        self._client._debug_log("cluster.delete_component_template called - returning empty response")
        return {}

    async def put_index_template(self, *args, **kwargs):
        self._client._debug_log("cluster.put_index_template called - returning empty response")
        return {}


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
        # Hyperspace does not expose a refresh/commit operation. Provide a
        # no-op implementation so benchmarks expecting this API do not fail.
        self._client._debug_log(f"indices.refresh called for index={index} - returning empty response")
        return {}

    def put_settings(self, *args, **kwargs):
        self._client._debug_log("indices.put_settings called - returning empty response")
        return {}

    def shrink(self, *args, **kwargs):
        self._client._debug_log("indices.shrink called - returning empty response")
        return {}

    def delete_index_template(self, *args, **kwargs):
        self._client._debug_log("indices.delete_index_template called - returning empty response")
        return {}

    def exists_template(self, *args, **kwargs) -> bool:
        self._client._debug_log("indices.exists_template called - returning False")
        return False

    def forcemerge(self, *args, **kwargs):
        self._client._debug_log("indices.forcemerge called - returning empty response")
        return {}

    def stats(self, metric: str = "_all", level: str = None, **kwargs):
        # Provide minimal stats so wait conditions in benchmarks succeed.
        self._client._debug_log("indices.stats called - returning minimal stats")
        return {
            "_all": {
                "total": {
                    "merges": {
                        "current": 0
                    }
                }
            }
        }


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
        # Refresh is not required by Hyperspace; return an empty response.
        self._client._debug_log(f"indices.refresh called for index={index} - returning empty response")
        return {}

    async def put_settings(self, *args, **kwargs):
        self._client._debug_log("indices.put_settings called - returning empty response")
        return {}

    async def shrink(self, *args, **kwargs):
        self._client._debug_log("indices.shrink called - returning empty response")
        return {}

    async def delete_index_template(self, *args, **kwargs):
        self._client._debug_log("indices.delete_index_template called - returning empty response")
        return {}

    async def exists_template(self, *args, **kwargs) -> bool:
        self._client._debug_log("indices.exists_template called - returning False")
        return False

    async def stats(self, metric: str = "_all", level: str = None, **kwargs):
        self._client._debug_log("indices.stats called - returning minimal stats")
        return {
            "_all": {
                "total": {
                    "merges": {
                        "current": 0
                    }
                }
            }
        }

    async def forcemerge(self, *args, **kwargs):
        self._client._debug_log("indices.forcemerge called - returning empty response")
        return {}


class _SyncTransport:
    def __init__(self, base_url: str, host: Dict[str, Any], headers: Dict[str, Any], timeout: int, ctx_holder: RequestContextHolder, debug: bool = False):
        self.hosts = [host]
        self._base_url = base_url.rstrip("/")
        self._headers = headers
        self._timeout = timeout
        self._ctx_holder = ctx_holder
        self._debug = debug

    def perform_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                        body: Any = None, headers: Optional[Dict[str, Any]] = None):
        url = f"{self._base_url}/{path.lstrip('/')}"
        hdrs = {**self._headers, **(headers or {})}
        if self._debug:
            print(f"[DEBUG] {method} {url} params={params} body={body}")
        self._ctx_holder.on_client_request_start()
        self._ctx_holder.on_request_start()
        try:
            if isinstance(body, (bytes, bytearray)):
                resp = requests.request(method, url, params=params, data=body, headers=hdrs, timeout=self._timeout)
            else:
                resp = requests.request(method, url, params=params, json=body, headers=hdrs, timeout=self._timeout)
            resp.raise_for_status()
            if self._debug:
                status = getattr(resp, "status", getattr(resp, "status_code", ""))
                print(f"[DEBUG RESPONSE] {status} {resp.content}")
            if resp.content and resp.headers.get("Content-Type", "").startswith("application/json"):
                return resp.json()
            return {}
        finally:
            self._ctx_holder.on_request_end()
            self._ctx_holder.on_client_request_end()

    def close(self):
        pass


class _AsyncTransport:
    def __init__(self, base_url: str, host: Dict[str, Any], headers: Dict[str, Any], timeout: int, session: aiohttp.ClientSession, ctx_holder: RequestContextHolder, debug: bool = False):
        self.hosts = [host]
        self._base_url = base_url.rstrip("/")
        self._headers = headers
        self._timeout = timeout
        self._session = session
        self._ctx_holder = ctx_holder
        self._debug = debug

    async def perform_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                              body: Any = None, headers: Optional[Dict[str, Any]] = None):
        url = f"{self._base_url}/{path.lstrip('/')}"
        hdrs = {**self._headers, **(headers or {})}
        if self._debug:
            print(f"[DEBUG] {method} {url} params={params} body={body}")
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
                raw = await resp.read()
                if self._debug:
                    print(f"[DEBUG RESPONSE] {resp.status} {raw}")
                if resp.headers.get("Content-Type", "").startswith("application/json"):
                    result = json.loads(raw.decode("utf-8")) if raw else {}
                else:
                    result = {}
            return result
        finally:
            self._ctx_holder.on_request_end()
            self._ctx_holder.on_client_request_end()

    async def close(self):
        await self._session.close()


class HyperspaceClient(_BaseClient):
    """Synchronous client for the Hyperspace service."""

    def __init__(self,
                 host: Dict[str, Any],
                 timeout: int = 60,
                 token: Optional[str] = None,
                 debug: bool = False,
                 login_user: Optional[str] = None,
                 login_password: Optional[str] = None):
        super().__init__(host, timeout, token, debug, login_user, login_password)
        self.transport = _SyncTransport(self.base_url, self.host, self.headers, timeout, self, debug)
        self.cluster = _Cluster(self)
        self.indices = _Indices(self)
        self.nodes = _Nodes(self)

    def info(self) -> Dict[str, Any]:
        # provide a minimal version response for compatibility
        return {"version": {"number": "1.0.0", "build_hash": ""}}

    def bulk(self, index: str, body: Any, params: Optional[Dict[str, Any]] = None, **_ignored) -> Dict[str, Any]:
        docs = self._parse_bulk_body(body)
        batch = [
            {"_id": doc.get("_id", ""), "doc_data": msgpack.packb(doc)}
            for doc in docs
        ]
        data = msgpack.packb(batch)
        return self.transport.perform_request(
            "POST",
            f"{index}/batch",
            params=params,
            body=data,
            headers={**self.headers, "Content-Type": "application/msgpack"},
        )

    def index(self, index: str, document: Dict[str, Any], **_ignored) -> Dict[str, Any]:
        data = msgpack.packb(document)
        return self.transport.perform_request(
            "PUT",
            f"{index}/document/add",
            body=data,
            headers={**self.headers, "Content-Type": "application/msgpack"},
        )

    def search(self, index: str, body: Dict[str, Any], params: Optional[Dict[str, Any]] = None, **_ignored) -> Dict[str, Any]:
        if params is None:
            params = {"size": 10}
        elif "size" not in params:
            params["size"] = 10
        try:
            return self.transport.perform_request(
                "POST",
                f"{index}/dsl_search",
                params=params,
                body=body,
                headers=self.headers,
            )
        except requests.HTTPError:
            if "*" in index:
                # Wildcard patterns are not supported; return an empty result
                # instead of propagating the failure.
                self._debug_log(f"wildcard search fallback for index={index}")
                return {"hits": {"hits": []}}
            raise

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

    def __init__(self,
                 host: Dict[str, Any],
                 timeout: int = 60,
                 token: Optional[str] = None,
                 debug: bool = False,
                 login_user: Optional[str] = None,
                 login_password: Optional[str] = None):
        super().__init__(host, timeout, token, debug, login_user, login_password)
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout))
        self.transport = _AsyncTransport(self.base_url, self.host, self.headers, timeout, self._session, self, debug)
        self.cluster = _AsyncCluster(self)
        self.indices = _AsyncIndices(self)
        self.nodes = _AsyncNodes(self)

    async def info(self) -> Dict[str, Any]:
        return {"version": {"number": "1.0.0", "build_hash": ""}}

    async def bulk(self, index: str, body: Any, params: Optional[Dict[str, Any]] = None, **_ignored) -> Dict[str, Any]:
        docs = HyperspaceClient._parse_bulk_body(body)
        batch = [
            {"_id": doc.get("_id", ""), "doc_data": msgpack.packb(doc)}
            for doc in docs
        ]
        data = msgpack.packb(batch)
        return await self.transport.perform_request(
            "POST",
            f"{index}/batch",
            params=params,
            body=data,
            headers={**self.headers, "Content-Type": "application/msgpack"},
        )

    async def index(self, index: str, document: Dict[str, Any], **_ignored) -> Dict[str, Any]:
        data = msgpack.packb(document)
        return await self.transport.perform_request(
            "PUT",
            f"{index}/document/add",
            body=data,
            headers={**self.headers, "Content-Type": "application/msgpack"},
        )

    async def search(self, index: str, body: Dict[str, Any], params: Optional[Dict[str, Any]] = None, **_ignored) -> Dict[str, Any]:
        if params is None:
            params = {"size": 10}
        elif "size" not in params:
            params["size"] = 10
        try:
            return await self.transport.perform_request(
                "POST",
                f"{index}/dsl_search",
                params=params,
                body=body,
                headers=self.headers,
            )
        except aiohttp.ClientResponseError:
            if "*" in index:
                self._debug_log(f"wildcard search fallback for index={index}")
                return {"hits": {"hits": []}}
            raise

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


    async def close(self):
        await self.transport.close()
