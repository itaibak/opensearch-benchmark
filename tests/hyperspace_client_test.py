import json
import pytest
import requests
import msgpack
from pathlib import Path

from osbenchmark.hyperspace_client import HyperspaceClient


def test_parse_bulk_body_from_string():
    body = '{"index":{"_id":"1"}}\n{"field":1}\n{"index":{"_id":"2"}}\n{"field":2}\n'
    docs = HyperspaceClient._parse_bulk_body(body)
    assert docs == [{"field": 1}, {"field": 2}]


def test_parse_bulk_body_from_list():
    docs_in = [{"field": 1}, {"field": 2}]
    assert HyperspaceClient._parse_bulk_body(docs_in) == docs_in


def test_parse_bulk_body_from_file(tmp_path):
    p = tmp_path / "bulk.json"
    p.write_text('{"index":{"_id":"1"}}\n{"field":1}\n')
    assert HyperspaceClient._parse_bulk_body(str(p)) == [{"field": 1}]


def test_parse_bulk_body_from_handle(tmp_path):
    p = tmp_path / "bulk.json"
    p.write_text('{"index":{"_id":"1"}}\n{"field":1}\n')
    with open(p, 'r') as f:
        docs = HyperspaceClient._parse_bulk_body(f)
    assert docs == [{"field": 1}]


def test_transport_hosts():
    client = HyperspaceClient({"host": "localhost", "port": 9200})
    assert client.transport.hosts[0]["host"] == "localhost"
    client.close()


def test_cluster_health_default():
    client = HyperspaceClient({"host": "localhost"})
    assert client.cluster.health()["status"] == "green"
    client.close()


def test_indices_wrapper():
    client = HyperspaceClient({"host": "localhost"})
    assert hasattr(client, "indices")
    assert callable(getattr(client.indices, "create"))
    client.close()


def test_nodes_info_and_stats():
    client = HyperspaceClient({"host": "localhost"})
    info = client.nodes.info()
    stats = client.nodes.stats()
    assert "nodes" in info
    assert "nodes" in stats
    client.close()


def test_info_build_hash():
    client = HyperspaceClient({"host": "localhost"})
    assert "build_hash" in client.info()["version"]
    client.close()


def test_debug_prints_api_call(monkeypatch, capsys):
    class Resp:
        content = b"{}"
        headers = {"Content-Type": "application/json"}
        def raise_for_status(self):
            pass
        def json(self):
            return {}

    def dummy_request(method, url, params=None, data=None, json=None, headers=None, timeout=None):
        return Resp()

    monkeypatch.setattr(requests, "request", dummy_request)
    monkeypatch.setattr(HyperspaceClient, "on_client_request_start", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_client_request_end", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_request_start", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_request_end", lambda self: None)
    client = HyperspaceClient({"host": "localhost"}, debug=True)
    client.transport.perform_request("GET", "collectionsInfo")
    out = capsys.readouterr().out
    assert "[DEBUG] GET" in out
    client.close()


def test_bulk_packs_msgpack(monkeypatch):
    captured = {}

    class Resp:
        content = b"{}"
        headers = {"Content-Type": "application/json"}

        def raise_for_status(self):
            pass

        def json(self):
            return {}

    def dummy_request(method, url, params=None, data=None, json=None, headers=None, timeout=None):
        captured["data"] = data
        return Resp()

    monkeypatch.setattr(requests, "request", dummy_request)
    monkeypatch.setattr(HyperspaceClient, "on_client_request_start", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_client_request_end", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_request_start", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_request_end", lambda self: None)

    client = HyperspaceClient({"host": "localhost"})
    docs = [{"_id": "1", "field": 1}, {"field": 2}]
    client.bulk("test", docs)
    packed = msgpack.unpackb(captured["data"])  # list of dicts
    assert packed[0]["_id"] == "1"
    assert isinstance(packed[0]["doc_data"], bytes)
    client.close()


def test_non_json_response(monkeypatch):
    class Resp:
        headers = {"Content-Type": "text/plain"}
        content = b"ok"
        def raise_for_status(self):
            pass
        def json(self):
            raise ValueError

    def dummy_request(method, url, params=None, data=None, json=None, headers=None, timeout=None):
        return Resp()

    monkeypatch.setattr(requests, "request", dummy_request)
    monkeypatch.setattr(HyperspaceClient, "on_client_request_start", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_client_request_end", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_request_start", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_request_end", lambda self: None)

    client = HyperspaceClient({"host": "localhost"})
    resp = client.transport.perform_request("GET", "collectionsInfo")
    assert resp == {}
    client.close()

def test_search_uses_dsl(monkeypatch):
    captured = {}

    class Resp:
        content = b"{}"
        headers = {"Content-Type": "application/json"}
        def raise_for_status(self):
            pass
        def json(self):
            return {}

    def dummy_request(method, url, params=None, data=None, json=None, headers=None, timeout=None):
        captured['method'] = method
        captured['url'] = url
        captured['params'] = params
        captured['json'] = json
        return Resp()

    monkeypatch.setattr(requests, "request", dummy_request)
    monkeypatch.setattr(HyperspaceClient, "on_client_request_start", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_client_request_end", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_request_start", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_request_end", lambda self: None)

    client = HyperspaceClient({"host": "localhost"})
    client.search("my-index", {"query": {"match_all": {}}}, params={"size": 5})
    assert captured['url'].endswith('/my-index/dsl_search')
    assert captured['json'] == {"query": {"match_all": {}}}
    assert captured['params']["size"] == 5
    client.close()


def test_stub_cluster_apis():
    client = HyperspaceClient({"host": "localhost"})
    assert client.cluster.put_settings({}) == {}
    assert client.cluster.put_component_template("t", body={}) == {}
    assert client.cluster.delete_component_template("t") == {}
    assert client.cluster.put_index_template("t", body={}) == {}
    client.close()


def test_stub_index_apis():
    client = HyperspaceClient({"host": "localhost"})
    assert client.indices.refresh("idx") == {}
    assert client.indices.put_settings({}) == {}
    assert client.indices.shrink("s", target="t") == {}
    assert client.indices.delete_index_template("t") == {}
    assert client.indices.exists_template("t") is False
    assert client.indices.forcemerge(index="idx") == {}
    stats = client.indices.stats()
    assert stats["_all"]["total"]["merges"]["current"] == 0
    client.close()


def test_wildcard_search_returns_empty(monkeypatch):
    captured = {}

    class Resp:
        content = b"{}"
        headers = {"Content-Type": "application/json"}

        def raise_for_status(self):
            raise requests.HTTPError()

        def json(self):
            return {}

    def dummy_request(method, url, params=None, data=None, json=None, headers=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        return Resp()

    monkeypatch.setattr(requests, "request", dummy_request)
    monkeypatch.setattr(HyperspaceClient, "on_client_request_start", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_client_request_end", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_request_start", lambda self: None)
    monkeypatch.setattr(HyperspaceClient, "on_request_end", lambda self: None)

    client = HyperspaceClient({"host": "localhost"})
    resp = client.search("logs-*", {"query": {"match_all": {}}})
    assert resp == {"hits": {"hits": []}}
    assert captured["method"] == "POST"
    assert captured["url"].endswith("logs-*/dsl_search")
    client.close()
