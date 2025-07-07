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
