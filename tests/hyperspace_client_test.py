import json
import pytest

from osbenchmark.hyperspace_client import HyperspaceClient


def test_parse_bulk_body_from_string():
    body = '{"index":{"_id":"1"}}\n{"field":1}\n{"index":{"_id":"2"}}\n{"field":2}\n'
    docs = HyperspaceClient._parse_bulk_body(body)
    assert docs == [{"field": 1}, {"field": 2}]


def test_parse_bulk_body_from_list():
    docs_in = [{"field": 1}, {"field": 2}]
    assert HyperspaceClient._parse_bulk_body(docs_in) == docs_in


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
