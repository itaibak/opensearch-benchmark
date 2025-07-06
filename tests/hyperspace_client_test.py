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
