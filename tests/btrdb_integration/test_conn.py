import logging
from uuid import uuid4 as new_uuid

import pytest

import btrdb


def test_connection_info(conn):
    info = conn.info()
    logging.info(f"connection info: {info}")


def test_create_stream(conn, tmp_collection):
    uuid = new_uuid()
    stream = conn.create(uuid, tmp_collection, tags={"name": "s"})
    assert stream.uuid == uuid
    assert stream.name == "s"


def test_query(conn, tmp_collection):
    conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    uuids = conn.query(
        "select name from streams where collection = $1 order by name;",
        [tmp_collection],
    )
    assert len(uuids) == 2
    assert uuids[0]["name"] == "s1"
    assert uuids[1]["name"] == "s2"


def test_list_collections(conn, tmp_collection):
    assert tmp_collection not in conn.list_collections()
    stream = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    assert tmp_collection in conn.list_collections()
