from dataclasses import fields
from pathlib import Path

import db
import pytest

import preprocessing


# TODO - add connection to test instance to allow deletion


def test_instantiation():
    client = db.connect_weaviate()
    assert client.is_ready() is True


@pytest.mark.parametrize(
    "collection_config",
    [
        {'class': 'TestCollectionA'}
    ]
)
def test_class_addition(collection_config):
    # Connect to Weaviate
    client = db.connect_weaviate()

    # Prep
    collection_name = collection_config['class']
    client.schema.delete_class(collection_name)

    # Add new class
    response = db.add_class_if_not_present(client, collection_config)
    assert response is True  # Should be True if newly added
    response = db.add_class_if_not_present(client, collection_config)
    assert response is None  # Should be None if it exists

    # Clean up
    client.schema.delete_class(collection_name)


# def test_start_db():
#     # Connect to Weaviate
#     client = db.connect_weaviate()  # TODO - replace this with test client
#
#     # Prep
#     for c in db.DEFAULT_CLASSES["classes"]:
#         client.schema.delete_class(c["class"])
#         assert not client.schema.exists(c["class"])
#
#     # Check that it has added classes
#     db.start_db(custom_client=client)
#     for c in db.DEFAULT_CLASSES["classes"]:
#         assert client.schema.exists(c["class"])
#
#     # Cleanup
#     for c in db.DEFAULT_CLASSES["classes"]:
#         client.schema.delete_class(c["class"])
#         assert not client.schema.exists(c["class"])


@pytest.mark.parametrize(
    "source_class, chunk_class",
    [
        ('TestSource', 'TestChunk')
    ]
)
def test_collection_instantiation(source_class: str, chunk_class: str):
    # Connect to Weaviate
    client = db.connect_weaviate()  # TODO - replace this with test client
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = db.DistylledData(client=client, source_class=source_class, chunk_class=chunk_class)
    for c in [source_class, chunk_class]:
        assert client.schema.exists(c)
    assert collection.client == client

    for c in [source_class, chunk_class]:
        client.schema.delete_class(c)


@pytest.mark.parametrize(
    "wv_object, source_class, chunk_class",
    [
        ({"name": "value"}, 'TestSource', 'TestChunk')
    ]
)
def test_add_object(wv_object, source_class, chunk_class):
    # Connect to Weaviate
    client = db.connect_weaviate()  # TODO - replace this with test client

    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = db.DistylledData(client=client, source_class=source_class, chunk_class=chunk_class)

    # Tests
    for c in [source_class, chunk_class]:
        response = collection._add_object(wv_object, c)
        assert response is True
        response = client.query.aggregate(c).with_meta_count().do()
        assert response["data"]["Aggregate"][c][0]["meta"]["count"] == 1

        response = collection._add_object(wv_object, c)
        assert response is None
        response = client.query.aggregate(c).with_meta_count().do()
        assert response["data"]["Aggregate"][c][0]["meta"]["count"] == 1

        collection._add_object({"test": "AnotherObject"}, c)
        response = client.query.aggregate(c).with_meta_count().do()
        assert response["data"]["Aggregate"][c][0]["meta"]["count"] == 2
        client.schema.delete_class(c)


@pytest.mark.parametrize(
    "n_chunks, source_object_data, source_class, chunk_class",
    [
        (1, db.SourceData(path="youTube", body="Why, hello there"), 'TestSource', 'TestChunk'),
        (10, db.SourceData(path="youTube", body="Why, hello there"), 'TestSource', 'TestChunk'),
    ]
)
def test_add_chunks(n_chunks, source_object_data, source_class, chunk_class):
    chunks = ["A" * (i+1) for i in range(n_chunks)]

    # Connect to Weaviate
    client = db.connect_weaviate()  # TODO - replace this with test client

    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = db.DistylledData(client=client, source_class=source_class, chunk_class=chunk_class)

    # Tests
    collection.import_chunks(chunks, source_object_data)
    response = client.query.aggregate(chunk_class).with_meta_count().do()
    assert response["data"]["Aggregate"][chunk_class][0]["meta"]["count"] == n_chunks
    client.schema.delete_class(chunk_class)


@pytest.mark.parametrize(
    "n_chunks, source_class, chunk_class, chunk_number_offset",
    [
        (1, 'TestSource', 'TestChunk', 0),
        (5, 'TestSource', 'TestChunk', 0),
    ]
)
def test_add_data(n_chunks, source_class, chunk_class, chunk_number_offset):
    source_object_data = db.SourceData(
        path="youTube",
        body="A" * preprocessing.MAX_CHUNK_CHARS * n_chunks
    )

    # Connect to Weaviate
    client = db.connect_weaviate()  # TODO - replace this with test client

    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = db.DistylledData(client=client, source_class=source_class, chunk_class=chunk_class)

    # Tests
    collection.add_data(source_object_data, chunk_number_offset=chunk_number_offset)
    response = client.query.aggregate(chunk_class).with_meta_count().do()
    assert response["data"]["Aggregate"][chunk_class][0]["meta"]["count"] == n_chunks + 1
    client.schema.delete_class(chunk_class)
    # TODO - add test for offset


@pytest.mark.parametrize(
    "source_path, n_chunks, chunk_number_offset, source_title, source_class, chunk_class",
    [
        ("YouTube", 1, 0, "YouTubeVideo", 'TestSource', 'TestChunk'),
        ("YouTube", 5, 0, "YouTubeVideo", 'TestSource', 'TestChunk'),
    ]
)
def test_add_text(source_path, n_chunks, chunk_number_offset, source_title, source_class, chunk_class):
    # Connect to Weaviate
    client = db.connect_weaviate()  # TODO - replace this with test client

    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = db.DistylledData(client=client, source_class=source_class, chunk_class=chunk_class)

    # Tests
    source_text = "A" * preprocessing.MAX_CHUNK_CHARS * n_chunks
    collection.add_text(
        source_path=source_path,
        source_text=source_text,
        source_title=source_title,
        chunk_number_offset=chunk_number_offset
    )
    response = client.query.aggregate(chunk_class).with_meta_count().do()
    assert response["data"]["Aggregate"][chunk_class][0]["meta"]["count"] == n_chunks + 1
    client.schema.delete_class(chunk_class)
    # TODO - add test for offset


@pytest.mark.parametrize(
    "youtube_url, source_class, chunk_class",
    [
        ("https://youtu.be/ni3T4vStzBI", 'TestSource', 'TestChunk')
    ]
)
def test_add_from_youtube(youtube_url, source_class, chunk_class):
    # Connect to Weaviate
    client = db.connect_weaviate()  # TODO - replace this with test client

    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = db.DistylledData(client=client, source_class=source_class, chunk_class=chunk_class)

    # Tests
    collection.add_from_youtube(youtube_url)
    response = (
        client.query.aggregate(chunk_class)
        .with_where({
            "path": ["source_path"],
            "operator": "Equal",
            "valueText": youtube_url
        })
        .with_meta_count()
        .do()
    )
    count = response['data']['Aggregate'][chunk_class][0]['meta']['count']
    assert count > 0

    response = (
        client.query.get(chunk_class, [i.name for i in fields(db.ChunkData)])
        .with_where({
            "path": ["source_path"],
            "operator": "Equal",
            "valueText": youtube_url
        })
        .with_sort({
            'path': ['chunk_number'],
            'order': 'asc'
        })
        .with_limit(5)
        .do()
    )

    for row in response['data']['Get'][chunk_class]:
        assert len(row['chunk_text']) > 0
