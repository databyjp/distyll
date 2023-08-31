from dataclasses import fields
from pathlib import Path

import db
import pytest

import preprocessing


# TODO - add connection to test instance to allow deletion


# def test_instantiation():
#     client = db.connect_weaviate()
#     assert client.is_ready() is True
#
#
# @pytest.mark.parametrize(
#     "collection_config",
#     [
#         {'class': 'TestCollectionA'}
#     ]
# )
# def test_class_addition(collection_config):
#     # Connect to Weaviate
#     client = db.connect_weaviate()
#
#     # Prep
#     collection_name = collection_config['class']
#     client.schema.delete_class(collection_name)
#
#     # Add new class
#     response = db.add_class_if_not_present(client, collection_config)
#     assert response is True  # Should be True if newly added
#     response = db.add_class_if_not_present(client, collection_config)
#     assert response is None  # Should be None if it exists
#
#     # Clean up
#     client.schema.delete_class(collection_name)
#
#
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
#
#
# @pytest.mark.parametrize(
#     "collection_name",
#     [
#         'TestCollectionA'
#     ]
# )
# def test_collection_instantiation(collection_name):
#     # Connect to Weaviate
#     client = db.connect_weaviate()  # TODO - replace this with test client
#     client.schema.delete_class(collection_name)
#
#     # Instantiate a collection
#     collection = db.Collection(client=client, collection_name=collection_name)
#     assert collection.collection_name == collection_name
#     assert collection.client == client
#     client.schema.delete_class(collection_name)
#
#
# @pytest.mark.parametrize(
#     "wv_object, collection_name",
#     [
#         ({"name": "value"}, "TestCollectionA")
#     ]
# )
# def test_add_object(wv_object, collection_name):
#     # Connect to Weaviate
#     client = db.connect_weaviate()  # TODO - replace this with test client
#
#     # Instantiate a collection
#     if client.schema.exists(collection_name):
#         client.schema.delete_class(collection_name)
#     collection = db.Collection(client=client, collection_name=collection_name)
#
#     # Tests
#     response = collection.add_object(wv_object)
#     assert response is True
#     response = client.query.aggregate(collection_name).with_meta_count().do()
#     assert response["data"]["Aggregate"][collection_name][0]["meta"]["count"] == 1
#     response = collection.add_object(wv_object)
#     assert response is None
#     response = client.query.aggregate(collection_name).with_meta_count().do()
#     assert response["data"]["Aggregate"][collection_name][0]["meta"]["count"] == 1
#     collection.add_object({"test": "AnotherObject"})
#     response = client.query.aggregate(collection_name).with_meta_count().do()
#     assert response["data"]["Aggregate"][collection_name][0]["meta"]["count"] == 2
#     client.schema.delete_class(collection_name)
#
#
# @pytest.mark.parametrize(
#     "n_chunks, source_object_data, collection_name",
#     [
#         (1, db.SourceData(path="youTube", body="Why, hello there"), "TestCollectionA"),
#         (10, db.SourceData(path="youTube", body="Why, hello there"), "TestCollectionA"),
#     ]
# )
# def test_add_chunks(n_chunks, source_object_data, collection_name):
#     chunks = ["A" * (i+1) for i in range(n_chunks)]
#
#     # Connect to Weaviate
#     client = db.connect_weaviate()  # TODO - replace this with test client
#
#     # Instantiate a collection
#     if client.schema.exists(collection_name):
#         client.schema.delete_class(collection_name)
#     collection = db.Collection(client=client, collection_name=collection_name)
#
#     # Tests
#     collection.import_chunks(chunks, source_object_data)
#     response = client.query.aggregate(collection_name).with_meta_count().do()
#     assert response["data"]["Aggregate"][collection_name][0]["meta"]["count"] == n_chunks
#     client.schema.delete_class(collection_name)
#
#
# @pytest.mark.parametrize(
#     "n_chunks, collection_name, chunk_number_offset",
#     [
#         (1, "TestCollectionA", 0),
#         (5, "TestCollectionA", 0),
#     ]
# )
# def test_add_data(n_chunks, collection_name, chunk_number_offset):
#     source_object_data = db.SourceData(
#         path="youTube",
#         body="A" * preprocessing.MAX_CHUNK_CHARS * n_chunks
#     )
#
#     # Connect to Weaviate
#     client = db.connect_weaviate()  # TODO - replace this with test client
#
#     # Instantiate a collection
#     if client.schema.exists(collection_name):
#         client.schema.delete_class(collection_name)
#     collection = db.Collection(client=client, collection_name=collection_name)
#
#     # Tests
#     collection.add_data(source_object_data, chunk_number_offset=chunk_number_offset)
#     response = client.query.aggregate(collection_name).with_meta_count().do()
#     assert response["data"]["Aggregate"][collection_name][0]["meta"]["count"] == n_chunks + 1
#     client.schema.delete_class(collection_name)
#     # TODO - add test for offset
#
#
# @pytest.mark.parametrize(
#     "source_path, n_chunks, chunk_number_offset, source_title, collection_name",
#     [
#         ("YouTube", 1, 0, "YouTubeVideo", "TestCollectionA"),
#         ("YouTube", 5, 0, "YouTubeVideo", "TestCollectionA"),
#     ]
# )
# def test_add_text(source_path, n_chunks, chunk_number_offset, source_title, collection_name):
#     # Connect to Weaviate
#     client = db.connect_weaviate()  # TODO - replace this with test client
#
#     # Instantiate a collection
#     if client.schema.exists(collection_name):
#         client.schema.delete_class(collection_name)
#     collection = db.Collection(client=client, collection_name=collection_name)
#
#     # Tests
#     source_text = "A" * preprocessing.MAX_CHUNK_CHARS * n_chunks
#     collection.add_text(
#         source_path=source_path,
#         source_text=source_text,
#         source_title=source_title,
#         chunk_number_offset=chunk_number_offset
#     )
#     response = client.query.aggregate(collection_name).with_meta_count().do()
#     assert response["data"]["Aggregate"][collection_name][0]["meta"]["count"] == n_chunks + 1
#     client.schema.delete_class(collection_name)
#     # TODO - add test for offset


@pytest.mark.parametrize(
    "youtube_url, collection_name",
    [
        ("https://youtu.be/sNw40lEhaIQ", "TestCollectionA")
    ]
)
def test_add_from_youtube(youtube_url, collection_name):
    # Connect to Weaviate
    client = db.connect_weaviate()  # TODO - replace this with test client

    # Instantiate a collection
    if client.schema.exists(collection_name):
        client.schema.delete_class(collection_name)
    collection = db.Collection(client=client, collection_name=collection_name)

    # Tests
    collection.add_from_youtube(youtube_url)
    response = (
        client.query.aggregate(collection_name)
        .with_where({
            "path": ["source_path"],
            "operator": "Equal",
            "valueText": youtube_url
        })
        .with_meta_count()
        .do()
    )
    count = response['data']['Aggregate'][collection_name][0]['meta']['count']
    assert count > 0

    response = (
        client.query.get(collection_name, [i.name for i in fields(db.ChunkData)])
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

    for row in response['data']['Get'][collection_name]:
        assert len(row['chunk_text']) > 0
