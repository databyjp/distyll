from dataclasses import fields
import pytest
import distyll
import preprocessing


@pytest.fixture(scope="session")
def client():
    return distyll.connect_weaviate()


@pytest.fixture(scope="session")
def testclasses():
    return 'TestSource', 'TestChunk'


def test_instantiation(client):
    assert client.is_ready() is True


@pytest.mark.parametrize(
    "collection_config",
    [
        {'class': 'TestCollectionA'}
    ]
)
def test_class_addition(client, collection_config):
    # Prep
    collection_name = collection_config['class']
    client.schema.delete_class(collection_name)

    # Add new class
    response = distyll._add_class_if_not_present(client, collection_config)
    assert response is True  # Should be True if newly added
    response = distyll._add_class_if_not_present(client, collection_config)
    assert response is None  # Should be None if it exists

    # Clean up
    client.schema.delete_class(collection_name)


# TODO - add tests for get_all_property_names


def test_collection_instantiation(client, testclasses):
    source_class, chunk_class = testclasses

    # Connect to Weaviate
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)
    for c in [source_class, chunk_class]:
        assert client.schema.exists(c)
    assert collection.client == client
    # TODO - add tests for collection properties

    for c in [source_class, chunk_class]:
        client.schema.delete_class(c)


@pytest.mark.parametrize(
    "wv_object",
    [
        ({"name": "value"})
    ]
)
def test_add_object(client, wv_object, testclasses):
    source_class, chunk_class = testclasses

    # Connect to Weaviate
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)

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
    "n_chunks, source_object_data",
    [
        (1, distyll.SourceData(path="youTube", body="Why, hello there")),
        (10, distyll.SourceData(path="youTube", body="Why, hello there")),
    ]
)
def test_add_chunks(client, n_chunks, source_object_data, testclasses):
    source_class, chunk_class = testclasses
    chunks = ["A" * (i+1) for i in range(n_chunks)]

    # Connect to Weaviate
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)

    # Tests
    collection.import_chunks(chunks, source_object_data)
    response = client.query.aggregate(chunk_class).with_meta_count().do()
    assert response["data"]["Aggregate"][chunk_class][0]["meta"]["count"] == n_chunks
    client.schema.delete_class(chunk_class)


@pytest.mark.parametrize(
    "n_chunks, chunk_number_offset",
    [
        (1, 0),
        (5, 0),
    ]
)
def test_add_data(client, n_chunks, testclasses, chunk_number_offset):
    source_class, chunk_class = testclasses
    source_object_data = distyll.SourceData(
        path="youTube",
        body="A" * preprocessing.MAX_CHUNK_CHARS * n_chunks
    )

    # Connect to Weaviate
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)

    # Tests
    collection.add_data(source_object_data, chunk_number_offset=chunk_number_offset)
    response = client.query.aggregate(chunk_class).with_meta_count().do()
    assert response["data"]["Aggregate"][chunk_class][0]["meta"]["count"] == n_chunks + 1
    client.schema.delete_class(chunk_class)
    # TODO - add test for offset
    # TODO - add test to check:
    #  - if the source object was added as well as chunks
    #  - check UUID


@pytest.mark.parametrize(
    "source_path, n_chunks, chunk_number_offset, source_title",
    [
        ("YouTube", 1, 0, "YouTubeVideo"),
        ("YouTube", 5, 0, "YouTubeVideo"),
    ]
)
def test_add_text(client, source_path, n_chunks, chunk_number_offset, source_title, testclasses):
    source_class, chunk_class = testclasses

    # Connect to Weaviate
    client = distyll.connect_weaviate()  # TODO - replace this with test client

    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)

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
    "youtube_url",
    [
        "https://youtu.be/ni3T4vStzBI"
    ]
)
def test_add_from_youtube(client, youtube_url, testclasses):
    source_class, chunk_class = testclasses
    # Connect to Weaviate
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    collection = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)

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
        client.query.get(chunk_class, [i.name for i in fields(distyll.ChunkData)])
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
