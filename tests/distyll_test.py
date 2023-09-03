from dataclasses import fields
import pytest
import distyll
import preprocessing


@pytest.fixture(scope="session")
def client():
    return distyll.connect_to_default_weaviate()


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


def test_get_all_property_names(client, testclasses):
    source_class, chunk_class = testclasses

    # Delete existing test data
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    db = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)
    source_properties = distyll._get_all_property_names(client, source_class)
    for s in source_properties:
        assert s in [f.name for f in fields(distyll.SourceData)]
    chunk_properties = distyll._get_all_property_names(client, chunk_class)
    for s in chunk_properties:
        assert s in [f.name for f in fields(distyll.ChunkData)]


def test_collection_instantiation(client, testclasses):
    source_class, chunk_class = testclasses

    # Delete existing test data
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    db = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)
    for c in [source_class, chunk_class]:
        assert client.schema.exists(c)
    assert db.client == client

    # Clean up
    for c in [source_class, chunk_class]:
        client.schema.delete_class(c)


def test_get_total_object_counts(client, testclasses):
    source_class, chunk_class = testclasses

    # Delete existing test data
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    db = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)
    object_counts = db.get_total_object_counts()
    for k, v in object_counts.items():
        assert v == 0

    db._add_object(distyll.ChunkData(source_path="somewhere", chunk_text="something", chunk_number=0), chunk_class)
    object_counts = db.get_total_object_counts()
    assert object_counts['chunk_count'] == 1
    assert object_counts['source_count'] == 0

    db._add_object(distyll.ChunkData(source_path="somewhere", chunk_text="something", chunk_number=0), chunk_class)
    object_counts = db.get_total_object_counts()
    assert object_counts['chunk_count'] == 2
    assert object_counts['source_count'] == 0

    db._add_object(distyll.SourceData(path="somewhere", body="something"), source_class)
    assert object_counts['chunk_count'] == 2
    assert object_counts['source_count'] == 1

    # Clean up
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

    # Delete existing test data
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    db = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)

    # Tests
    for c in [source_class, chunk_class]:
        response = db._add_object(wv_object, c)
        assert response is True
        response = client.query.aggregate(c).with_meta_count().do()
        assert response["data"]["Aggregate"][c][0]["meta"]["count"] == 1

        response = db._add_object(wv_object, c)
        assert response is None
        response = client.query.aggregate(c).with_meta_count().do()
        assert response["data"]["Aggregate"][c][0]["meta"]["count"] == 1

        db._add_object({"test": "AnotherObject"}, c)
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

    # Delete existing test data
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    db = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)

    # Tests
    db.import_chunks(chunks, source_object_data)
    response = client.query.aggregate(chunk_class).with_meta_count().do()
    assert response["data"]["Aggregate"][chunk_class][0]["meta"]["count"] == n_chunks

    # Clean up
    for c in [source_class, chunk_class]:
        client.schema.delete_class(c)


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

    # Delete existing test data
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    db = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)

    # Tests
    db.add_data(source_object_data, chunk_number_offset=chunk_number_offset)
    response = client.query.aggregate(chunk_class).with_meta_count().do()
    assert response["data"]["Aggregate"][chunk_class][0]["meta"]["count"] == n_chunks + 1

    # Clean up
    for c in [source_class, chunk_class]:
        client.schema.delete_class(c)

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

    # Delete existing test data
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    db = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)

    # Tests
    source_text = "A" * preprocessing.MAX_CHUNK_CHARS * n_chunks
    db.add_text(
        source_path=source_path,
        source_text=source_text,
        source_title=source_title,
        chunk_number_offset=chunk_number_offset
    )
    response = client.query.aggregate(chunk_class).with_meta_count().do()
    assert response["data"]["Aggregate"][chunk_class][0]["meta"]["count"] == n_chunks + 1

    # Clean up
    for c in [source_class, chunk_class]:
        client.schema.delete_class(c)

    # TODO - add test for offset


def get_filtered_count(collection_name, source_path):
    response = (
        client.query.aggregate(collection_name)
        .with_where({
            "path": ["source_path"],
            "operator": "Equal",
            "valueText": source_path
        })
        .with_meta_count()
        .do()
    )
    count = response['data']['Aggregate'][collection_name][0]['meta']['count']
    return count


def get_first_n_chunk_objects(collection_name, source_path):
    response = (
        client.query.get(collection_name, [i.name for i in fields(distyll.ChunkData)])
        .with_where({
            "path": ["source_path"],
            "operator": "Equal",
            "valueText": source_path
        })
        .with_sort({
            'path': ['chunk_number'],
            'order': 'asc'
        })
        .with_limit(5)
        .do()
    )
    return response['data']['Get'][collection_name]


@pytest.mark.parametrize(
    "youtube_url, source_type",
    [
        ("https://youtu.be/ni3T4vStzBI", "youtube"),
        ("https://arxiv.org/pdf/1706.03762", "pdf"),
        ("https://arxiv.org/pdf/1706.03762", "arxiv"),
    ]
)
def test_add_from_media(client, testclasses, source_url, source_type):
    source_class, chunk_class = testclasses

    # Delete existing test data
    for c in [source_class, chunk_class]:
        if client.schema.exists(c):
            client.schema.delete_class(c)

    # Instantiate a collection
    db = distyll.DBConnection(client=client, source_class=source_class, chunk_class=chunk_class)

    # Select the right data importer function
    if source_type == 'youtube':
        db.add_from_youtube(source_url)
    elif source_type == 'pdf':
        db.add_pdf(source_url)
    elif source_type == 'arxiv':
        db.add_arxiv(source_url)
    else:
        raise ValueError("Input type unknown!")

    # Tests
    assert get_filtered_count(collection_name=source_class, source_path=source_url) == 1
    assert get_filtered_count(collection_name=chunk_class, source_path=source_url) > 1
    db.add_pdf(pdf_url=source_url)
    assert get_filtered_count(collection_name=source_class, source_path=source_url) == 1

    chunk_objects = get_first_n_chunk_objects(chunk_class, source_url)
    for row in chunk_objects:
        assert len(row['chunk_text']) > 0

    # Clean up
    for c in [source_class, chunk_class]:
        client.schema.delete_class(c)
