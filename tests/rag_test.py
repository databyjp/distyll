import weaviate
from weaviate.util import generate_uuid5
import pytest
import os
import rag

client = weaviate.Client(
    embedded_options=weaviate.EmbeddedOptions(),
    additional_headers={
        "X-OpenAI-Api-Key": os.environ["OPENAI_APIKEY"]
    }
)

TEST_COLLECTION_NAME = "TestCollection"

with open("demodata/short_transport_layer.txt", "r") as f:
    dummy_text = f.read()


@pytest.mark.parametrize(
    "source_text",
    [
        "Once more into the breach, dear friends.",
        [
            "If I ask you about love, you’d probably quote me a sonnet.",
            "But you’ve never looked at a woman and been totally vulnerable."
        ]
    ]
)
def test_prompt_gen(source_text):
    if type(source_text) == list:
        source_text = ' '.join(source_text)

    test_prompts = rag.PromptGenerator(source_text)
    assert source_text in test_prompts.for_summarisation()
    assert source_text in test_prompts.for_glossary()
    assert source_text in test_prompts.for_revision_quiz()
    assert source_text in test_prompts.for_revision_quiz_answers()
    assert source_text in test_prompts.for_question_answer()
    assert source_text in test_prompts.for_search_string()


@pytest.mark.parametrize(
    "collection_name, data_object",
    [
        (TEST_COLLECTION_NAME, {rag.OUTPUT_PROPERTY_NAME: dummy_text})
    ]
)
def test_add_and_get(collection_name, data_object):
    # Set up required variables & cleanup if needed
    retrieved_data = rag.RAGBase(source_text=dummy_text, weaviate_client=client)
    uuid = retrieved_data.uuid
    if client.data_object.exists(uuid=uuid, class_name=collection_name):
        client.data_object.delete(uuid=uuid, class_name=collection_name)

    # Build retrieved data object & add to database
    returned_obj = retrieved_data.add_to_db(
        collection_name=collection_name,
        data_object=data_object,
    )

    # Check object creation
    assert client.data_object.exists(uuid=uuid, class_name=collection_name)
    assert returned_obj["id"] == uuid

    # Check get
    object_from_db = retrieved_data.get_from_db(collection_name=collection_name)
    assert object_from_db["id"] == returned_obj["id"]

    # Clean up
    client.data_object.delete(uuid=uuid, class_name=collection_name)

    # Check get after deletion
    object_from_db = retrieved_data.get_from_db(collection_name=collection_name)
    assert object_from_db is None
    assert client.data_object.exists(uuid=uuid, class_name=collection_name) is False


@pytest.mark.parametrize(
    "collection_name, prompt",
    [
        (TEST_COLLECTION_NAME, "Write a short haiku")
    ]
)
def test_get_or_generate(collection_name, prompt):
    retrieved_data = rag.RAGBase(source_text=dummy_text, weaviate_client=client)

    uuid = generate_uuid5(prompt)
    if client.data_object.exists(uuid=uuid, class_name=collection_name):
        client.data_object.delete(uuid=uuid, class_name=collection_name)

    object_from_db = retrieved_data.get_from_db(collection_name=collection_name)
    assert object_from_db is None
    new_object = retrieved_data.get_or_generate(collection_name, prompt)
    assert new_object is not None
    retrieved_object = retrieved_data.get_or_generate(collection_name, prompt)
    assert new_object == retrieved_object
    client.data_object.delete(uuid=new_object['id'], class_name=collection_name)


@pytest.mark.parametrize(
    "source_text",
    [
        dummy_text
    ]
)
def test_basic_rag_functions(source_text):
    retrieved_data = rag.RAGBase(source_text=source_text, weaviate_client=client)
    for collection_name in rag.CollectionNames:
        retrieved_data.delete_existing(collection_name.value)

    # Test creating new derivative objects
    glossary = retrieved_data.get_glossary()
    summary = retrieved_data.summarize()
    revision_quiz = retrieved_data.get_revision_quiz()
    revision_quiz_answers = retrieved_data.get_revision_quiz_answers()
    for output in [glossary, summary, revision_quiz, revision_quiz_answers]:
        assert rag.OUTPUT_PROPERTY_NAME in output['properties'].keys()
        assert len(output['properties'][rag.OUTPUT_PROPERTY_NAME]) > 50

    # Test deletion
    for collection_name in rag.CollectionNames:
        # When first deleting, we should get True after deletion
        deleted = retrieved_data.delete_existing(collection_name.value)
        assert deleted is True
        # When deleting again, we should get None
        deleted_again = retrieved_data.delete_existing(collection_name.value)
        assert deleted_again is None
