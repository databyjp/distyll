import weaviate
from weaviate import Client
from utils import chunk_text, build_weaviate_object, load_wiki_page, SourceData, WV_CLASS

BASE_CLASS_OBJ = {
    "class": WV_CLASS,
    "vectorizer": "text2vec-openai",
    "moduleConfig": {
        "generate-openai": []
    }
}
WV_SCHEMA = {
    "classes": [
        BASE_CLASS_OBJ
    ]
}


def instantiate_weaviate() -> Client:
    """
    Instantiate Weaviate
    :return:
    """
    client = weaviate.Client("http://localhost:8080")
    return client


def add_default_class(client: Client) -> bool:
    """
    Add the default class to be used with this knowledge base DB
    :param client:
    :return:
    """
    if not client.schema.contains({"class": WV_CLASS, "properties": []}):
        print("Creating a new class:")
        client.schema.create(WV_SCHEMA)
        return True
    else:
        print("Found class. Skipping class creation")
        return True


def start_db() -> Client:
    """
    Instantiate this knowledge database & return the client object
    :return:
    """
    client = instantiate_weaviate()
    add_default_class(client)
    return client

