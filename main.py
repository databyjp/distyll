import weaviate
from weaviate import Client
from dataclasses import dataclass
from weaviate.util import generate_uuid5
from utils import chunk_text, build_weaviate_object, load_wiki_page, load_data

WV_CLASS = "Knowledge_chunk"
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


@dataclass
class SourceData:
    source_path: str
    source_text: str


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


def add_wiki_article(
    client: Client, wiki_title: str
) -> int:
    """
    Add a wikipedia article to the DB
    :param client:
    :param wiki_title: Title of the Wiki page to add
    :return:
    """
    src_data = SourceData(
        source_path=wiki_title,
        source_text=load_wiki_page(wiki_title)
    )
    return add_to_weaviate(src_data, client)


def add_text_file(
    client: Client, text_file_path: str
) -> int:
    """
    Add a text file to the DB
    :param client:
    :param text_file_path: Local path to the text file to add
    :return:
    """
    from pathlib import Path
    filepath = Path(text_file_path)
    src_data = SourceData(
        source_path=text_file_path,
        source_text=load_data(filepath)
    )
    return add_to_weaviate(src_data, client)


def add_to_weaviate(
        source_data: SourceData, client: Client,
) -> int:
    """
    Add objects to Weaviate
    :param source_data: DataClass of source data, with "source_path" and "source_text"
    :param client: Weaviate client object for adding object
    :return:
    """
    chunks = chunk_text(source_data.source_text)
    object_data = {
        "source_path": str(source_data.source_path)
    }
    counter = 0
    with client.batch() as batch:
        for c in chunks:
            wv_obj = build_weaviate_object(c, object_data)
            batch.add_data_object(
                class_name=WV_CLASS,
                data_object=wv_obj,
                uuid=generate_uuid5(wv_obj)
            )
            counter += 1

    return counter  # TODO add error handling
