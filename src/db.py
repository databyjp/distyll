from dataclasses import dataclass, fields
from typing import Optional, Union, List, Dict
import utils
import rag
from utils import MAX_N_CHUNKS, PROMPTS
import weaviate
from weaviate import Client
from weaviate.util import generate_uuid5
import openai
import os

openai.api_key = os.environ["OPENAI_APIKEY"]

COLLECTION_NAME_CHUNKS = "Knowledge_chunk"
COLLECTION_NAME_SOURCES = "Source"
COLLECTION_BODY_PROPERTY = "body"
COLLECTION_PROPERTIES = ["source_title", "source_path", COLLECTION_BODY_PROPERTY]

CHUNK_NO_COL = "chunk_number"

CLASS_CONFIG = {
    "vectorizer": "text2vec-openai",
    "moduleConfig": {
        "generative-openai": {}
    },
}


@dataclass
class SourceData:
    source_title: Optional[str]
    source_path: str
    source_text: str


def create_class_obj(collection_name, properties):
    """
    Create a object for a particular class
    :param collection_name:
    :param properties:
    :return:
    """
    return {
        "class": collection_name,
        "properties": properties,
        **CLASS_CONFIG
    }


DEFAULT_CLASSES = {
    "classes": [
        create_class_obj(
            COLLECTION_NAME_CHUNKS,
            [{"name": field, "dataType": ["text"]} for field in COLLECTION_PROPERTIES]
        ),
        create_class_obj(
            COLLECTION_NAME_SOURCES,
            [{"name": field, "dataType": ["text"]} for field in COLLECTION_PROPERTIES]
        ),
    ]
}


def source_filter(value):
    """
    Build a filter for convenience
    :param value:
    :return:
    """
    return {
        "path": "source_path",
        "operator": "Equal",
        "valueText": value
    }


class Collection:

    def __init__(self, client: Client, target_class: str):
        self.client = client
        self.target_class = target_class

    def add_object(self, data_object):
        """
        Add an object to the collection
        :param data_object:
        :return:
        """
        self.client.data_object.create(
            data_object=data_object,
            class_name=self.target_class
        )

    def get_all_property_names(self) -> List[str]:
        """
        Get property names from the Weaviate collection
        :return:
        """
        class_schema = self.client.schema.get(self.target_class)
        return [p["name"] for p in class_schema["properties"]]

    def get_entry_count(self, value: str) -> int:
        """
        Get the number of objects available in a collection matching the value/name
        :param value:
        :param property_name:
        :return:
        """
        response = (
            self.client.query.aggregate(self.target_class)
            .with_where(source_filter(value))
            .with_meta_count()
            .do()
        )
        return response["data"]["Aggregate"][self.target_class][0]["meta"]["count"]

    def get_total_obj_count(self) -> Union[int, str]:
        """
        Get a total object count of this collection
        :return:
        """
        res = self.client.query.aggregate(self.target_class).with_meta_count().do()
        return res["data"]["Aggregate"][self.target_class][0]["meta"]["count"]

    def _import_chunks(self, chunks: List[str], base_object_data: Dict, chunk_number_offset: int):
        """
        Import text chunks via batch import process
        :param chunks:
        :param base_object_data:
        :param chunk_number_offset:
        :return:
        """
        counter = 0
        self.client.batch.configure(batch_size=100)
        with self.client.batch as batch:
            for i, c in enumerate(chunks):
                wv_obj = build_weaviate_object(c, base_object_data, chunk_number=i+chunk_number_offset)
                batch.add_data_object(
                    class_name=self.target_class,
                    data_object=wv_obj,
                    uuid=generate_uuid5(wv_obj)
                )
                counter += 1
        return counter

    def _add_to_database(
            self, source_data: SourceData, chunk_number_offset: int = 0
    ) -> int:
        """
        Add objects to Weaviate
        :param source_data: DataClass of source data, with "source_path" and "source_text"
        :return:
        """
        chunks = utils.chunk_text(source_data.source_text)

        object_data = {
            "source_path": str(source_data.source_path),
            "source_title": getattr(source_data, "source_title", None)
        }

        counter = self._import_chunks(chunks, object_data, chunk_number_offset)

        return counter

    def add_text(self, source_path: str, source_text: str, chunk_number_offset: int = 0, source_title: Optional[str] = None):
        """
        Add data from text input
        :param source_path:
        :param source_text:
        :return:
        """
        src_data = SourceData(
            source_path=source_path,
            source_text=source_text,
            source_title=source_title,
        )
        print(f"Adding the data from {source_path}")
        return self._add_to_database(src_data, chunk_number_offset=chunk_number_offset)

    def add_pdf(self, pdf_url: str) -> int:
        """
        Add a PDF to the database
        :param pdf_url:
        :return:
        """
        text_content = utils.download_and_parse_pdf(pdf_url)
        return self.add_text(
            source_path=pdf_url,
            source_text=text_content,
            source_title=pdf_url
        )

    # def add_from_youtube(self, youtube_url: str) -> int:
    #     """
    #     Add the transcript of a YouTube video to Weaviate
    #     :param youtube_url:
    #     :return:
    #     """
    #     # Grab the YouTube Video & convert to transcript text
    #     tmp_outpath = 'temp_audio.mp3'
    #     utils.download_audio(youtube_url, tmp_outpath)
    #     transcript_texts = utils._get_transcripts_from_audio_file(tmp_outpath)
    #
    #     # Ingest transcripts into the database
    #     obj_count = 0
    #     for transcript_text in transcript_texts:
    #         obj_count += self.add_text(
    #             source_path=youtube_url, source_text=transcript_text,
    #             chunk_number_offset=obj_count, source_title=utils.get_youtube_title(youtube_url)
    #         )
    #
    #     # Cleanup - if original file still exists
    #     if os.path.exists(tmp_outpath):
    #         os.remove(tmp_outpath)
    #
    #     return obj_count

    def summarize_entry(
            self, source_path: str,
    ) -> str:
        """
        Summarize all objects for a particular entry
        :param source_path:
        :return:
        """
        entry_count = self.get_entry_count(source_path)
        print(f"entry count: {entry_count}")
        property_names = self.get_all_property_names()

        chunk_texts = list()
        summary_sets = (entry_count // MAX_N_CHUNKS) + 1
        for i in range(summary_sets):
            response = (
                self.client.query.get(self.target_class, property_names)
                .with_where(source_filter(source_path))
                .with_offset((i * MAX_N_CHUNKS))
                .with_limit(MAX_N_CHUNKS)
                .do()
            )
            chunk_texts_subset = [r[COLLECTION_BODY_PROPERTY] for r in response["data"]["Get"][self.target_class]]
            chunk_texts += chunk_texts_subset
        print(f"passing chunks: {len(chunk_texts)}")
        return utils.summarize_multiple_paragraphs(chunk_texts)


def add_default_classes(client: Client) -> bool:
    """
    Add the default class to be used with this knowledge base DB
    :param client:
    :return:
    """
    for c in DEFAULT_CLASSES["classes"]:
        if not client.schema.exists(c["class"]):
            print(f"Creating a new class: {c['class']}")
            client.schema.create_class(c)
        else:
            print(f"Found {c['class']} in the schema. Skipping class creation")
            return True


def instantiate_weaviate(version: str = "latest") -> Client:
    """
    :param version: Weaviate version to use
    Instantiate Weaviate
    :return:
    """
    from weaviate import EmbeddedOptions

    # Replace this with other client instantiation method to connect to another instance of Weaviate
    client = weaviate.Client(
        embedded_options=EmbeddedOptions(version=version),
    )

    return client


def start_db(version: str = "latest", custom_client: Client = None) -> Client:
    """
    Instantiate this knowledge database & return the client object
    :param version: Version for pulling in specific versions (with Embedded Weaviate instantiation)
    :param custom_client: Pass on a custom client for use
    :return:
    """
    if not custom_client:
        client = instantiate_weaviate(version)
    else:
        client = custom_client

    add_default_classes(client)
    return client


def reinitialize_db(client: Client):
    """
    Delete existing data
    :param client:
    :return:
    """
    for c in DEFAULT_CLASSES["classes"]:
        client.schema.delete_class(c["class"])
    add_default_classes(client)


def build_weaviate_object(chunk_body: str, object_data: dict, chunk_number: int = None) -> dict:
    """
    Build a Weaviate object after chunking
    :param chunk_body: Chunk text
    :param object_data: Any other object data to be passed
    :param chunk_number: Chunk number
    :return:
    """
    wv_object = dict()
    for k, v in object_data.items():
        wv_object[k] = v
    wv_object[COLLECTION_BODY_PROPERTY] = chunk_body

    if chunk_number is not None:
        wv_object[CHUNK_NO_COL] = chunk_number
    return wv_object


# ===== QUERIES =====
def ask_object(client: Client, source_path: str, search_string: str, question: Optional[str] = None) -> (str, Dict):
    print("Getting summary")
    if question is None:
        question = search_string

    summmary_response = (
        client.query.get(COLLECTION_NAME_SOURCES, "body")
        .with_where(source_filter(source_path))
        .with_limit(1)
        .do()
    )
    summary = summmary_response["data"]["Get"][COLLECTION_NAME_SOURCES][0]["body"]

    print("Getting chunks")
    chunks_response = (
        client.query.get(COLLECTION_NAME_CHUNKS, "body")
        .with_where(source_filter(source_path))
        .with_near_text({"concepts": search_string})
        .with_limit(MAX_N_CHUNKS)
        .do()
    )
    chunks = [c["body"] for c in chunks_response["data"]["Get"][COLLECTION_NAME_CHUNKS]]

    paragraphs = {
        "summary": summary,
        "chunks": chunks
    }
    print(f"Got {len(paragraphs['chunks'])} paragraphs")

    prompt = f"""
    Based on the following text, answer {question}.
    If the text does not contain required information,
    do not answer the question, and indicate as such to the user.
    """

    full_prompt = prompt + ("=" * 10) + str(paragraphs)
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system",
             "content": """
                You are a helpful assistant who can summarize information very well in
                clear, concise language without resorting to domain-specific jargon.
                """
             },
            {"role": "user", "content": full_prompt}
        ]
    )
    return completion.choices[0].message["content"], paragraphs


def summarize_multiple_paragraphs(paragraphs: List) -> Union[str, List]:
    """
    Helper function for summarizing multiple paragraphs using an LLM
    :param paragraphs:
    :return:
    """
    paragraph_count = len(paragraphs)
    if paragraph_count < MAX_N_CHUNKS:
        print(f"Summarizing {paragraph_count} paragraphs")
        source_data = rag.RAGBase(paragraphs)
        return source_data.summarize()
    else:
        print(f"{paragraph_count} paragraphs is too many - let's split them up")
        summary_sets = (paragraph_count // MAX_N_CHUNKS) + 1
        subsets = [
            paragraphs[MAX_N_CHUNKS*i:MAX_N_CHUNKS*(i+1)] for i in range(summary_sets)
        ]
        summaries = list()
        for i, subset in enumerate(subsets):
            print(f"Summarizing set {i} of {len(subsets)}")
            source_data = rag.RAGBase(paragraphs)
            summaries.append(source_data.summarize())
        return summarize_multiple_paragraphs(summaries)
