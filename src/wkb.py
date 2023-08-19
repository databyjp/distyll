from dataclasses import dataclass
from typing import Optional, Union, List, Dict

import weaviate
from weaviate import Client
import openai
import os

from weaviate.util import generate_uuid5

import utils
from utils import MAX_N_CHUNKS, PROMPTS

openai.api_key = os.environ["OPENAI_APIKEY"]

COLLECTION_NAME_CHUNKS = "Knowledge_chunk"
COLLECTION_NAME_SOURCES = "Source"
DB_BODY_PROPERTY = "body"
CHUNK_NO_COL = "chunk_number"

CLASS_CONFIG = {
    "vectorizer": "text2vec-openai",
    "moduleConfig": {
        "generative-openai": {}
    },
}


def create_class_obj(collection_name):
    return {
        "class": collection_name,
        **CLASS_CONFIG
    }


DEFAULT_CLASSES = {
    "classes": [
        create_class_obj(COLLECTION_NAME_CHUNKS),
        # create_class_obj(COLLECTION_NAME_SOURCES),
    ]
}


@dataclass
class SourceData:
    source_title: Optional[str]
    source_path: str
    source_text: str


class Collection:

    def __init__(self, client: Client, target_class: str):
        self.client = client
        self.target_class = target_class

    def _get_all_property_names(self) -> List[str]:
        """
        Get property names from the Weaviate collection
        :return:
        """
        class_schema = self.client.schema.get(self.target_class)
        return [p["name"] for p in class_schema["properties"]]

    def set_apikey(self, openai_key: Optional[str] = None, cohere_key: Optional[str] = None):

        if openai_key:
            self.client._connection._headers["x-openai-api-key"] = openai_key

        if cohere_key:
            self.client._connection._headers["x-cohere-api-key"] = cohere_key

    def reinitialize_db(self):
        self.client.schema.delete_class(self.target_class)
        add_default_classes(self.client)

    def _get_entry_count(self, source_path: str) -> int:
        """
        Get the number of objects available in a source path
        :param source_path:
        :return:
        """
        response = (
            self.client.query.aggregate(self.target_class)
            .with_where({
                "path": "source_path",
                "operator": "Equal",
                "valueText": source_path
            })
            .with_meta_count()
            .do()
        )
        return response["data"]["Aggregate"][self.target_class][0]["meta"]["count"]

    def get_total_obj_count(self) -> Union[int, str]:
        res = self.client.query.aggregate(self.target_class).with_meta_count().do()
        return res["data"]["Aggregate"][self.target_class][0]["meta"]["count"]

    def _import_chunks_via_batch(self, chunks: List[str], base_object_data: Dict, chunk_number_offset: int):
        """
        Import text chunks via batch import process
        :param chunks:
        :param base_object_data:
        :param chunk_number_offset:
        :return:
        """
        counter = 0
        with self.client.batch() as batch:
            for i, c in enumerate(chunks):
                wv_obj = build_weaviate_object(c, base_object_data, chunk_number=i+chunk_number_offset)
                batch.add_data_object(
                    class_name=self.target_class,
                    data_object=wv_obj,
                    uuid=generate_uuid5(wv_obj)
                )
                counter += 1
        return counter

    def _add_to_weaviate(
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

        counter = self._import_chunks_via_batch(chunks, object_data, chunk_number_offset)

        return counter

    def _add_text(self, source_path: str, source_text: str, chunk_number_offset: int = 0, source_title: Optional[str] = None):
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
        return self._add_to_weaviate(src_data, chunk_number_offset=chunk_number_offset)

    def add_pdf(self, pdf_url: str) -> int:
        text_content = utils.download_and_parse_pdf(pdf_url)
        return self._add_text(
            source_path=pdf_url,
            source_text=text_content,
            source_title=pdf_url
        )

    def _generative_with_object(
            self,
            source_path: str,
            query_str: str,
            topic_prompt: str,
            obj_limit: int = MAX_N_CHUNKS, max_distance: float = 0.30,
            debug: bool = False
    ) -> dict:
        """
        Get a grouped set of results and *something*
        :param source_path:
        :param topic_prompt:
        :param obj_limit:
        :param max_distance:
        :param debug:
        :return:
        """
        response = (
            self.client.query.get(self.target_class, self._get_all_property_names())
            .with_near_text(
                {
                    "concepts": [query_str],
                    "distance": max_distance
                }
            )
            .with_where(
                {
                    "path": ["source_path"],
                    "operator": "Equal",
                    "valueText": source_path
                }
            )
            .with_limit(obj_limit)
            .with_sort({"path": [CHUNK_NO_COL], "order": "asc"})
            .with_generate(
                grouped_task=PROMPTS.RAG_PREAMBLE + topic_prompt + ("=" * 10)
            )
            .do()
        )

        if debug:
            return response
        else:
            return response

    def _get_generated_result(self, weaviate_response: dict) -> str:
        """
        Parse the generated results
        :param weaviate_response:
        :return:
        """
        return weaviate_response["data"]["Get"][self.target_class][0]["_additional"]["generate"]["groupedResult"]

    def ask_object(self, source_path: str, question: str, topic_prompt: Optional[str] = None):
        if topic_prompt is None:
            response = self._generative_with_object(source_path=source_path, query_str=question, topic_prompt=question)
        else:
            response = self._generative_with_object(source_path=source_path, query_str=question, topic_prompt=topic_prompt)
        return (
            response["data"]["Get"][self.target_class],
            self._get_generated_result(response)
        )

    def summarize_entry(
            self, source_path: str,
            debug: bool = False
    ) -> Union[str, List]:
        """
        Summarize all objects for a particular entry
        :param source_path:
        :param debug:
        :return:
        """
        entry_count = self._get_entry_count(source_path)
        where_filter = {
            "path": ["source_path"],
            "operator": "Equal",
            "valueText": source_path
        }
        property_names = self._get_all_property_names()
        topic_prompt = PROMPTS.SUMMARIZE

        section_summaries = list()
        summary_sets = (entry_count // MAX_N_CHUNKS) + 1
        # summary_sets = min(summary_sets, 3)  # For debugging
        for i in range(summary_sets):
            print(f"Summarizing set {i+1} of {summary_sets}")
            response = (
                self.client.query.get(self.target_class, property_names)
                .with_where(where_filter)
                .with_offset((i * MAX_N_CHUNKS))
                .with_limit(MAX_N_CHUNKS)
                .with_generate(
                    grouped_task=topic_prompt
                )
                .do()
            )
            section_summaries.append(response)

        if debug:
            return section_summaries
        else:
            section_summaries = [self._get_generated_result(s) for s in section_summaries]
            return utils.summarize_multiple_paragraphs(section_summaries)


def add_default_classes(client: Client) -> bool:
    """
    Add the default class to be used with this knowledge base DB
    :param client:
    :return:
    """
    for c in DEFAULT_CLASSES["classes"]:
        if not client.schema.exists(c["class"]):
            print("Creating a new class:")
            client.schema.create(DEFAULT_CLASSES)
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
    wv_object[DB_BODY_PROPERTY] = chunk_body

    if chunk_number is not None:
        wv_object[CHUNK_NO_COL] = chunk_number
    return wv_object
