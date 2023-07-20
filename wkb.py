import weaviate
from weaviate import Client
from weaviate.util import generate_uuid5
from dataclasses import dataclass
import openai
import os
from typing import Optional, Union, List, Dict
from utils import (
    download_audio, get_youtube_title, _summarize_multiple_paragraphs, _get_transcripts_from_audio_file, load_wiki_page,
    load_data, chunk_text, MAX_CHUNK_WORDS
)

openai.api_key = os.environ["OPENAI_APIKEY"]

MAX_CONTEXT_LENGTH = 1000
MAX_N_CHUNKS = 1 + (MAX_CONTEXT_LENGTH // MAX_CHUNK_WORDS)
WV_CLASS = "Knowledge_chunk"
DB_BODY_PROPERTY = "body"
CHUNK_NO_COL = "chunk_number"

BASE_CLASS_OBJ = {
    "class": WV_CLASS,
    "vectorizer": "text2vec-openai",
    "moduleConfig": {
        "generative-openai": {}
    },
}
WV_SCHEMA = {
    "classes": [
        BASE_CLASS_OBJ
    ]
}


@dataclass
class SourceData:
    source_title: Optional[str]
    source_path: str
    source_text: str


def _extract_get_results(res, target_class):
    """
    Extract results from returned GET GraphQL call from Weaviate
    :param res:
    :param target_class:
    :return:
    """
    return res["data"]["Get"][target_class]


class Collection:

    def __init__(self, client: Client, target_class: str, user_agent: str = ""):
        self.client = client
        self.target_class = target_class

        if user_agent == "":
            print("Warning - please set a user agent. Otherwise, the Wikipedia functionalities may not work.")
        else:
            self.user_agent = user_agent

    def set_apikey(self, openai_key: Optional[str] = None, cohere_key: Optional[str] = None):

        if openai_key:
            self.client._connection._headers["x-openai-api-key"] = openai_key

        if cohere_key:
            self.client._connection._headers["x-cohere-api-key"] = cohere_key

    def reinitialize_db(self):
        self.client.schema.delete_class(self.target_class)
        add_default_class(self.client)

    def get_total_obj_count(self) -> Union[int, str]:
        res = self.client.query.aggregate(self.target_class).with_meta_count().do()
        return res["data"]["Aggregate"][self.target_class][0]["meta"]["count"]

    def get_sample_objs(self, max_objs: int = 3) -> List:
        """
        Get some objects from our collection
        TODO - randomise sample
        :param max_objs:
        :return:
        """
        response = (
            self.client.query
            .get(WV_CLASS, self._get_all_property_names())
            .with_limit(max_objs)
            .do()
        )
        return _extract_get_results(response, self.target_class)

    def get_all_objs_by_path(
            self,
            source_path: str,
    ) -> List:
        """
        Get a grouped set of results and *something*
        :param source_path:
        :return:
        """
        response = (
            self.client.query.get(self.target_class, self._get_all_property_names())
            .with_where(
                {
                    "path": ["source_path"],
                    "operator": "Equal",
                    "valueText": source_path
                }
            )
            .with_sort({"path": [CHUNK_NO_COL], "order": "asc"})
            .do()
        )

        return _extract_get_results(response, self.target_class)

    def _get_all_property_names(self) -> List[str]:
        """
        Get property names from a Weaviate class
        :return:
        """
        class_schema = self.client.schema.get(self.target_class)
        return [p["name"] for p in class_schema["properties"]]

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

    def text_search(self, neartext_query: str, limit: int = 10) -> list:
        """
        Wrapper for a nearText search
        :param neartext_query:
        :param limit:
        :return:
        """
        class_response = self.client.schema.get(self.target_class)
        properties = [c["name"] for c in class_response["properties"]]
        response = (
            self.client.query.get(self.target_class, properties)
            .with_additional("distance")
            .with_near_text({"concepts": [neartext_query]})
            .with_limit(limit)
            .do()
        )
        resp_data = response["data"]["Get"][self.target_class]
        return resp_data

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
        chunks = chunk_text(source_data.source_text)

        object_data = {
            "source_path": str(source_data.source_path),
            "source_title": getattr(source_data, "source_title", None)
        }

        counter = self._import_chunks_via_batch(chunks, object_data, chunk_number_offset)

        return counter  # TODO add error handling

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

    def add_text_file(
            self, text_file_path: str
    ) -> int:
        """
        Add a text file to the DB
        :param text_file_path: Local path to the text file to add
        :return:
        """
        from pathlib import Path
        filepath = Path(text_file_path)
        return self._add_text(source_path=text_file_path, source_text=load_data(filepath), source_title=text_file_path)

    def add_wiki_article(
            self, wiki_title: str
    ) -> int:
        """
        Add a wikipedia article to the DB
        :param wiki_title: Title of the Wiki page to add
        :return:
        """
        return self._add_text(source_path=wiki_title, source_text=load_wiki_page(wiki_title, self.user_agent), source_title=wiki_title)

    def add_from_youtube(self, youtube_url: str) -> int:
        """
        Add the transcript of a YouTube video to Weaviate
        :param youtube_url:
        :return:
        """
        # Grab the YouTube Video & convert to transcript text
        tmp_outpath = 'temp_audio.mp3'
        download_audio(youtube_url, tmp_outpath)
        transcript_texts = _get_transcripts_from_audio_file(tmp_outpath)

        # Ingest transcripts into the database
        obj_count = 0
        for transcript_text in transcript_texts:
            obj_count += self._add_text(source_path=youtube_url, source_text=transcript_text,
                                        chunk_number_offset=obj_count, source_title=get_youtube_title(youtube_url))

        # Cleanup - if original file still exists
        if os.path.exists(tmp_outpath):
            os.remove(tmp_outpath)

        return obj_count

    def _get_generated_result(self, weaviate_response: dict) -> str:
        """
        Parse the generated results
        :param weaviate_response:
        :return:
        """
        return weaviate_response["data"]["Get"][self.target_class][0]["_additional"]["generate"]["groupedResult"]

    def _generative_with_query(
            self, query_str: str,
            topic_prompt: str,
            obj_limit: int = MAX_N_CHUNKS, max_distance: float = 0.28,
            debug: bool = False
    ) -> str:
        """
        Get a grouped set of results and *something*
        :param query_str:
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
            .with_limit(obj_limit)
            .with_generate(
                grouped_task=topic_prompt + ("=" * 10)
            )
            .do()
        )

        if debug:
            return response
        else:
            return self._get_generated_result(response)

    def _generative_with_object(
            self,
            source_path: str,
            query_str: str,
            topic_prompt: str,
            obj_limit: int = MAX_N_CHUNKS, max_distance: float = 0.30,
            debug: bool = False
    ) -> str:
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
                grouped_task=topic_prompt + ("=" * 10)
            )
            .do()
        )

        if debug:
            return response
        else:
            return self._get_generated_result(response)

    def summarize_entry(
            self, source_path: str,
            custom_prompt: str = None,
            debug: bool = False
    ) -> Union[str, List]:
        """
        Summarize all objects for a particular entry
        :param source_path:
        :param custom_prompt: A custom prompt or instruction for the final summary
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
        topic_prompt = f"""
        Using plain language, summarize the following as a whole into a paragraph or two of text.
        List the topics it covers, and what the reader might learn by listening to it. 
        """

        # TODO: Save summaries of long content to Weaviate so that they can be re-used
        section_summaries = list()
        for i in range((entry_count // MAX_N_CHUNKS) + 1):
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
            return _summarize_multiple_paragraphs(section_summaries, custom_prompt)

    def summarize_topic(
            self, query_str: str,
            obj_limit: int = MAX_N_CHUNKS,
            max_distance: float = 0.28,
            debug: bool = False
    ) -> str:
        """
        Given a topic, summarise relevant contents of the DB
        :param query_str:
        :param obj_limit:
        :param max_distance:
        :param debug:
        :return:
        """
        topic_prompt = f"""
        Based on the following text, summarize any information relating to {query_str} concisely.
        If the text does not contain required information, 
        do not answer the question, and indicate as such to the user.
        """

        return self._generative_with_query(
            query_str,
            topic_prompt,
            obj_limit=obj_limit, max_distance=max_distance,
            debug=debug
        )

    def ask_object(self, source_path: str, question: str, topic_prompt: Optional[str] = None):
        if topic_prompt is None:
            return self._generative_with_object(source_path=source_path, query_str=question, topic_prompt=question)
        else:
            return self._generative_with_object(source_path=source_path, query_str=question, topic_prompt=topic_prompt)

    def suggest_topics_to_learn(
            self, query_str: str,
            obj_limit: int = MAX_N_CHUNKS, max_distance: float = 0.28,
            debug: bool = False
    ) -> str:
        """
        Given a topic, suggest sub-topics to learn based on contents of the DB
        :param query_str:
        :param obj_limit:
        :param max_distance:
        :param debug:
        :return:
        """
        topic_prompt = f"""
        If the following text does includes information about {query_str}, 
        extract a list of three to six related sub-topics
        related to {query_str} that the user might learn about.
        Deliver the topics as a short list, each separated by two consecutive newlines like `\n\n`

        If the following information does not includes information about {query_str}, 
        tell the user that not enough information could not be found.
        """

        return self._generative_with_query(
            query_str,
            topic_prompt,
            obj_limit=obj_limit, max_distance=max_distance,
            debug=debug
        )


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


def start_db(version: str = "latest") -> Client:
    """
    Instantiate this knowledge database & return the client object
    :return:
    """
    client = instantiate_weaviate(version)
    add_default_class(client)
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


