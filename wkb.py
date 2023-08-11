import weaviate
from weaviate import Client
from weaviate.util import generate_uuid5
from dataclasses import dataclass
import openai
import os
from typing import Optional, Union, List, Dict
from enum import Enum
from generative import get_prompt, Prompts
from utils import (
    download_audio, get_youtube_title, _summarize_multiple_paragraphs, _get_transcripts_from_audio_file, load_wiki_page,
    load_data, chunk_text, MAX_CHUNK_WORDS
)

openai.api_key = os.environ["OPENAI_APIKEY"]

MAX_CONTEXT_LENGTH = 1000
MAX_N_CHUNKS = 1 + (MAX_CONTEXT_LENGTH // MAX_CHUNK_WORDS)
WV_CLASS = "Knowledge_chunk"
SUMMARY_CLASS = "Summary"
DB_BODY_PROPERTY = "body"
CHUNK_NO_COL = "chunk_number"

BASE_CLASS_OBJ = {
    "class": WV_CLASS,
    "vectorizer": "text2vec-openai",
    "moduleConfig": {
        "generative-openai": {}
    },
}
SUMMARY_CLASS_OBJ = {
    "class": SUMMARY_CLASS,
    "vectorizer": "text2vec-openai",
    "moduleConfig": {
        "generative-openai": {}
    },
}
WV_SCHEMA = {
    "classes": [
        BASE_CLASS_OBJ,
        SUMMARY_CLASS_OBJ
    ]
}


@dataclass
class SourceData:
    source_path: str
    source_text: str
    source_title: Optional[str] = None


class ResponseExtract(Enum):
    GET_OBJECTS = 10
    AGGREGATE_COUNT = 20
    GENERATE_GROUPEDRESULT = 30


def _filter_source_path(source_path: str) -> dict:
    return {
        "path": ["source_path"],
        "operator": "Equal",
        "valueText": source_path
    }


def _parse_response(response: dict, target_class: str, extract: ResponseExtract = ResponseExtract.GET_OBJECTS):
    """
    Extract results from returned GET GraphQL call from Weaviate
    :param response:
    :param target_class:
    :return:
    """
    if extract == ResponseExtract.GET_OBJECTS:
        return response["data"]["Get"][target_class]
    elif extract == ResponseExtract.GENERATE_GROUPEDRESULT:
        return response["data"]["Get"][target_class][0]["_additional"]["generate"]["groupedResult"]
    elif extract == ResponseExtract.AGGREGATE_COUNT:
        return response["data"]["Aggregate"][target_class][0]["meta"]["count"]


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

    def get_unique_paths(self) -> List[str]:
        response = (
            self.client.query
            .aggregate("Knowledge_chunk")
            .with_group_by_filter(["source_path"])
            .with_meta_count()
            .with_fields("groupedBy { path value }")
            .do()
        )
        groups = response["data"]["Aggregate"][self.target_class]
        source_paths = [g["groupedBy"]["value"] for g in groups]
        return source_paths

    def remote_from_db(self, source_path):
        self.client.batch.delete_objects(
            class_name=WV_CLASS,
            where=_filter_source_path(source_path),
        )
        return True

    def get_total_obj_count(self) -> Union[int, str]:
        response = self.client.query.aggregate(self.target_class).with_meta_count().do()
        return _parse_response(response, self.target_class, ResponseExtract.AGGREGATE_COUNT)

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
        return _parse_response(response, self.target_class)

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
            .with_where(_filter_source_path(source_path))
            .with_sort({"path": [CHUNK_NO_COL], "order": "asc"})
            .do()
        )

        return _parse_response(response, self.target_class)

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
        return _parse_response(response, self.target_class, ResponseExtract.AGGREGATE_COUNT)

    def get_obj_sample(self, source_path):
        response = (
            self.client.query.get(self.target_class, ["source_path", "source_title"])
            .with_where(_filter_source_path(source_path))
            .with_limit(1)
            .do()
        )
        return _parse_response(response, self.target_class)[0]

    def check_if_obj_present(self, identifier: Union[str, SourceData]):
        if type(identifier) == SourceData:
            presence_bool = self._get_sourcedata_obj_count(identifier) > 0
        elif type(identifier) == str:
            tmp_srcdata = SourceData(
                source_path=identifier,
                source_text=identifier,
            )
            presence_bool = self._get_sourcedata_obj_count(tmp_srcdata) > 0
        else:
            return 0

        if presence_bool:
            print("Data already present in DB")
            return True
        else:
            return False

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
        resp_data = _parse_response(response, self.target_class)
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

    def _get_sourcedata_obj_count(self, source_data: SourceData) -> bool:
        try:
            response = (
                self.client.query.aggregate(
                    class_name=WV_CLASS,
                )
                .with_where(_filter_source_path(source_data.source_path))
                .with_meta_count().do()
            )
            return _parse_response(response, self.target_class, ResponseExtract.AGGREGATE_COUNT)
        except:
            return 0

    def _add_to_weaviate(
            self, source_data: SourceData, chunk_number_offset: int = 0
    ) -> int:
        """
        Add objects to Weaviate
        :param source_data: DataClass of source data, with "source_path" and "source_text"
        :return:
        """
        if self.check_if_obj_present(source_data):
            return 0
        else:
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

    def add_pdf(self, pdf_url: str) -> int:
        """
        Add a PDF
        :param pdf_url:
        :return:
        """
        from pypdf import PdfReader
        import requests
        from io import BytesIO

        def download_and_parse_pdf(url):
            # Send a GET request to the URL
            response = requests.get(url)

            # Create a file-like object from the content of the response
            pdf_file = BytesIO(response.content)
            pdf_reader = PdfReader(pdf_file)

            # Initialize a string to store the text content
            pdf_text = ""

            # Iterate through the pages and extract the text
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                pdf_text += page.extract_text()

            return pdf_text

        text_content = download_and_parse_pdf(pdf_url)
        return self._add_text(source_path=pdf_url, source_text=text_content,
                              source_title=pdf_url)

    def add_from_movie_local(self, movie_path: str) -> int:
        """
        Add the transcript of a video to Weaviate
        :param movie_path:
        :return:
        """
        # Grab the Video & convert to transcript text
        from moviepy.editor import VideoFileClip
        import tempfile

        video = VideoFileClip(movie_path)
        audio = video.audio

        temp_audio_file = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)

        # Write audio to the temporary file
        audio.write_audiofile(temp_audio_file.name)

        print(f"Audio saved to temporary file: {temp_audio_file.name}")
        transcript_texts = _get_transcripts_from_audio_file(temp_audio_file.name)

        # Ingest transcripts into the database
        obj_count = 0
        for transcript_text in transcript_texts:
            obj_count += self._add_text(source_path=movie_path, source_text=transcript_text,
                                        chunk_number_offset=obj_count, source_title=movie_path)

        # Cleanup - if original file still exists
        # Optionally, you can delete the temporary file when you're done
        temp_audio_file.close()

        return obj_count

    def add_from_youtube(self, youtube_url: str) -> int:
        """
        Add the transcript of a YouTube video to Weaviate
        :param youtube_url:
        :return:
        """

        if self.check_if_obj_present(youtube_url):
            return 0
        else:
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
        return _parse_response(weaviate_response, self.target_class, ResponseExtract.GENERATE_GROUPEDRESULT)

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
            .with_where(_filter_source_path(source_path))
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
        response = (
            self.client.query.aggregate(SUMMARY_CLASS)
            .with_where(_filter_source_path(source_path))
            .with_meta_count().do()
        )
        count = _parse_response(response, SUMMARY_CLASS, ResponseExtract.AGGREGATE_COUNT)

        if count > 0:
            response = (
                self.client.query.get(SUMMARY_CLASS, ["body", "source_path"])
                .with_where(_filter_source_path(source_path)).do()
            )
            summary = _parse_response(response, SUMMARY_CLASS)[0]["body"]
            return summary
        else:
            entry_count = self._get_entry_count(source_path)
            property_names = self._get_all_property_names()
            topic_prompt = get_prompt(Prompts.SUMMARIZE)

            section_summaries = list()

            # Summarize subsections
            for i in range((entry_count // MAX_N_CHUNKS) + 1):
                response = (
                    self.client.query.get(self.target_class, property_names)
                    .with_where(_filter_source_path(source_path))
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
                summary = _summarize_multiple_paragraphs(section_summaries, custom_prompt)

                # Add summary object
                self.client.data_object.create(
                    data_object={
                        "body": summary,
                        "source_path": source_path
                    },
                    class_name=SUMMARY_CLASS,
                    uuid=generate_uuid5(source_path)
                )
                return summary

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
        topic_prompt = get_prompt(Prompts.SUMMARIZE_WITH_CONTEXT, query_str)

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

    def suggest_topics_to_learn(  # TODO: Convert stuff like this to a set of prompts / enums
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
        topic_prompt = get_prompt(Prompts.SUB_TOPICS, query_str)

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
