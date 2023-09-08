from dataclasses import dataclass
from weaviate import Client
from typing import Union, List, Dict, Optional
import rag
import logging

logger = logging.getLogger(__name__)

N_RAG_CHUNKS = int(rag.MAX_N_CHUNKS * 0.25)


@dataclass
class RAGResponse:
    generated_text: str
    objects: List[Dict]
    error: Optional[str] = None


def path_filter(object_path):
    where_filter = {
        "path": ["source_path"],
        "operator": "Equal",
        "valueText": object_path
    }
    return where_filter


def get_source_objects(
        client: Client, collection_name: str, collection_properties: List[str],
        where_filter: Dict
):
    response = (
        client.query
        .get(collection_name, collection_properties)
        .with_where(where_filter)
        .with_limit(N_RAG_CHUNKS)
        .do()
    )
    return response["data"]["Get"][collection_name]


def get_chunk_objects(
        client: Client, collection_name: str, collection_properties: List[str],
        where_filter: Dict, limit: int = N_RAG_CHUNKS
):
    response = (
        client.query
        .get(collection_name, collection_properties)
        .with_where(where_filter)
        .with_limit(limit)
        .with_sort({
            'path': ['chunk_number'],
            'order': 'asc'
        })
        .do()
    )
    return response["data"]["Get"][collection_name]


def parse_generative_response(weaviate_response: dict, collection_name) -> RAGResponse:
    generated = weaviate_response['data']['Get'][collection_name][0]['_additional']['generate']
    generated_text = generated['groupedResult']
    if 'error' in generated.keys():
        error = generated['error']
    else:
        error = None
    objects = weaviate_response['data']['Get'][collection_name]
    del objects[0]['_additional']
    rag_response = RAGResponse(
        generated_text=generated_text,
        objects=objects,
        error=error,
    )
    return rag_response


def generate_on_search(
        client: Client,
        class_name: str, class_properties: List[str],
        prompt: str, search_query: str,
        object_path: Union[None, str], limit: int = N_RAG_CHUNKS
):
    """
    Perform a search and then a generative task on those search results
    For specific tasks that should be paired with a search (e.g. what does video AA say about topic BB?)
    :param client:
    :param class_name:
    :param class_properties:
    :param prompt:
    :param search_query:
    :param object_path: Object path identifier for filtering
    :param limit:
    :return:
    """
    if object_path is not None:
        where_filter = {
            "path": ["source_path"],
            "operator": "Equal",
            "valueText": object_path
        }
        response = (
            client.query
            .get(class_name, class_properties)
            .with_where(where_filter)
            .with_near_text({'concepts': [search_query]})
            .with_generate(grouped_task=prompt)
            .with_limit(limit)
            # .with_sort({
            #     'path': ['chunk_number'],
            #     'order': 'asc'
            # })
            .do()
        )
    else:
        response = (
            client.query
            .get(class_name, class_properties)
            .with_near_text({'concepts': [search_query]})
            .with_generate(grouped_task=prompt)
            .with_limit(N_RAG_CHUNKS)
            .do()
        )
    return parse_generative_response(response, class_name)


def generate_on_summary(
        client: Client,
        class_name: str, class_properties: List[str],
        prompt: str, object_path: str
) -> RAGResponse:
    """
    Perform a generative task on a summary of an object.
    For questions that relate to the entire object (e.g. what does video AA cover?)
    :param client:
    :param class_name:
    :param class_properties:
    :param prompt:
    :param object_path: Object path identifier for filtering
    :return:
    """
    where_filter = {
        "path": ["path"],
        "operator": "Equal",
        "valueText": object_path
    }
    response = (
        client.query
        .get(class_name, class_properties)
        .with_where(where_filter)
        .with_generate(grouped_task=prompt)
        .with_limit(N_RAG_CHUNKS)  # There should only be 1 object here, but leaving this line in anyway
        .do()
    )
    return parse_generative_response(response, class_name)


def generate_on_both(
        client: Client,
        class_name: str, class_properties: List[str],
        prompt: str, object_path: str
) -> RAGResponse:
    """
    Perform a generative task on a summary of an object.
    For questions that relate to the entire object (e.g. what does video AA cover?)
    :param client:
    :param class_name:
    :param class_properties:
    :param prompt:
    :param object_path: Object path identifier for filtering
    :return:
    """
    where_filter = {
        "path": ["path"],
        "operator": "Equal",
        "valueText": object_path
    }
    chunks_response = (
        client.query
        .get(class_name, class_properties)
        .with_where(where_filter)
        .with_limit(N_RAG_CHUNKS)  # There should only be 1 object here, but leaving this line in anyway
        .do()
    )
    # Combine chunks with summary
    # Run generative search on both with quuery
    # return parse_generative_response(chunks_response, class_name)
