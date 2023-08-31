from dataclasses import dataclass, fields
from typing import Union, List, Dict
import distyll
import rag


@dataclass
class RAGResponse:
    generated_text: str
    objects: List[Dict]


def parse_response(weaviate_response: dict, collection_name) -> RAGResponse:
    generated_text = weaviate_response['data']['Get'][collection_name][0]['_additional']['generate']['groupedResult']
    objects = weaviate_response['data']['Get'][collection_name]
    del objects[0]['_additional']
    rag_response = RAGResponse(
        generated_text=generated_text,
        objects=objects
    )
    return rag_response


def generate_on_search(
        db: distyll.DBConnection, prompt: str, search_query: str,
        object_path: Union[None, str], limit: int = rag.MAX_N_CHUNKS
):
    """
    Perform a search and then a generative task on those search results
    For specific tasks that should be paired with a search (e.g. what does video AA say about topic BB?)
    :param db:
    :param prompt:
    :param search_query:
    :param object_path: Object path identifier for filtering
    :param limit:
    :return:
    """
    if object_path is not None:
        where_filter = {
            "path": [f.name for f in fields(distyll.ChunkData) if 'path' in f.name],
            "operator": "Equal",
            "valueText": object_path
        }
        response = (
            db.client.query
            .get(db.chunk_class, db.chunk_properties)
            .with_where(where_filter)
            .with_near_text({'concepts': [search_query]})
            .with_generate(grouped_task=prompt)
            .with_limit(limit)
            .do()
        )
    else:
        response = (
            db.client.query
            .get(db.chunk_class, db.chunk_properties)
            .with_near_text({'concepts': [search_query]})
            .with_generate(grouped_task=prompt)
            .with_limit(rag.MAX_N_CHUNKS)
            .do()
        )

    # generated_text = response['data']['Get'][db.chunk_class][0]['_additional']['generate']['groupedResult']
    # objects = response['data']['Get'][db.chunk_class]
    # del objects[0]['_additional']
    # rag_response = RAGResponse(
    #     generated_text=generated_text,
    #     objects=objects
    # )
    # return rag_response
    return parse_response(response, db.chunk_class)


def generate_on_summary(db: distyll.DBConnection, prompt: str, object_path: str):
    """
    Perform a generative task on a summary of an object.
    For questions that relate to the entire object (e.g. what does video AA cover?)
    :param db:
    :param prompt:
    :param object_path: Object path identifier for filtering
    :return:
    """
    where_filter = {
        "path": [f.name for f in fields(distyll.SourceData) if 'path' in f.name],
        "operator": "Equal",
        "valueText": object_path
    }
    response = (
        db.client.query
        .get(db.source_class, db.source_properties)
        .with_where(where_filter)
        .with_generate(grouped_task=prompt)
        .with_limit(rag.MAX_N_CHUNKS)  # There should only be 1 object here, but leaving this line in anyway
        .do()
    )

    # generated_text = response['data']['Get'][db.source_class][0]['_additional']['generate']['groupedResult']
    # objects = response['data']['Get'][db.source_class]
    # del objects[0]['_additional']
    # rag_response = RAGResponse(
    #     generated_text=generated_text,
    #     objects=objects
    # )
    # return rag_response
    return parse_response(response, db.source_class)
