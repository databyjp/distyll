from pathlib import Path
import weaviate
from weaviate import Client
from weaviate.util import generate_uuid5
from dataclasses import dataclass

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
MAX_CHUNK_WORDS = 100
MAX_N_CHUNKS = 1 + (1000 // MAX_CHUNK_WORDS)


@dataclass
class SourceData:
    source_path: str
    source_text: str


def instantiate_weaviate() -> Client:
    client = weaviate.Client("http://localhost:8080")
    if not client.schema.contains({"class": WV_CLASS, "properties": []}):
        print("Creating a new class:")
        client.schema.create(WV_SCHEMA)
    else:
        print("Skipping class creation")
    return client


def load_txt_file(txt_path: Path = None) -> str:
    """
    Load a text (.txt) file and return the resulting string
    :param txt_path: Path of text file
    :return:
    """
    if txt_path is None:
        txt_path = Path("data/kubernetes_concepts_overview.txt")
    return txt_path.read_text()


def load_wiki_page(wiki_title: str) -> str:
    import wikipediaapi
    wiki_en = wikipediaapi.Wikipedia('en')
    page_py = wiki_en.page(wiki_title)
    if page_py.exists():
        return page_py.summary
    else:
        print(f"Could not find a page called {wiki_title}.")


def load_data(source_path: Path) -> str:
    return load_txt_file(source_path)  # TODO - add other media types


def chunk_text(str_in: str) -> list:
    # TODO - add other chunking methods
    return chunk_text_by_num_words(str_in)


def chunk_text_by_num_words(str_in: str, max_chunk_words: int = MAX_CHUNK_WORDS, overlap: float = 0.25) -> list:
    """
    Chunk text input into a list of strings
    :param str_in: Input string to be chunked
    :param max_chunk_words: Maximum length of chunk, in words
    :param overlap: Overlap as a percentage of chunk_words
    :return: return a list of words
    """
    sep = " "
    overlap_words = int(max_chunk_words * overlap)

    str_in = str_in.strip()
    word_list = str_in.split(sep)
    chunks_list = list()

    n_chunks = ((len(word_list) - 1 + overlap_words) // max_chunk_words) + 1
    for i in range(n_chunks):
        window_words = word_list[
                       max(max_chunk_words * i - overlap_words, 0):
                       max_chunk_words * (i + 1)
                       ]
        chunks_list.append(sep.join(window_words))
    return chunks_list


def build_weaviate_object(chunk: str, object_data: dict) -> dict:
    wv_object = dict()
    for k, v in object_data.items():
        wv_object[k] = v
    wv_object["body"] = chunk
    return wv_object


def add_to_weaviate(source_data: SourceData, client: Client) -> int:
    """
    Add objects to Weaviate
    :param source_data: Dict of source data, with "source_path" and "source_text"
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


def get_generated_result(weaviate_response: dict) -> str:
    return weaviate_response["data"]["Get"][WV_CLASS][0]["_additional"]["generate"]["groupedResult"]


def generate_summary_with_weaviate(query_str: str, client) -> str:

    topic_prompt = f"""
    Based on the following text snippets, answer the following question
    If the information does not include relevant information, 
    do not answer the question, and indicate as such to the user.
    
    =====
    QUESTION: {query_str}.
    =====
    
    ANSWER:
    """

    response = (
        client.query.get(WV_CLASS, ["body"])
        .with_near_text({"concepts": [query_str]})
        .with_limit(MAX_N_CHUNKS)
        .with_generate(
            grouped_task=topic_prompt
        )
        .do()
    )

    return get_generated_result(response)


def suggest_topics_with_weaviate(query_str: str, client) -> str:
    topic_prompt = f"""
    If the following text does includes information about {query_str}, 
    extract a list of three to six related sub-topics
    related to {query_str} that the user might learn about.
    Deliver the topics as a short list, each separated by two consecutive newlines like `\n\n`

    If the following information does not includes information about {query_str}, 
    tell the user that the information could not be found.
    =====
    """

    response = (
        client.query.get(WV_CLASS, ["body"])
        .with_near_text({"concepts": [query_str]})
        .with_limit(MAX_N_CHUNKS)
        .with_generate(
            grouped_task=topic_prompt
        )
        .do()
    )
    return get_generated_result(response)


# def summarize_chunk(long_text: str) -> str:
#     # TODO - input a real summarizer
#     return long_text[:10]
#
#
# def build_summary(source_path: Path, max_context_length: int = 2000) -> str:
#     """
#     Build a summary of the source data
#     :param source_path: Path of source file
#     :param max_context_length: Maximum length for sending chunks to be summarized
#     :return:
#     """
#     source_data = load_data(source_path)
#     chunks = chunk_text_by_num_words(source_data, max_context_length)
#     summary_list = list()
#     for c in chunks:
#         summary_list.append(summarize_chunk(c))
#     summary_text = "\n".join(summary_list)
#     return summary_text
#
#
# def get_llm_response(source_text: str) -> str:
#     # TODO - Use an LLM to get a list of topics from a text
#     return "cats at home\n\ncats at work"
#
#
# def get_suggested_subtopics(source_path: Path, source_topic: str) -> list:
#     """
#     Return a list of sub-topics that a user could dive into, from the given topic
#     :param source_path: Path of source file
#     :return:
#     """
#     summary_text = build_summary(source_path)
#     topics_list = list()
#     item_sep = "\n\n"
#     topic_prompt = f"""
#     Based on the following text, suggest a list of three to six related sub-topics
#     related to {source_topic} that the user might learn about.
#     Deliver the topics as a short list, each separated by two consecutive newlines like {item_sep}
#
#     =====
#
#     Source text:
#     {summary_text}
#     =====
#
#     Sub-topics:
#     """
#
#     llm_response = get_llm_response(topic_prompt)
#
#     topics_list = llm_response.split(item_sep)
#
#     return topics_list
#
#
# def prompt_user_for_input():
#     return True
#
#
# def generate_summary_from_input():
#     return True