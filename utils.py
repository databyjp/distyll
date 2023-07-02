from pathlib import Path
from weaviate import Client

MAX_CHUNK_WORDS = 100
MAX_N_CHUNKS = 1 + (1000 // MAX_CHUNK_WORDS)


class Collection:

    def __init__(self, client: Client, target_class: str):
        self.client = client
        self.target_class = target_class

    def text_search(self, neartext_query: str, limit: int = 10) -> list:
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

    def _get_generated_result(self, weaviate_response: dict) -> str:
        return weaviate_response["data"]["Get"][self.target_class][0]["_additional"]["generate"]["groupedResult"]

    def generate_summary(self, query_str: str) -> str:
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
            self.client.query.get(self.target_class, ["body"])
            .with_near_text({"concepts": [query_str]})
            .with_limit(MAX_N_CHUNKS)
            .with_generate(
                grouped_task=topic_prompt
            )
            .do()
        )

        return self._get_generated_result(response)

    def suggest_topics_with_weaviate(self, query_str: str) -> str:
        topic_prompt = f"""
        If the following text does includes information about {query_str}, 
        extract a list of three to six related sub-topics
        related to {query_str} that the user might learn about.
        Deliver the topics as a short list, each separated by two consecutive newlines like `\n\n`
    
        If the following information does not includes information about {query_str}, 
        tell the user that not enough information could not be found.
        =====
        """

        response = (
            self.client.query.get(self.target_class, ["body"])
            .with_near_text({"concepts": [query_str]})
            .with_limit(MAX_N_CHUNKS)
            .with_generate(
                grouped_task=topic_prompt
            )
            .do()
        )
        return self._get_generated_result(response)


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