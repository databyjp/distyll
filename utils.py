from pathlib import Path
from weaviate import Client
from dataclasses import dataclass
from weaviate.util import generate_uuid5

MAX_CHUNK_WORDS = 100
MAX_N_CHUNKS = 1 + (1000 // MAX_CHUNK_WORDS)
WV_CLASS = "Knowledge_chunk"


@dataclass
class SourceData:
    source_path: str
    source_text: str


class Collection:

    def __init__(self, client: Client, target_class: str):
        self.client = client
        self.target_class = target_class

    def _add_text(self, source_path: str, source_text: str):
        src_data = SourceData(
            source_path=source_path,
            source_text=source_text
        )
        return self.add_to_weaviate(src_data)

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
        return self._add_text(text_file_path, load_data(filepath))

    def add_wiki_article(
            self, wiki_title: str
    ) -> int:
        """
        Add a wikipedia article to the DB
        :param wiki_title: Title of the Wiki page to add
        :return:
        """
        return self._add_text(wiki_title, load_wiki_page(wiki_title))

    def add_to_weaviate(
            self, source_data: SourceData
    ) -> int:
        """
        Add objects to Weaviate
        :param source_data: DataClass of source data, with "source_path" and "source_text"
        :return:
        """
        chunks = chunk_text(source_data.source_text)
        object_data = {
            "source_path": str(source_data.source_path)
        }
        counter = 0
        with self.client.batch() as batch:
            for c in chunks:
                wv_obj = build_weaviate_object(c, object_data)
                batch.add_data_object(
                    class_name=WV_CLASS,
                    data_object=wv_obj,
                    uuid=generate_uuid5(wv_obj)
                )
                counter += 1

        return counter  # TODO add error handling

    def add_from_youtube(self, youtube_url: str):

        import yt_dlp
        import openai
        import os

        outpath = 'temp_audio.mp3'

        def download_audio(link: str):
            with yt_dlp.YoutubeDL({'extract_audio': True, 'format': 'bestaudio', 'outtmpl': outpath}) as video:
                info_dict = video.extract_info(link, download=True)
                video_title = info_dict['title']
                print(f"Found {video_title} - downloading")
                video.download(link)
                print(f"Successfully Downloaded to {outpath}")

        download_audio(youtube_url)
        openai.api_key = os.environ["OPENAI_APIKEY"]
        audio_file = open("Hello Weaviate - What Is a Vector？.mp3", "rb")
        transcript = openai.Audio.transcribe("whisper-1", audio_file)
        transcript_text = transcript["text"]
        os.remove(outpath)

        return self._add_text(youtube_url, transcript_text)

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

    def _get_generated_result(self, weaviate_response: dict) -> str:
        """
        Parse the generated results
        :param weaviate_response:
        :return:
        """
        return weaviate_response["data"]["Get"][self.target_class][0]["_additional"]["generate"]["groupedResult"]

    def _get_grouped_task(
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
            self.client.query.get(self.target_class, ["body"])
            .with_near_text(
                {
                    "concepts": [query_str],
                    "distance": max_distance
                }
            )
            .with_limit(obj_limit)
            .with_generate(
                grouped_task=topic_prompt
            )
            .do()
        )

        if debug:
            return response
        else:
            return self._get_generated_result(response)

    def generate_summary(
            self, query_str: str,
            obj_limit: int = MAX_N_CHUNKS, max_distance: float = 0.28,
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
        Based on the following text, summarize any information relating to {query_str}.
        If the text does not contain required information, 
        do not answer the question, and indicate as such to the user.
        """

        return self._get_grouped_task(
            query_str,
            topic_prompt,
            obj_limit=obj_limit, max_distance=max_distance,
            debug=debug
        )

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
        =====
        """

        return self._get_grouped_task(
            query_str,
            topic_prompt,
            obj_limit=obj_limit, max_distance=max_distance,
            debug=debug
        )


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
    """
    Load contents of a Wiki page
    :param wiki_title:
    :return:
    """
    import wikipediaapi
    wiki_en = wikipediaapi.Wikipedia('en')
    page_py = wiki_en.page(wiki_title)
    if page_py.exists():
        return page_py.summary
    else:
        print(f"Could not find a page called {wiki_title}.")


def load_data(source_path: Path) -> str:
    """
    Load various data types
    :param source_path:
    :return:
    """
    return load_txt_file(source_path)  # TODO - add other media types


def chunk_text(str_in: str) -> list:
    """
    Chunk longer text
    :param str_in:
    :return:
    """
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
    """
    Build a Weaviate object after chunking
    :param chunk:
    :param object_data:
    :return:
    """
    wv_object = dict()
    for k, v in object_data.items():
        wv_object[k] = v
    wv_object["body"] = chunk
    return wv_object
