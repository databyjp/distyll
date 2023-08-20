from dataclasses import dataclass
from typing import List, Union
from pypdf import PdfReader
import requests
from io import BytesIO
import openai


MAX_CHUNK_WORDS = 100  # Max chunk size - in words
MAX_CONTEXT_LENGTH = 1000  # Max length of a context
MAX_PARAGRAPHS = 10  # Max number of paragraphs to summarize at a time
MAX_N_CHUNKS = 1 + (MAX_CONTEXT_LENGTH // MAX_CHUNK_WORDS)

@dataclass
class PROMPTS:
    RAG_PREAMBLE = "Using only the included data here, answer the following question:"
    SUMMARIZE = f"""
        Using plain language, summarize the following as a whole into a paragraph or two of text.
        List the topics it covers, and what the reader might learn from it. 
        """


def chunk_text(str_in: str) -> List:
    """
    Chunk longer text
    :param str_in:
    :return:
    """
    return chunk_text_by_num_words(str_in)


def chunk_text_by_num_words(str_in: str, max_chunk_words: int = MAX_CHUNK_WORDS, overlap: float = 0.25) -> List:
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


def download_and_parse_pdf(pdf_url):
    """
    Get the text from a PDF and parse it
    :param pdf_url:
    :return:
    """
    # Send a GET request to the URL
    response = requests.get(pdf_url)

    # Create a file-like object from the content of the response
    pdf_file = BytesIO(response.content)
    pdf_reader = PdfReader(pdf_file)

    # Initialize a string to store the text content
    pdf_text = ""

    # Iterate through the pages and extract the text
    for page_num in range(len(pdf_reader.pages)):
        page = pdf_reader.pages[page_num]
        pdf_text += "\n" + page.extract_text()

    return pdf_text


def summarize_paragraph_set(paragraphs: List) -> str:
    topic_prompt = PROMPTS.SUMMARIZE + ("=" * 10) + str(paragraphs)
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system",
             "content": """
                You are a helpful assistant who can summarize information very well in 
                clear, concise language without resorting to domain-specific jargon.
                """
             },
            {"role": "user", "content": topic_prompt}
        ]
    )
    return completion.choices[0].message["content"]


def summarize_multiple_paragraphs(paragraphs: List) -> Union[str, List]:
    """
    Helper function for summarizing multiple paragraphs using an LLM
    :param paragraphs:
    :return:
    """
    paragraph_count = len(paragraphs)
    if paragraph_count < MAX_PARAGRAPHS:
        print(f"Summarizing {paragraph_count} paragraphs")
        return summarize_paragraph_set(paragraphs)
    else:
        print(f"{paragraph_count} paragraphs is too many - let's split them up")
        summary_sets = (paragraph_count // MAX_N_CHUNKS) + 1
        subsets = [
            paragraphs[MAX_PARAGRAPHS*i:MAX_PARAGRAPHS*(i+1)] for i in range(summary_sets)
        ]
        summaries = [
            summarize_paragraph_set(subset) for subset in subsets
        ]
        return summarize_multiple_paragraphs(summaries)
