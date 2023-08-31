from typing import List
import logging

logger = logging.getLogger(__name__)

MAX_CHUNK_WORDS = 150  # Max chunk size - in words
MAX_CHUNK_CHARS = 500  # Max chunk size - in characters


def chunk_text_by_num_words(source_text: str, max_chunk_words: int = MAX_CHUNK_WORDS, overlap_fraction: float = 0.25) -> List:
    """
    Chunk text input into a list of strings, using a number of words
    :param source_text: Input string to be chunked
    :param max_chunk_words: Maximum length of chunk, in words
    :param overlap_fraction: Overlap as a percentage of chunk_words. The overlap is prepended to each chunk.
    :return: return a list of words
    """
    sep = " "
    overlap_words = int(max_chunk_words * overlap_fraction)

    source_text = source_text.strip()
    word_list = source_text.split(sep)
    chunks_list = list()

    n_chunks = ((len(word_list) - 1 + overlap_words) // max_chunk_words) + 1
    for i in range(n_chunks):
        window_words = word_list[
                       max(max_chunk_words * i - overlap_words, 0):
                       max_chunk_words * (i + 1)
                       ]
        chunks_list.append(sep.join(window_words))
    return chunks_list


def chunk_text_by_num_chars(source_text: str, max_chunk_chars: int = MAX_CHUNK_CHARS, overlap_fraction: float = 0.25) -> List:
    """
    Chunk text input into a list of strings
    :param source_text: Input string to be chunked
    :param max_chunk_chars: Maximum length of chunk, in words
    :param overlap_fraction: Overlap as a percentage of chunk_words
    :return: return a list of words
    """
    overlap_chars = int(max_chunk_chars * overlap_fraction)

    source_text = source_text.strip()
    chunks_list = list()

    n_chunks = ((len(source_text) - 1 + overlap_chars) // max_chunk_chars) + 1
    for i in range(n_chunks):
        chunk = source_text[
                max(max_chunk_chars * i - overlap_chars, 0):
                max_chunk_chars * (i + 1)
                ]
        chunks_list.append(chunk)
    return chunks_list


def remove_multiple_whitespaces(source_text: str) -> str:
    """
    Replace multiple whitespaces with single space
    :param source_text:
    :return:
    """
    import re
    source_text = re.sub(r"\s+", " ", source_text)
    return source_text


def chunk_text(source_text: str) -> List:
    """
    Chunk longer text
    :param source_text:
    :return:
    """
    source_text = remove_multiple_whitespaces(source_text)
    return chunk_text_by_num_chars(source_text)