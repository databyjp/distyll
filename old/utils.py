from dataclasses import dataclass
from typing import List, Union
from pypdf import PdfReader
import requests
from io import BytesIO
import openai
import yt_dlp
import os


MAX_CHUNK_WORDS = 150  # Max chunk size - in words
MAX_CHUNK_CHARS = 500  # Max chunk size - in characters
MAX_N_CHUNKS = 15  # Max number of chunks to grab in a set of results


@dataclass
class PROMPTS:
    RAG_PREAMBLE = "Using only the included data here, answer the following question:"
    SUMMARIZE = f"""
        Using plain language, summarize the following as a whole into a paragraph or two of text.
        List the topics it covers, and what the reader might learn from it.
        """


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


def chunk_text_by_num_chars(str_in: str, max_chunk_chars: int = MAX_CHUNK_CHARS, overlap: float = 0.25) -> List:
    """
    Chunk text input into a list of strings
    :param str_in: Input string to be chunked
    :param max_chunk_chars: Maximum length of chunk, in words
    :param overlap: Overlap as a percentage of chunk_words
    :return: return a list of words
    """
    overlap_size = int(max_chunk_chars * overlap)

    str_in = str_in.strip()
    chunks_list = list()

    n_chunks = ((len(str_in) - 1 + overlap_size) // max_chunk_chars) + 1
    for i in range(n_chunks):
        chunk = str_in[
                max(max_chunk_chars * i - overlap_size, 0):
                max_chunk_chars * (i + 1)
                ]
        chunks_list.append(chunk)
    return chunks_list


def preprocess_text(str_in: str) -> str:
    """
    Preprocess a piece of input text
    :param str_in:
    :return:
    """
    import re
    str_in = re.sub(r"\s+", " ", str_in)
    return str_in


def chunk_text(str_in: str) -> List:
    """
    Chunk longer text
    :param str_in:
    :return:
    """
    str_in = preprocess_text(str_in)
    return chunk_text_by_num_chars(str_in)


def download_and_parse_pdf(pdf_url):
    """
    Get the text from a PDF and parse it
    :param pdf_url:
    :return:
    """
    print(f"Parsing {pdf_url} text")
    # Send a GET request to the URL
    response = requests.get(pdf_url)

    # Create a file-like object from the content of the response
    pdf_file = BytesIO(response.content)
    pdf_reader = PdfReader(pdf_file)

    # Initialize a string to store the text content
    pdf_text = ""
    n_pages = len(pdf_reader.pages)

    # Iterate through the pages and extract the text
    for page_num in range(n_pages):
        page = pdf_reader.pages[page_num]
        pdf_text += "\n" + page.extract_text()

    print(f"Finished parsing {n_pages} pages from {pdf_url}")
    return pdf_text


def download_youtube(link: str, outpath: str, audio_only: bool = True):
    if audio_only:
        yt_dlp_params = {'extract_audio': True, 'format': 'bestaudio', 'outtmpl': outpath, 'quiet': True, 'cachedir': False}
    else:
        yt_dlp_params = {'format': 'best', 'outtmpl': outpath, 'quiet': True, 'cachedir': False}

    with yt_dlp.YoutubeDL(yt_dlp_params) as video:

        if os.path.exists(outpath):
            os.remove(outpath)

        info_dict = video.extract_info(link, download=True)
        video_title = info_dict['title']
        print(f"Found {video_title} - downloading")
        video.download(link)
        print(f"Successfully Downloaded to {outpath}")
    return True


# def summarize_paragraph_set(paragraphs: List) -> str:
#     topic_prompt = PROMPTS.SUMMARIZE + ("=" * 10) + str(paragraphs)
#     completion = openai.ChatCompletion.create(
#         model="gpt-3.5-turbo",
#         messages=[
#             {"role": "system",
#              "content": """
#                 You are a helpful assistant who can summarize information very well in
#                 clear, concise language without resorting to domain-specific jargon.
#                 """
#              },
#             {"role": "user", "content": topic_prompt}
#         ]
#     )
#     return completion.choices[0].message["content"]


# def ask_chatgpt(prompt: str) -> str:
#     """
#     Helper function for summarizing multiple paragraphs using an LLM
#     :param prompt:
#     :return:
#     """
#
#     completion = openai.ChatCompletion.create(
#         model="gpt-3.5-turbo",
#         messages=[
#             {"role": "system",
#              "content": """
#                 You are a helpful, intelligent, AI assistant. Please answer the following question the best you can.
#                 """
#              },
#             {"role": "user", "content": prompt}
#         ]
#     )
#     return completion.choices[0].message["content"]


# ==========


def download_audio(link: str, outpath: str):
    with yt_dlp.YoutubeDL({
        'extract_audio': True, 'format': 'bestaudio', 'outtmpl': outpath, 'quiet': True, 'cachedir': False
    }) as video:

        if os.path.exists(outpath):
            os.remove(outpath)

        info_dict = video.extract_info(link, download=True)
        video_title = info_dict['title']
        print(f"Found {video_title} - downloading")
        video.download(link)
        print(f"Successfully Downloaded to {outpath}")


def get_youtube_title(link: str):
    with yt_dlp.YoutubeDL({'quiet': True, 'cachedir': False}) as ydl:
        info_dict = ydl.extract_info(link, download=False)
        return info_dict.get('title', None)


def _get_transcripts_from_audio_file(audio_file_path: str) -> List:
    """
    Get transcripts of audio files using
    :param audio_file_path:
    :return:
    """

    clip_outpaths = _split_audio_files(audio_file_path)
    transcript_texts = list()
    print(f"Getting transcripts from {len(clip_outpaths)} audio files...")
    for i, clip_outpath in enumerate(clip_outpaths):
        print(f"Processing transcript {i+1} of {len(clip_outpaths)}...")
        with open(clip_outpath, "rb") as audio_file:
            transcript = openai.Audio.transcribe("whisper-1", audio_file)
            transcript_texts.append(transcript["text"])

    # Clean up
    for clip_outpath in clip_outpaths:
        os.remove(clip_outpath)

    return transcript_texts


def _split_audio_files(audio_file_path: str) -> List:
    """
    Split long audio files
    (e.g. so that they fit within the allowed size for Whisper)
    :param audio_file_path:
    :return: A list of file paths
    """
    from pydub import AudioSegment

    audio = AudioSegment.from_file(audio_file_path)

    # Split long audio into 15-minute clips
    segment_len = 900
    clip_outpaths = list()

    if audio.duration_seconds > segment_len:
        n_segments = 1 + int(audio.duration_seconds) // segment_len
        print(f"Splitting audio to {n_segments}")
        for i in range(n_segments):
            start = max(0, (i * segment_len) - 10) * 1000
            end = ((i + 1) * segment_len) * 1000
            clip = audio[start:end]
            clip_outpath = f"{i}_" + audio_file_path
            outfile = clip.export(f"{i}_" + audio_file_path)
            outfile.close()
            clip_outpaths.append(clip_outpath)
        return clip_outpaths
    else:
        print(f"Audio file under {segment_len} seconds. No split required.")
        return [audio_file_path]
