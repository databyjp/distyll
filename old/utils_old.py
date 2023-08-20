import os
import openai
import yt_dlp
from pathlib import Path
from typing import List


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


def load_txt_file(txt_path: Path = None) -> str:
    """
    Load a text (.txt) file and return the resulting string
    :param txt_path: Path of text file
    :return:
    """
    if txt_path is None:
        txt_path = Path("../demodata/kubernetes_concepts_overview.txt")
    return txt_path.read_text()


# def load_wiki_page(wiki_title: str, user_agent: str = 'YourProjectName (yournamehere@gmail.com)') -> str:
#     """
#     Load contents of a Wiki page
#     :param wiki_title:
#     :param user_agent:
#     :return:
#     """
#     import wikipediaapi
#     wiki_en = wikipediaapi.Wikipedia(user_agent, 'en')
#
#     page_py = wiki_en.page(wiki_title)
#     if page_py.exists():
#         return page_py.text  # Could also return page_py.summary
#     else:
#         print(f"Could not find a page called {wiki_title}.")


# def load_data(source_path: Path) -> str:
#     """
#     Load various data types
#     :param source_path:
#     :return:
#     """
#     return load_txt_file(source_path)  # TODO - add other media types

#
# def chunk_text(str_in: str) -> List:
#     """
#     Chunk longer text
#     :param str_in:
#     :return:
#     """
#     return chunk_text_by_num_words(str_in)
#
#
# def chunk_text_by_num_words(str_in: str, max_chunk_words: int = MAX_CHUNK_WORDS, overlap: float = 0.25) -> List:
#     """
#     Chunk text input into a list of strings
#     :param str_in: Input string to be chunked
#     :param max_chunk_words: Maximum length of chunk, in words
#     :param overlap: Overlap as a percentage of chunk_words
#     :return: return a list of words
#     """
#     sep = " "
#     overlap_words = int(max_chunk_words * overlap)
#
#     str_in = str_in.strip()
#     word_list = str_in.split(sep)
#     chunks_list = list()
#
#     n_chunks = ((len(word_list) - 1 + overlap_words) // max_chunk_words) + 1
#     for i in range(n_chunks):
#         window_words = word_list[
#                        max(max_chunk_words * i - overlap_words, 0):
#                        max_chunk_words * (i + 1)
#                        ]
#         chunks_list.append(sep.join(window_words))
#     return chunks_list

#
# def download_and_parse_pdf(pdf_url):
#     """
#     Get the text from a PDF and parse it
#     :param pdf_url:
#     :return:
#     """
#     # Send a GET request to the URL
#     response = requests.get(pdf_url)
#
#     # Create a file-like object from the content of the response
#     pdf_file = BytesIO(response.content)
#     pdf_reader = PdfReader(pdf_file)
#
#     # Initialize a string to store the text content
#     pdf_text = ""
#
#     # Iterate through the pages and extract the text
#     for page_num in range(len(pdf_reader.pages)):
#         page = pdf_reader.pages[page_num]
#         pdf_text += "\n" + page.extract_text()
#
#     return pdf_text
