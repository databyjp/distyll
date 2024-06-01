from bs4 import BeautifulSoup
from typing import Union, List, Dict, Any, Literal
import requests
import logging
from pathlib import Path
from openai import OpenAI
import yt_dlp
import os


OPENAI_APIKEY = None


def init_dl_dir(dir_path: Union[str, Path]) -> Path:
    """
    Initializes the download directory.

    Args:
        dir_path (Union[str, Path]): The path to the download directory.

    Returns:
        Path: The path to the initialized download directory.
    """
    if type(dir_path) is str:
        dir_path = Path(dir_path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def set_api_key(openai: str = None) -> bool:
    """
    Set the OpenAI API key as an environment variable.

    Args:
        openai (str): The OpenAI API key.

    Returns:
        bool: True if the OpenAI API key is set, False otherwise.
    """
    if openai is not None:
        global OPENAI_APIKEY
        OPENAI_APIKEY = openai
        return True
    return False


def get_openai_client(apikey: Union[str, None] = None) -> OpenAI:
    """
    Helper function to get an OpenAI client
    :param apikey:
    :return:
    """
    global OPENAI_APIKEY
    if apikey is not None:
        oai_client = OpenAI(api_key=apikey)
    elif OPENAI_APIKEY is not None:
        oai_client = OpenAI(api_key=OPENAI_APIKEY)
    else:
        if os.getenv("OPENAI_APIKEY") is not None:
            oai_client = OpenAI(api_key=os.getenv("OPENAI_APIKEY"))
        else:
            raise ValueError("OpenAI API key not provided.")
    return oai_client


def get_arxiv_title(arxiv_url: str) -> Union[str, None]:
    """
    Helper function to get the title of an ArXiV paper
    :param arxiv_url:
    :return:
    """
    logging.info(f"Getting arXiV title from {arxiv_url}")
    response = requests.get(arxiv_url)
    if response.status_code != 200:
        logging.info(
            f"Failed to get the page. HTTP status code: {response.status_code}"
        )
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    title_element = soup.find("meta", {"name": "citation_title"})
    if title_element:
        return title_element["content"]
    else:
        logging.info("Failed to find the title element")
        return None


def chunk_text_by_num_words(
    source_text: str, max_chunk_words: int = 100, overlap_fraction: float = 0.25, prevent_short_last_chunks: bool = True
) -> List[str]:
    """
    Chunk text input into a list of strings, using a number of words
    :param source_text: Input string to be chunked
    :param max_chunk_words: Maximum length of chunk, in words
    :param overlap_fraction: Overlap as a percentage of chunk_words. The overlap is prepended to each chunk.
    :param prevent_short_last_chunks: Prevent very short last chunks
    :return: return a list of words
    """
    logging.info(f"Chunking text of {len(source_text)} chars by number of words.")
    sep = " "
    overlap_words = int(max_chunk_words * overlap_fraction)

    source_text = source_text.strip()
    word_list = source_text.split(sep)
    chunks_list = list()

    n_chunks = ((len(word_list) - 1 + overlap_words) // max_chunk_words) + 1
    for i in range(n_chunks):
        window_words = word_list[
            max(max_chunk_words * i - overlap_words, 0) : max_chunk_words * (i + 1)
        ]
        if prevent_short_last_chunks:
            # Conditional logic to handle the last two chunks and prevent very short chunks
            if i < n_chunks - 2:
                chunks_list.append(sep.join(window_words))
            else:  # Second to last chunk onwards
                remaining_words = word_list[max(max_chunk_words * i - overlap_words, 0) :]
                if len(remaining_words) <= max_chunk_words:
                    chunks_list.append(sep.join(remaining_words))
                    break
                second_last_chunk = remaining_words[
                    : len(remaining_words) // 2 + overlap_words
                ]
                last_chunk = remaining_words[len(remaining_words) // 2 :]
                chunks_list.append(sep.join(second_last_chunk))
                chunks_list.append(sep.join(last_chunk))
                break
        else:
            chunks_list.append(sep.join(window_words))

    return chunks_list


def chunk_text_by_num_chars(
    source_text: str, max_chunk_chars: int = 300, overlap_fraction: float = 0.25
) -> List[str]:
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
            max(max_chunk_chars * i - overlap_chars, 0) : max_chunk_chars * (i + 1)
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


def chunk_text(
    source_text: str,
    method: Literal["words", "chars"] = "words",
    token_length: Union[None, int] = 100,
    overlap_fraction: float = 0.25,
) -> List[str]:
    """
    Chunk longer text
    :param source_text: Input text
    :param method: "words" or "chars"
    :param token_length: Number of tokens to chunk by
    :param overlap_fraction: Overlap as a percentage of chunk
    :return:
    """
    logging.info(
        f"Chunking text of {len(source_text)} characters with {method} method."
    )
    source_text = remove_multiple_whitespaces(source_text)
    if method == "words":
        return chunk_text_by_num_words(
            source_text, max_chunk_words=token_length, overlap_fraction=overlap_fraction
        )
    elif method == "chars":
        return chunk_text_by_num_chars(
            source_text, max_chunk_chars=token_length, overlap_fraction=overlap_fraction
        )
    else:
        raise ValueError(f"Unsupported method: {method}")


def extract_metadata(video_info: Dict[str, Any]) -> Dict[str, Any]:
    metadata = dict()
    for k in ["title", "upload_date", "channel", "uploader"]:
        if k in video_info:
            metadata[k] = video_info[k]
    return metadata


def download_youtube(youtube_url: str, path_out: Path) -> Dict[str, Any]:
    """
    Download a YouTube video's audio and return its title
    :param youtube_url: URL of the YouTube video
    :param path_out: Path where the audio file will be saved
    :return: Video title
    """
    path_template = str(path_out.absolute())
    if path_template.endswith(".mp3"):
        path_template = path_template[:-4]

    yt_dlp_params = {
        "extract_audio": True,
        "format": "bestaudio/best",
        "audioformat": "mp3",
        "outtmpl": path_template,
        "quiet": True,
        "cachedir": False,
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ],
    }

    with yt_dlp.YoutubeDL(yt_dlp_params) as video:
        result = video.extract_info(youtube_url, download=True)
        metadata = extract_metadata(result)
        video_title = result["title"]
        logging.info(f"Found {video_title} - downloading")
        video.download(youtube_url)
        logging.info(f"Successfully downloaded to {path_out}")

    return metadata


def get_youtube_metadata(youtube_url: str) -> Dict[str, str]:
    """
    Download a YouTube video and return its metadata
    :param youtube_url:
    :param path_out:
    :param audio_only:
    :return: Video title
    """
    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "force_generic_extractor": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        result = ydl.extract_info(youtube_url, download=False)
        metadata = extract_metadata(result)
    return metadata


def download_youtube_video(youtube_url: str, path_out: Path) -> str:
    """
    Download a YouTube video's video
    :param youtube_url: URL of the YouTube video
    :param path_out: Path where the video file will be saved
    :return: Video title
    """
    path_template = str(path_out.absolute())
    if path_template.endswith(".mp4"):
        path_template = path_template[
            :-4
        ]  # Just in case, but usually this should be handled externally

    # Update yt_dlp parameters for video download
    yt_dlp_params = {
        "format": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]",  # Choose best video up to 1080p and best audio
        "outtmpl": f"{path_template}.%(ext)s",  # Define output template
        "quiet": True,
        "cachedir": False,
    }

    with yt_dlp.YoutubeDL(yt_dlp_params) as video:
        result = video.extract_info(youtube_url, download=True)
        video_title = result.get(
            "title", "Downloaded Video"
        )  # Fallback to a default title if not available
        logging.info(f"Found {video_title} - downloading")
        logging.info(f"Successfully downloaded to {path_out}")

    return video_title


def get_audio_from_video(video_path: Union[str, Path]) -> Path:
    from moviepy.editor import VideoFileClip

    if type(video_path) == str:
        video_pathobj = Path(video_path)
    else:
        video_pathobj = Path(video_path)

    audio_path = video_pathobj.with_suffix(".mp3")
    video = VideoFileClip(str(video_pathobj))

    audio = video.audio
    audio.write_audiofile(audio_path)

    # Close the video file
    video.close()

    return audio_path


def get_transcripts_from_audio_file(
    audio_file_path: Path,
    max_segment_len: int = 900,
    openai_apikey: Union[str, None] = None,
) -> List[str]:
    """
    Get transcripts of audio files using
    :param audio_file_path:
    :param max_segment_len:
    :param openai_apikey:
    :return:
    """
    oai_client = get_openai_client(openai_apikey)
    clip_outpaths = split_audio_files(audio_file_path, max_segment_len)
    transcript_texts = list()
    logging.info(f"Getting transcripts from {len(clip_outpaths)} audio files...")
    for i, clip_outpath in enumerate(clip_outpaths):
        logging.info(f"Processing transcript {i+1} of {len(clip_outpaths)}...")
        with clip_outpath.open("rb") as audio_file:
            transcript = oai_client.audio.transcriptions.create(
                model="whisper-1", file=audio_file
            )
            transcript_texts.append(transcript.text)

    # Clean up
    for clip_outpath in clip_outpaths:
        os.remove(clip_outpath)

    return transcript_texts


def split_audio_files(audio_file_path: Path, max_segment_len: int = 900) -> List[Path]:
    """
    Split long audio files
    (e.g. so that they fit within the allowed size for Whisper)
    :param audio_file_path:
    :param max_segment_len:
    :return: A list of file paths
    """
    from pydub import AudioSegment

    audio = AudioSegment.from_file(str(audio_file_path))
    logging.info(f"Splitting {audio_file_path} to chunks of {max_segment_len} seconds.")
    # Split long audio into segments
    clip_outpaths = list()
    if audio.duration_seconds > max_segment_len:
        n_segments = 1 + int(audio.duration_seconds) // max_segment_len
    else:
        n_segments = 1
    logging.info(f"Splitting audio to {n_segments}")
    for i in range(n_segments):
        start = max(0, (i * max_segment_len) - 5) * 1000
        end = ((i + 1) * max_segment_len) * 1000
        clip = audio[start:end]

        clip_outpath = audio_file_path.with_suffix(f".{i}.mp3")
        outfile = clip.export(str(clip_outpath))
        outfile.close()
        clip_outpaths.append(clip_outpath)
    return clip_outpaths


def get_yt_video_id(input_str: str) -> str:
    """
    Get the YouTube video ID (name) from a URL
    :param input_str:
    :return:
    """
    output_str = input_str

    # Remove the https:// if it exists
    if output_str.startswith("https://"):
        output_str = output_str.split("https://")[-1]

    # Remove the prefix
    if "watch?v=" in output_str:
        output_str = output_str.split("watch?v=")[-1]
    else:
        output_str = output_str.split("/")[-1]

    # Remove the suffix
    if "?" in output_str:
        output_str = output_str.split("?")[0]

    # Return the string
    if len(output_str) == 11:
        return output_str
    else:
        raise ValueError(f"Error parsing the video ID from {input_str}")
