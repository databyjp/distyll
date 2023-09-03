import os
from io import BytesIO
from typing import List
import openai
import yt_dlp
from pathlib import Path
import logging
import requests
from pypdf import PdfReader
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

openai.api_key = os.environ["OPENAI_APIKEY"]


def download_youtube(youtube_url: str, path_out: Path, audio_only: bool = True) -> str:
    """
    Download a YouTube video and return its title
    :param youtube_url:
    :param path_out:
    :param audio_only:
    :return: Video title
    """
    if audio_only:
        yt_dlp_params = {
            'extract_audio': True,
            'format': 'bestaudio',
            'outtmpl': str(path_out.absolute()), 'quiet': True, 'cachedir': False
        }
    else:
        yt_dlp_params = {
            'format': 'best',
            'outtmpl': str(path_out.absolute()), 'quiet': True, 'cachedir': False
        }

    with yt_dlp.YoutubeDL(yt_dlp_params) as video:

        if path_out.exists():
            # TODO - add something for overwriting deleting data
            pass

        info_dict = video.extract_info(youtube_url, download=True)
        video_title = info_dict['title']
        logger.info(f"Found {video_title} - downloading")
        video.download(youtube_url)
        logger.info(f"Successfully Downloaded to {path_out}")
    return video_title


def get_transcripts_from_audio_file(audio_file_path: Path, max_segment_len: int = 900) -> List[str]:
    """
    Get transcripts of audio files using
    :param audio_file_path:
    :param max_segment_len:
    :return:
    """

    clip_outpaths = split_audio_files(audio_file_path, max_segment_len)
    transcript_texts = list()
    logger.info(f"Getting transcripts from {len(clip_outpaths)} audio files...")
    for i, clip_outpath in enumerate(clip_outpaths):
        logger.info(f"Processing transcript {i+1} of {len(clip_outpaths)}...")
        with clip_outpath.open('rb') as audio_file:
        # with open(clip_outpath, "rb") as audio_file:
            transcript = openai.Audio.transcribe("whisper-1", audio_file)
            transcript_texts.append(transcript["text"])

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
    logger.info(f"Splitting {audio_file_path} to chunks of {max_segment_len} seconds.")

    # Split long audio into segments
    clip_outpaths = list()
    if audio.duration_seconds > max_segment_len:
        n_segments = 1 + int(audio.duration_seconds) // max_segment_len
    else:
        n_segments = 1
    logger.info(f"Splitting audio to {n_segments}")
    for i in range(n_segments):
        start = max(0, (i * max_segment_len) - 10) * 1000
        end = ((i + 1) * max_segment_len) * 1000
        clip = audio[start:end]
        clip_outpath = audio_file_path.with_suffix(f'.{i}.mp3')
        outfile = clip.export(str(clip_outpath))
        outfile.close()
        clip_outpaths.append(clip_outpath)
    return clip_outpaths


def download_and_parse_pdf(pdf_url: str) -> str:
    """
    Get the text from a PDF and parse it
    :param pdf_url:
    :return:
    """
    logger.info(f"Parsing {pdf_url} text")
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

    logger.info(f"Finished parsing {n_pages} pages from {pdf_url}")
    # TODO - add tests
    return pdf_text


def get_arxiv_title(arxiv_url: str):
    response = requests.get(arxiv_url)

    # Check if the request was successful
    if response.status_code != 200:
        logger.info(f"Failed to get the page. HTTP status code: {response.status_code}")
        return None

    # Parse the HTML content of the page with BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the title element. The title is usually in a meta tag with name="citation_title"
    title_element = soup.find('meta', {'name': 'citation_title'})

    if title_element:
        return title_element['content']
    else:
        logger.info("Failed to find the title element")
        return None


def get_arxiv_paper(arxiv_url: str):
    # Example URL: 'https://arxiv.org/pdf/2305.15334'
    # Validate `arxiv.org` in URL
    if 'arxiv.org' not in arxiv_url:
        logger.info("URL is not from arxiv.org")
        return None

    # Get Arxiv paper ID
    arxiv_id = arxiv_url.split('/')[-1]

    # Get title
    abstract_url = f'https://arxiv.org/abs/{arxiv_id}'
    title = get_arxiv_title(abstract_url)

    # Get PDf
    pdf_url = f'https://arxiv.org/pdf/{arxiv_id}'
    pdf_text = download_and_parse_pdf(pdf_url)

    # Return object with title and PDF
    return {
        'title': title,
        'pdf_text': pdf_text
    }

