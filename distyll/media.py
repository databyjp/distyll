import json

from distyll.utils import get_arxiv_title
from distyll.utils import (
    get_transcripts_from_audio_file,
    get_youtube_metadata,
    download_youtube,
    init_dl_dir
)
from pypdf import PdfReader
from typing import Union, Dict
from pathlib import Path
import requests
import logging
import distyll.loggerconfig


DL_DIR = "dl_data"



def _download_pdf(pdf_url: str, dl_dir: Union[str, Path] = DL_DIR) -> Path:
    """
    Get the text from a PDF and parse it
    :param pdf_url:
    :param dl_dir
    :return:
    """
    logging.info(f"Downloading {pdf_url} text")
    pdf_filename = pdf_url.split("/")[-1]

    # Set up download
    dl_dir = init_dl_dir(dl_dir)
    out_path = Path(dl_dir) / pdf_filename

    # Does file exist already
    if out_path.exists():
        return out_path

    # Get PDF file
    else:
        response = requests.get(pdf_url)
        with out_path.open("wb") as f:
            f.write(response.content)

    return out_path


def _parse_pdf(pdf_path: Union[Path, str]) -> str:
    """
    Read contents of a PDF files
    :param pdf_path:
    :return:
    """
    logging.info(f"Parsing text from {pdf_path}")
    if type(pdf_path) == str:
        pdf_path = Path(pdf_path)
    pdf_reader = PdfReader(pdf_path)

    # Initialize a string to store the text content
    pdf_text = ""
    n_pages = len(pdf_reader.pages)

    # Iterate through the pages and extract the text
    for page_num in range(n_pages):
        page = pdf_reader.pages[page_num]
        pdf_text += "\n" + page.extract_text()
    return pdf_text


def download_and_parse_pdf(pdf_url: str) -> str:
    """
    Downloads a PDF file from the specified URL and parses its text content.

    :param pdf_url: The URL of the PDF file to download and parse.
    :return: The parsed text content of the PDF file.
    """
    logging.info(f"Downloading and reading text from {pdf_url}")
    pdf_path = _download_pdf(pdf_url)
    pdf_text = _parse_pdf(pdf_path)
    return pdf_text


def get_arxiv_paper(arxiv_url: str) -> Union[Dict[str, str], None]:
    """
    Retrieve arXiv paper information.

    :param arxiv_url: The URL of the arXiv paper.
    :return: A dictionary containing the title, URL, and text of the arXiv paper.
    """
    logging.info(f"Getting arXiV paper from {arxiv_url}")
    if "arxiv.org" not in arxiv_url:
        logging.info("URL is not from arxiv.org")
        return None

    # Get Arxiv paper ID
    arxiv_id = arxiv_url.split("/")[-1]
    if arxiv_id.endswith(".pdf"):
        arxiv_id = arxiv_id[:-4]
    txt_path = Path(DL_DIR) / f"{arxiv_id}.txt"

    # Get title
    title = get_arxiv_title(f"https://arxiv.org/abs/{arxiv_id}")

    # Check if text exists already
    if txt_path.exists():
        pdf_text = txt_path.read_text()
        return {"title": title, "url": arxiv_url, "text": pdf_text}
    else:
        pdf_text = download_and_parse_pdf(f"https://arxiv.org/pdf/{arxiv_id}.pdf")
        with txt_path.open("w") as f:
            f.write(pdf_text)
        return {"title": title, "url": arxiv_url, "text": pdf_text}


def get_youtube_transcript(
    yt_url: str, dl_dir: Union[str, Path] = DL_DIR, openai_apikey: str = None
) -> Dict[str, str]:
    """
    Retrieves the transcript of a YouTube video.

    :param yt_url: The URL of the YouTube video.
    :param dl_dir: (Optional) The directory to download the video to.
    :param openai_apikey: (Optional) OpenAI API key.
    :return: A dictionary containing the video title, the YouTube URL, and the transcript texts.
    """
    logging.info(f"Processing {yt_url}, just getting the video title.")
    # Set up download
    dl_dir = init_dl_dir(dl_dir)
    video_id = yt_url.split("/")[-1]
    transcript_json = video_id + ".json"
    transcript_json_path = Path(dl_dir) / transcript_json
    yt_filename = video_id + ".mp3"
    yt_out_path = Path(dl_dir) / yt_filename

    if not transcript_json_path.exists():
        if yt_out_path.exists():
            logging.info(f"Already downloaded {yt_filename}, just getting the video title.")
            video_metadata = get_youtube_metadata(youtube_url=yt_url)
        else:
            logging.info(f"Downloading {yt_filename}, just getting the video title.")
            video_metadata = download_youtube(youtube_url=yt_url, path_out=yt_out_path)

        video_title = video_metadata["title"]
        video_date = video_metadata["upload_date"]
        video_uploader = video_metadata["uploader"]
        channel = video_metadata["channel"]

        transcript_texts = get_transcripts_from_audio_file(yt_out_path, openai_apikey=openai_apikey)
        transcript_data = {
            "title": video_title,
            "date": video_date,
            "yt_url": yt_url,
            "uploader": video_uploader,
            "channel": channel,
            "transcripts": transcript_texts,
        }
        transcript_json_path.write_text(json.dumps(transcript_data))
        return transcript_data
    else:
        logging.info(f"Already downloaded {video_id}")
        return json.loads(transcript_json_path.read_text())
