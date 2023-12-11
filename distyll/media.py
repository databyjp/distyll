from distyll.utils import get_arxiv_title
from distyll.utils import (
    get_transcripts_from_audio_file,
    get_youtube_title,
    download_youtube,
)
from pypdf import PdfReader
from typing import Union, Dict
from pathlib import Path
import requests
import logging
import distyll.loggerconfig


DL_DIR = "dl_data"


def init_dl_dir(dir_path: Union[str, Path]) -> Path:
    if type(dir_path) is str:
        dir_path = Path(dir_path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


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
    Get the text from a PDF and parse it
    :param pdf_url:
    :return:
    """
    logging.info(f"Downloading and reading text from {pdf_url}")
    pdf_path = _download_pdf(pdf_url)
    pdf_text = _parse_pdf(pdf_path)
    return pdf_text


def get_arxiv_paper(arxiv_url: str) -> Union[Dict[str, str], None]:
    """
    Get arXiv paper text
    :param arxiv_url: Paper URL, e.g. 'https://arxiv.org/pdf/2305.15334' or 'https://arxiv.org/pdf/2305.15334.pdf'
    :return:
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
    yt_url: str, dl_dir: Union[str, Path] = DL_DIR
) -> Dict[str, str]:
    logging.info(f"Processing {yt_url}, just getting the video title.")
    # Set up download
    dl_dir = init_dl_dir(dl_dir)
    yt_filename = yt_url.split("/")[-1] + ".mp3"
    out_path = Path(dl_dir) / yt_filename

    # Does file exist already
    if out_path.exists():
        logging.info(f"Already downloaded {yt_filename}, just getting the video title.")
        video_title = get_youtube_title(youtube_url=yt_url)
    else:
        logging.info(f"Downloading {yt_filename}, just getting the video title.")
        video_title = download_youtube(youtube_url=yt_url, path_out=out_path)

    transcript_texts = get_transcripts_from_audio_file(out_path)
    return {
        "title": video_title,
        "yt_url": yt_url,
        "transcripts": transcript_texts,
    }
