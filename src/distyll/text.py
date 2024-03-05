from distyll.utils import get_arxiv_title
from distyll.utils import (
    init_dl_dir,
)
from distyll.config import DL_DIR
from pypdf import PdfReader
from typing import Union, Dict
from pathlib import Path
import requests
import logging


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


def from_pdf(pdf_url: str) -> str:
    """
    Downloads a PDF file from the specified URL and parses its text content.

    :param pdf_url: The URL of the PDF file to download and parse.
    :return: The parsed text content of the PDF file.
    """
    logging.info(f"Downloading and reading text from {pdf_url}")
    pdf_path = _download_pdf(pdf_url)
    pdf_text = _parse_pdf(pdf_path)
    return pdf_text


def from_arxiv_paper(arxiv_url: str) -> Union[Dict[str, str], None]:
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
        pdf_text = from_pdf(f"https://arxiv.org/pdf/{arxiv_id}.pdf")
        with txt_path.open("w") as f:
            f.write(pdf_text)
        return {"title": title, "url": arxiv_url, "text": pdf_text}
