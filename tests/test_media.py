from distyll.media import (
    _download_pdf,
    _parse_pdf,
    download_and_parse_pdf,
    get_arxiv_paper,
    get_youtube_transcript,
)
from pathlib import Path
import pytest
import logging
import distyll.loggerconfig


pdf_urls = [("https://arxiv.org/pdf/1706.03762.pdf", "dl_data")]


@pytest.mark.parametrize("pdf_url, dl_dir", pdf_urls)
def test_download_pdf(pdf_url, dl_dir):
    outpath = Path(dl_dir) / pdf_url.split("/")[-1]
    if outpath.exists():
        outpath.unlink()
    assert not outpath.exists()
    new_outpath = _download_pdf(pdf_url=pdf_url, dl_dir=dl_dir)
    assert outpath.exists()
    assert outpath == new_outpath


pdf_data = [
    (
        "test_data/1706.03762.pdf",
        [
            "mechanisms have become an integral part of compelling sequence",
            "outperforms the best previously reported models",
        ],
    )
]


@pytest.mark.parametrize("pdf_path, val_strings", pdf_data)
def test_read_pdf(pdf_path, val_strings):
    for p in [pdf_path, Path(pdf_path)]:
        pdf_str = _parse_pdf(p)
        for val_str in val_strings:
            assert val_str in pdf_str


pdf_data = [
    (
        "https://arxiv.org/pdf/1706.03762.pdf",
        [
            "mechanisms have become an integral part of compelling sequence",
            "outperforms the best previously reported models",
        ],
    )
]


@pytest.mark.parametrize("pdf_url, val_strings", pdf_data)
def test_download_and_read_pdf(pdf_url, val_strings):
    pdf_str = download_and_parse_pdf(pdf_url)
    for val_str in val_strings:
        assert val_str in pdf_str


arxiv_data = [
    (
        "https://arxiv.org/pdf/1706.03762.pdf",
        [
            "mechanisms have become an integral part of compelling sequence",
            "outperforms the best previously reported models",
        ],
        "Attention Is All You Need",
    )
]


@pytest.mark.parametrize("pdf_url, val_strings, title", arxiv_data)
def test_get_arxiv_paper(pdf_url, val_strings, title):
    arxiv_data = get_arxiv_paper(pdf_url)
    for val_str in val_strings:
        assert val_str in arxiv_data["text"]
    assert arxiv_data["title"] == title
    assert arxiv_data["url"] == pdf_url


youtube_testdata = [
    (
        "https://youtu.be/enRb6fp5_hw",
        "Stanford: NLU Information Retrieval: Guiding Ideas Spring 2023",
    )
]


@pytest.mark.parametrize("yt_url, title", youtube_testdata)
def test_get_youtube_transcript(yt_url, title):
    youtube_data = get_youtube_transcript(yt_url)
    assert "Stanford" in youtube_data["title"]
    assert yt_url in youtube_data["yt_url"]
    assert len(youtube_data["transcripts"][0]) > 1000
