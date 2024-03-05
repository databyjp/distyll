from distyll.transcripts import from_local_video, from_youtube

from distyll.text import (
    _download_pdf,
    _parse_pdf,
    from_pdf,
    from_arxiv_paper,
)
from distyll.utils import download_youtube_video, get_yt_video_id
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
        "tests/test_data/1706.03762.pdf",
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
def test_text_from_pdf(pdf_url, val_strings):
    pdf_str = from_pdf(pdf_url)
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
def test_text_from_arxiv_paper(pdf_url, val_strings, title):
    arxiv_data = from_arxiv_paper(pdf_url)
    for val_str in val_strings:
        assert val_str in arxiv_data["text"]
    assert arxiv_data["title"] == title
    assert arxiv_data["url"] == pdf_url


youtube_testdata = [
    (
        "https://youtu.be/6GEMkvT0DEk",
        "blueprints",
    ),
    (
        "https://www.youtube.com/watch?v=EYXQmbZNhy8",
        "Cake",
    ),
]


@pytest.mark.parametrize("yt_url, title", youtube_testdata)
def test_transcripts_from_youtube(yt_url, title):
    youtube_data = from_youtube(yt_url)
    assert title in youtube_data["title"]
    assert yt_url in youtube_data["yt_url"]
    assert len(youtube_data["transcripts"][0]) > 1000


@pytest.mark.parametrize("yt_url, _", youtube_testdata)
def test_transcripts_from_local_video(yt_url, _):
    video_path = Path("temp/dl_data") / (get_yt_video_id(yt_url) + ".mp4")
    download_youtube_video(yt_url, video_path)
    assert video_path.exists()
    transcript = from_local_video(video_path)
    assert len(transcript[0]) > 1000
