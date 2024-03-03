import pytest
from distyll.utils import download_youtube_video, get_yt_video_name
import logging
import distyll.loggerconfig
from pathlib import Path


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
def test_yt_video_download(yt_url, title):
    video_path = Path("temp/dl_data") / (get_yt_video_name(yt_url) + ".mp4")
    download_youtube_video(yt_url, video_path)
