import pytest
from distyll.utils import download_youtube_video, get_yt_video_name
import logging
import distyll.loggerconfig
from pathlib import Path


youtube_testdata = [
    # (
    #     "https://youtu.be/enRb6fp5_hw",
    #     "Stanford XCS224U",
    # ),
    (
        "https://www.youtube.com/watch?v=EYXQmbZNhy8",
        "cake",
    ),
]


@pytest.mark.parametrize("yt_url, title", youtube_testdata)
def test_get_audio_from_video(yt_url, title):
    video_path = Path("temp/dl_data") / (get_yt_video_name(yt_url) + ".mp4")
    download_youtube_video(yt_url, video_path)
