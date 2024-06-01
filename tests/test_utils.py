import pytest
from distyll.utils import (
    download_youtube_video,
    get_yt_video_id,
    get_openai_client,
    set_api_key,
)
import logging
import distyll.loggerconfig
from pathlib import Path
import os


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
    video_path = Path("temp/dl_data") / (get_yt_video_id(yt_url) + ".mp4")
    download_youtube_video(yt_url, video_path)


openai_apikeys = [(os.getenv("OPENAI_APIKEY"), "a" * len(os.getenv("OPENAI_APIKEY")))]


@pytest.mark.parametrize("valid_key, invalid_key", openai_apikeys)
def test_get_openai_client(valid_key, invalid_key):
    for key in [valid_key, invalid_key]:
        client = get_openai_client(apikey=key)
        assert client.api_key == key
    set_api_key(openai="b" * len(valid_key))
    client = get_openai_client()
    assert client.api_key == "b" * len(valid_key)
