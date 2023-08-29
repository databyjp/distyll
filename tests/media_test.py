import pytest
import os
import media
from pathlib import Path


@pytest.mark.parametrize(
    "youtube_url, path_out, audio_only",
    [
        ("SOME_SHORT_YOUTUBE_VIDEO", Path("tempdata/test.mp4"), False, "VIDEO_TITLE"),
        ("SOME_SHORT_YOUTUBE_VIDEO", Path("tempdata/test.mp3"), True, "VIDEO_TITLE")
    ]
)
def test_download_youtube(youtube_url, path_out, audio_only, expected_title):
    if path_out.exists():
        os.remove(path_out)
    assert not path_out.exists()
    video_title = media.download_youtube(youtube_url, path_out, audio_only)
    assert video_title == expected_title
    assert path_out.exists()
    os.remove(path_out)
