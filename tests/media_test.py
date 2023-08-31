import pytest
import os
import media
from pathlib import Path

DEMO_YOUTUBE_VIDEO = "https://youtu.be/ni3T4vStzBI"


@pytest.mark.parametrize(
    "youtube_url, path_out, audio_only, expected_title",
    [
        (DEMO_YOUTUBE_VIDEO, Path("tempdata/test.mp4"), False,
         "Stanford XCS224U: NLU I Contextual Word Representations, Part 10: Wrap-up I Spring 2023"),
        (DEMO_YOUTUBE_VIDEO, Path("tempdata/test.mp3"), True,
         "Stanford XCS224U: NLU I Contextual Word Representations, Part 10: Wrap-up I Spring 2023")
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


@pytest.mark.parametrize(
    "max_segment_len, n_segments",
    [
        (900, 1),
        (100, 4),
        (50, 8),
    ]
)
def test_split_audio_files(max_segment_len, n_segments):
    # Download file
    audio_file_path = Path("tempdata/ni3T4vStzBI.mp3")
    media.download_youtube(DEMO_YOUTUBE_VIDEO, audio_file_path, True)

    # Test
    split_files = media.split_audio_files(audio_file_path, max_segment_len)
    assert len(split_files) == n_segments
    # Clean up
    for f in split_files:
        os.remove(f)


@pytest.mark.parametrize(
    "max_segment_len, n_segments",
    [
        (900, 1),
        (100, 4),
    ]
)
def test_get_transcripts_from_audio_file(max_segment_len, n_segments):
    # Download file
    audio_file_path = Path("tempdata/ni3T4vStzBI.mp3")
    media.download_youtube(DEMO_YOUTUBE_VIDEO, audio_file_path, True)

    # Test
    transcripts = media.get_transcripts_from_audio_file(audio_file_path, max_segment_len)
    assert len(transcripts) == n_segments
    for t in transcripts:
        assert type(t) == str
