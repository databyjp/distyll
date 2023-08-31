import pytest
import os
import media
from pathlib import Path


# @pytest.mark.parametrize(
#     "youtube_url, path_out, audio_only, expected_title",
#     [
#         ("https://youtu.be/sNw40lEhaIQ", Path("tempdata/test.mp4"), False,
#          "Stanford XCS224U: NLU I Contextual Word Representations, Part 4: GPT I Spring 2023"),
#         ("https://youtu.be/sNw40lEhaIQ", Path("tempdata/test.mp3"), True,
#          "Stanford XCS224U: NLU I Contextual Word Representations, Part 4: GPT I Spring 2023")
#     ]
# )
# def test_download_youtube(youtube_url, path_out, audio_only, expected_title):
#     if path_out.exists():
#         os.remove(path_out)
#     assert not path_out.exists()
#     video_title = media.download_youtube(youtube_url, path_out, audio_only)
#     assert video_title == expected_title
#     assert path_out.exists()
#     os.remove(path_out)
#
#
# @pytest.mark.parametrize(
#     "audio_file_path, max_segment_len, n_segments",
#     [
#         (Path("tempdata/sNw40lEhaIQ.mp3"), 900, 1),
#         (Path("tempdata/sNw40lEhaIQ.mp3"), 300, 3),
#         (Path("tempdata/sNw40lEhaIQ.mp3"), 100, 9)
#     ]
# )
# def test_split_audio_files(audio_file_path, max_segment_len, n_segments):
#     split_files = media.split_audio_files(audio_file_path, max_segment_len)
#     assert len(split_files) == n_segments
#     # Clean up
#     for f in split_files:
#         os.remove(f)


@pytest.mark.parametrize(
    "audio_file_path, max_segment_len, n_segments",
    [
        (Path("tempdata/sNw40lEhaIQ.mp3"), 900, 1),
        (Path("tempdata/sNw40lEhaIQ.mp3"), 300, 3),
    ]
)
def test_get_transcripts_from_audio_file(audio_file_path, max_segment_len, n_segments):
    transcripts = media.get_transcripts_from_audio_file(audio_file_path, max_segment_len)
    assert len(transcripts) == n_segments
    for t in transcripts:
        assert type(t) == str
