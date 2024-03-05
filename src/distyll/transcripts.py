import json

from distyll.utils import (
    get_transcripts_from_audio_file,
    get_youtube_metadata,
    download_youtube,
    init_dl_dir,
    get_yt_video_id,
    get_audio_from_video,
)
from distyll.config import DL_DIR
from typing import Union, Dict
from pathlib import Path
import logging


def from_youtube(
    yt_url: str, dl_dir: Union[str, Path] = DL_DIR, openai_apikey: str = None
) -> Dict[str, str]:
    """
    Retrieves the transcript of a YouTube video.

    :param yt_url: The URL of the YouTube video.
    :param dl_dir: (Optional) The directory to download the video to.
    :param openai_apikey: (Optional) OpenAI API key.
    :return: A dictionary containing the video title, the YouTube URL, and the transcript texts.
    """
    logging.info(f"Processing {yt_url}, just getting the video title.")
    # Set up download
    dl_dir = init_dl_dir(dl_dir)
    video_id = get_yt_video_id(yt_url)
    transcript_json = video_id + ".json"
    transcript_json_path = Path(dl_dir) / transcript_json
    yt_filename = video_id + ".mp3"
    yt_out_path = Path(dl_dir) / yt_filename

    if not transcript_json_path.exists():
        if yt_out_path.exists():
            logging.info(
                f"Already downloaded {yt_filename}, just getting the video title."
            )
            video_metadata = get_youtube_metadata(youtube_url=yt_url)
        else:
            logging.info(f"Downloading {yt_filename}, just getting the video title.")
            video_metadata = download_youtube(youtube_url=yt_url, path_out=yt_out_path)

        video_title = video_metadata["title"]
        video_date = video_metadata["upload_date"]
        video_uploader = video_metadata["uploader"]
        channel = video_metadata["channel"]

        transcript_texts = get_transcripts_from_audio_file(
            yt_out_path, openai_apikey=openai_apikey
        )
        transcript_data = {
            "title": video_title,
            "date": video_date,
            "yt_url": yt_url,
            "uploader": video_uploader,
            "channel": channel,
            "transcripts": transcript_texts,
        }
        transcript_json_path.write_text(json.dumps(transcript_data))
        return transcript_data
    else:
        logging.info(f"Already downloaded {video_id}")
        return json.loads(transcript_json_path.read_text())


def from_local_video(video_path: Union[str, Path], openai_apikey: str = None):
    audio_path = get_audio_from_video(video_path)
    transcript_texts = get_transcripts_from_audio_file(
        audio_path, openai_apikey=openai_apikey
    )
    return transcript_texts
