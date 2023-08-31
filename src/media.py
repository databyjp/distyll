import os
from typing import List
import openai
import yt_dlp
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

openai.api_key = os.environ["OPENAI_APIKEY"]


def download_youtube(youtube_url: str, path_out: Path, audio_only: bool = True) -> str:
    """
    Download a YouTube video and return its title
    :param youtube_url:
    :param path_out:
    :param audio_only:
    :return: Video title
    """
    if audio_only:
        yt_dlp_params = {
            'extract_audio': True,
            'format': 'bestaudio',
            'outtmpl': str(path_out.absolute()), 'quiet': True, 'cachedir': False
        }
    else:
        yt_dlp_params = {
            'format': 'best',
            'outtmpl': str(path_out.absolute()), 'quiet': True, 'cachedir': False
        }

    with yt_dlp.YoutubeDL(yt_dlp_params) as video:

        if path_out.exists():
            # TODO - add something for overwriting deleting data
            pass

        info_dict = video.extract_info(youtube_url, download=True)
        video_title = info_dict['title']
        logger.info(f"Found {video_title} - downloading")
        video.download(youtube_url)
        logger.info(f"Successfully Downloaded to {path_out}")
    return video_title


def get_transcripts_from_audio_file(audio_file_path: Path, max_segment_len: int = 900) -> List[str]:
    """
    Get transcripts of audio files using
    :param audio_file_path:
    :param max_segment_len:
    :return:
    """

    clip_outpaths = split_audio_files(audio_file_path, max_segment_len)
    transcript_texts = list()
    logger.info(f"Getting transcripts from {len(clip_outpaths)} audio files...")
    for i, clip_outpath in enumerate(clip_outpaths):
        logger.info(f"Processing transcript {i+1} of {len(clip_outpaths)}...")
        with clip_outpath.open('rb') as audio_file:
        # with open(clip_outpath, "rb") as audio_file:
            transcript = openai.Audio.transcribe("whisper-1", audio_file)
            transcript_texts.append(transcript["text"])

    # Clean up
    for clip_outpath in clip_outpaths:
        os.remove(clip_outpath)

    return transcript_texts


def split_audio_files(audio_file_path: Path, max_segment_len: int = 900) -> List[Path]:
    """
    Split long audio files
    (e.g. so that they fit within the allowed size for Whisper)
    :param audio_file_path:
    :param max_segment_len:
    :return: A list of file paths
    """
    from pydub import AudioSegment

    audio = AudioSegment.from_file(str(audio_file_path))
    logger.info(f"Splitting {audio_file_path} to chunks of {max_segment_len} seconds.")

    # Split long audio into segments
    clip_outpaths = list()
    if audio.duration_seconds > max_segment_len:
        n_segments = 1 + int(audio.duration_seconds) // max_segment_len
    else:
        n_segments = 1
    logger.info(f"Splitting audio to {n_segments}")
    for i in range(n_segments):
        start = max(0, (i * max_segment_len) - 10) * 1000
        end = ((i + 1) * max_segment_len) * 1000
        clip = audio[start:end]
        clip_outpath = audio_file_path.with_suffix(f'.{i}.mp3')
        outfile = clip.export(str(clip_outpath))
        outfile.close()
        clip_outpaths.append(clip_outpath)
    return clip_outpaths
