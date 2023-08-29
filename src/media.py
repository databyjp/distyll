import os
import yt_dlp
from pathlib import Path


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
        print(f"Found {video_title} - downloading")
        video.download(youtube_url)
        print(f"Successfully Downloaded to {path_out}")
    return video_title


