## Installation

Install `ffmpeg` (`brew install ffmpeg` on macOS)
Install the package with `pip install distyll-info`

## Usage

- download_and_parse_pdf(pdf_url) -> pdf_text
- get_arxiv_paper(arxiv_url) -> {"title": title, "url": arxiv_url, "text": pdf_text}
- get_youtube_transcript(youtube_url) -> {"title": title, "date": date, "yt_url": youtube_url, "uploader": uploader, "channel": channel, "transcripts": List[transcript]}
- get_transcripts_from_video(video_url) -> List[transcript]

## What happened to the old version?

Sorry! I'm working on making this more streamlined and better. For the old version, please see the `distyll_old` branch.
