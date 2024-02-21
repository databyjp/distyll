For the old version - please see the `distyll_old` branch.

## Installation

Install `ffmpeg` (`brew install ffmpeg` on macOS)
Install packages from `requirements.txt` (`pip install -r requirements.txt`)

## Usage

See `demo.ipynb` for an example.

- download_and_parse_pdf(pdf_url) -> pdf_text
- get_arxiv_paper(arxiv_url) -> {"title": title, "url": arxiv_url, "text": pdf_text}
- get_youtube_transcript(youtube_url) -> {"title": title, "date": date, "yt_url": youtube_url, "uploader": uploader, "channel": channel, "transcripts": transcripts}
