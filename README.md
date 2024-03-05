## Installation

Install `ffmpeg` (`brew install ffmpeg` on macOS)
Install the package with `pip install distyll-info`

## Usage

- distyll.text.from_pdf(pdf_url) -> pdf_text
- distyll.text.from_arxiv_paper(arxiv_url) -> {"title": title, "url": arxiv_url, "text": pdf_text}
- distyll.transcripts.from_youtube(youtube_url) -> {"title": title, "date": date, "yt_url": youtube_url, "uploader": uploader, "channel": channel, "transcripts": List[transcript]}
- distyll.transcripts.from_local_video(video_url) -> List[transcript]

Please see the docstrings for more information.

### API keys

OpenAI: Audio -> text functionalities make use of OpenAI's Whisper. You will need an API key to use this functionality.
- Option 1: Provide it as an argument to the function.
- Option 2: Set it using `distyll.set_api_key(openai=<YOUR_API_KEY>)`.
- Option 3: Set it in the `OPENAI_APIKEY` environment variable.

## What happened to the old version?

Sorry! I'm working on making this more streamlined and better. For the old version, please see the `distyll_old` branch.
