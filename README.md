## README for distyll.info
**distyll.info**: Effortlessly distill content as required.

### Overview
**distyll.info** is a content-distillation tool designed to help you make the most out of online content. By providing a simple URL, users can have a video, article, or document distilled into its core essence, making information consumption faster and more efficient.

#### How to run
- Clone the repo
- Create a virtual environment
- Activate the virtual environment
- Run `pip install -r requirements.txt`
    - You may need to install `ffmpeg` for your system for the audio parsing to work
- Run the demo notebooks (`demo_arxiv.ipynb` or `demo_video.ipynb`)
- You may need to set the API key by modifying `db.set_apikey(openai_key=os.environ["OPENAI_APIKEY"])`

#### Components

- **Main components**
    - distyll.py: main (knowledge base) components
    - query.py: Pre-defined queries for the database
- **Utilties**
    - media.py: For dealing with source media (e.g. YouTube videos, PDFs, etc)
    - preprocessing.py: Text preprocessing utilities (chunking, remove whitespaces)
    - rag.py: Make retrieval augmented generation tasks easier

#### Notes
- Powered by [Weaviate](https://www.weaviate.io)
- Unofficial personal project - for educational purposes only

### Features
- **Convenient content ingestion**: Quickly add a resource for processing.
- **Content Summarization**: Extract the main points of any content.
- **Ask Questions**: Engage with the distilled content by asking questions and receiving concise answers.
- **Save generated outputs**: Save money on existing data

#### Tests
- run `python -m pytest tests`
