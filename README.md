## README for distyll.info
**distyll.info**: Effortlessly distill content into its essence.

### Overview
**distyll.info** is a content-distillation tool designed to help you make the most out of online content. By providing a simple URL, users can have a video, article, or document distilled into its core essence, making information consumption faster and more efficient.

#### How to run
- Clone the repo
- Create a virtual environment
- Activate the virtual environment
- Run `pip install -r requirements.txt`
- Run `python -m app.main`

#### Notes
- Powered by [Weaviate](https://www.weaviate.io)
- Unofficial personal project - for educational purposes only

### Features
- **Convenient content ingestion**: Quickly add a resource for processing.
- **Content Summarization**: Extract the main points of any content.
- **Ask Questions**: Engage with the distilled content by asking questions and receiving concise answers.

### Getting Started
- **Add object**: Use the `/add/` endpoint to start the distillation process.
- **Summarize**: Once processed, access the `/summarise/` endpoint to get a brief summary.
- **Ask Away**: Engage with the content using the `/ask/` endpoint to get answers related to the distilled content.

### Future Updates
Keep an eye out for continuous improvements and additional features to help make your content consumption even smoother!

#### Tests
run `python -m pytest tests`
