## Personal knowledge base app

- Powered by [Weaviate](https://www.weaviate.io)
- Unofficial personal project - for educational purposes only 

### Motivation

I wrote this to help me consume content & learn. The idea being that I could:
- Dump information into this knowledge base, and
- Help me digest it by:
    - getting summaries
    - asking it questions
    - having it suggest follow-up topic / sub-topics

## How to use

**See demo_notebook.ipynb for a quick demo of what you can do with it.**

You'll need an OpenAI api key

### Connect

#### Default configuration

The app is set up to use `Embedded Weaviate` with `openai` modules, and save all data to one collection (`Knowledge_chunk` class). To modify these, review/modify:

`instantiate_weaviate()`, `WV_CLASS` and `BASE_CLASS_OBJ` respectively.

#### How

Spin up the Weaviate cluster, and run this :

```python
import wkb
client = wkb.start_db()
collection = wkb.Collection(client, wkb.WV_CLASS)
```

This will instantiate, or connect to, the knowledge base collection in Weaviate.

**NOTE: Currently, it is configured to persist data in the default location for Embedded Weaviate.**

### Add objects

You can add objects from various sources:

```python
collection.add_text_file(PATH_TO_FILE)  # Add text from a local text file
collection.add_wiki_article(wiki_title)  # Add Wiki article summary (use Wikipedia article title)
collection.add_from_youtube(youtube_url)  # Add youtube video transcript (use YouTube URL)
collection.add_from_pdf_online(pdf_url)  # Add from PDF (using a URL)
collection.add_from_pdf_local(pdf_path)  # Add from PDF (using a local path)
```

They will be added like: 

```json
{
  "source_path": source_path,  // e.g. YouTube URL or Wiki title
  "source_text": text,  // Text body (chunk),
  "source_title": title_text, // Title - where applicable,
  "chunk_number": xx  // Chunk number if chunked from longer text
}
```

### Query objects

You can use the data like so:

```python
# Source-specific
collection.generate_summary(youtube_url)  # Summarize the entry
collection.generate_summary(youtube_url, "Summarize this into a tweet")  # Summarize the entry, using this specific instruction
collection.ask_object(youtube_url, "What is Weaviate?")  # Ask this particular source 

# Knowledge-base wide actions
collection.text_search("kubernetes", 2)  # Vector search
collection.summarize_topic("kubernetes")  # Vector search & summarize results
```

### Why doesn't this have a front end?

I'm working on it - it's just a demo project - please bear with me. 😅
