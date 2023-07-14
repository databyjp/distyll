## Personal knowledge base app

### Goals

Help me learn about stuff with a workflow of:
- Dump information into this database
- Help me digest it by summarizing

## How to use

You'll need an Open AI api key, and a Weaviate cluster

### Connect

Spin up the Weaviate cluster, and run this :

```python
import wkb
client = wkb.start_db()
collection = wkb.Collection(client, wkb.WV_CLASS)
```

This will instantiate, or connect to, the knowledge base collection in Weaviate.

**NOTE: Currently, it is configured to persist data in the `dbdata` subdirectory.**

### Add objects

You can add objects from various sources:

```python
collection.add_text_file(PATH_TO_FILE)  # Add text from a local text file
collection.add_wiki_article(wiki_title)  # Add Wiki article summary (use Wikipedia article title)
collection.add_from_youtube(youtube_url)  # Add youtube video transcript (use YouTube URL)
```

They will be added like: 

```json
{
  "source_path": source_path,  // e.g. YouTube URL or Wiki title
  "source_text": text,  // Text body (chunk)
  "chunk_number": xx  // Chunk number if chunked from longer text
}
```

### Query objects

You can use the data like so:

```python
# Source-specific
collection.generate_summary(youtube_url)  # Summarize the entry
collection.generate_summary(youtube_url, "Summarize into a tweet")  # Summarize the entry, using this specific instruction
collection.ask_object(youtube_url, "What is HNSW-FINGER?")  # Ask this particular source 

# Knowledge-base wide actions
collection.text_search("kubernetes", 2)  # Vector search
collection.summarize_topic("kubernetes")  # Vector search & summarize results
```
