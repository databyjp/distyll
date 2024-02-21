from weaviate import WeaviateClient
from weaviate.classes.config import Property, DataType, Configure
import distyll

def prep_db(client: WeaviateClient):
    if client.collections.exists("KnowledgeChunk"):
        pass
    else:
        client.collections.create(
            "KnowledgeChunk",
            properties=[
                Property(name="title", data_type=DataType.TEXT),
                Property(name="url", data_type=DataType.TEXT, skip_vectorization=True),
                Property(name="chunk", data_type=DataType.TEXT),
                Property(name="chunk_no", data_type=DataType.INT),
            ],
            vectorizer_config=Configure.Vectorizer.text2vec_openai(),
            generative_config=Configure.Generative.openai(model="gpt-4-1106-preview"),
        )


def add_yt_to_db(client: WeaviateClient, yt_url):
    transcript_data = distyll.get_youtube_transcript(yt_url)
    chunks_collection = client.collections.get("KnowledgeChunk")
    chunk_no = 0
    with chunks_collection.batch.fixed_size() as batch:
        for t in transcript_data["transcripts"]:
            for _, chunk in enumerate(distyll.chunk_text(transcript_data["text"])):
                batch.add_object(
                    properties={
                        "title": transcript_data["title"],
                        "url": transcript_data["title"],
                        "chunk": chunk,
                        "chunk_no": chunk_no,
                    }
                )
                chunk_no += 1
    print(f"Added {chunk_no} chunks to the database")
    return chunk_no
