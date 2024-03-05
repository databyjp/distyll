from weaviate import WeaviateClient
from weaviate.classes.config import Property, DataType, Configure
from weaviate.util import generate_uuid5
import distyll
from distyll.utils import chunk_text
import distyll.config


def prep_db(client: WeaviateClient) -> None:
    """
    Prepare the database for use
    :param client: Weaviate client
    :return: None
    """
    if client.collections.exists("TextChunk"):
        pass
    else:
        client.collections.create(
            "TextChunk",
            properties=[
                Property(name="title", data_type=DataType.TEXT),
                Property(name="url", data_type=DataType.TEXT, skip_vectorization=True),
                Property(name="chunk", data_type=DataType.TEXT),
                Property(name="chunk_no", data_type=DataType.INT),
            ],
            vectorizer_config=Configure.Vectorizer.text2vec_openai(),
            generative_config=Configure.Generative.openai(
                model=distyll.config.load_gen_model()
            ),
        )


def add_yt_to_db(client: WeaviateClient, yt_url) -> int:
    """
    Add a YouTube video to the database
    :param client: Weaviate client
    :param yt_url: YouTube URL
    :return: Number of chunks added
    """
    prep_db(client)
    transcript_data = distyll.transcripts.from_youtube(yt_url)
    chunks_collection = client.collections.get("TextChunk")
    chunk_no = 0
    with chunks_collection.batch.fixed_size() as batch:
        for t in transcript_data["transcripts"]:
            for _, transcript in enumerate(transcript_data["transcripts"]):
                for chunk in chunk_text(transcript):
                    batch.add_object(
                        properties={
                            "title": transcript_data["title"],
                            "url": transcript_data["yt_url"],
                            "chunk": chunk,
                            "chunk_no": chunk_no,
                        },
                        uuid=generate_uuid5(chunk),
                    )
                    chunk_no += 1
    print(f"Added {chunk_no} chunks to the database")
    return chunk_no
