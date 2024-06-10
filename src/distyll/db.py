from weaviate import WeaviateClient
from weaviate.classes.config import Property, DataType, Configure
from weaviate.util import generate_uuid5
import distyll
from distyll.utils import chunk_text
import distyll.config
from distyll.config import COLLECTION_NAME


def prep_db(client: WeaviateClient) -> None:
    """
    Prepare the database for use
    :param client: Weaviate client
    :return: None
    """
    if client.collections.exists(COLLECTION_NAME):
        pass
    else:
        client.collections.create(
            COLLECTION_NAME,
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
    chunks_collection = client.collections.get(COLLECTION_NAME)
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


def add_arxiv_to_db(client: WeaviateClient, arxiv_url: str) -> int:
    """
    Add an arXiv paper to the database
    :param client: Weaviate client
    :param arxiv_url: arXiv URL
    :return: Number of chunks added
    """
    prep_db(client)
    arxiv_data = distyll.text.from_arxiv_paper(arxiv_url)
    chunks_collection = client.collections.get(COLLECTION_NAME)
    chunk_no = 0
    with chunks_collection.batch.fixed_size() as batch:
        for chunk in chunk_text(arxiv_data["text"]):
            batch.add_object(
                properties={
                    "title": arxiv_data["title"],
                    "url": arxiv_url,
                    "chunk": chunk,
                    "chunk_no": chunk_no,
                },
                uuid=generate_uuid5(chunk),
            )
            chunk_no += 1

    print(f"Added {chunk_no} chunks to the database")
    return chunk_no


def add_pdf_to_db(client: WeaviateClient, pdf_url: str) -> int:
    """
    Add a PDF file to the database
    :param client: Weaviate client
    :param pdf_url: PDF URL
    :return: Number of chunks added
    """
    prep_db(client)
    pdf_text = distyll.text.from_pdf(pdf_url)
    chunks_collection = client.collections.get(COLLECTION_NAME)
    chunk_no = 0
    with chunks_collection.batch.fixed_size() as batch:
        for chunk in chunk_text(pdf_text):
            batch.add_object(
                properties={
                    "title": pdf_url,
                    "url": pdf_url,
                    "chunk": chunk,
                    "chunk_no": chunk_no,
                },
                uuid=generate_uuid5(chunk),
            )
            chunk_no += 1

    print(f"Added {chunk_no} chunks to the database")
    return chunk_no
