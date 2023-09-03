from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from query import RAGResponse
from dataclasses import asdict
import distyll
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
)

logger = logging.getLogger(__name__)


def instantiate_wcs():
    import weaviate
    import os
    client = weaviate.Client(
        url=os.environ['JP_WCS_URL'],
        auth_client_secret=weaviate.AuthApiKey(os.environ['JP_WCS_ADMIN_KEY']),
        additional_headers={
            'X-OpenAI-Api-Key': os.environ['OPENAI_APIKEY']
        }
    )
    return client


db = distyll.DBConnection(client=instantiate_wcs())

app = FastAPI(
    title="distyll.info",
    description="Effortlessly distill content into its essence.",
    version="0.1"
)


class URLSubmission(BaseModel):
    url: str


class QuerySummary(BaseModel):
    url: str
    question: str


class QueryChunks(BaseModel):
    url: str
    question: str
    search_query: str


@app.get("/")
def root():
    obj_counts = db.get_total_object_counts()
    return {
        "message": f"Hello there! You have {obj_counts['source_count']} source objects totalling {obj_counts['chunk_count']} chunks."}


def get_object_count(url: str) -> bool:
    """
    When a url is received, add it to the list of sources in COLLECTION_NAME_SOURCES class
    :param url:
    :return:
    """
    # Check if the resource is in a valid format
    obj_count = db.get_entry_count(source_path=url)
    if obj_count > 0:
        return True
    else:
        return False


def add_pdf(url: str):
    logger.info(f"Adding {url} to the database")
    n_chunks_added = db.add_arxiv(url)
    logger.info(f"Added {n_chunks_added} chunks from {url}")
    return True


@app.post("/arxiv/add/")
def process_add_pdf(submission: URLSubmission, background_tasks: BackgroundTasks):
    status = get_object_count(submission.url)
    if status:
        return {"status": f"{submission.url} is already in the database!"}
    else:
        background_tasks.add_task(add_pdf, submission.url)
        return {"status": "URL processing started"}


@app.post("/arxiv/query_summary/")
def query_summary(submission: QuerySummary):
    # rag_response = db.query_summary(
    #     prompt=submission.question,
    #     object_path=submission.url
    # )
    rag_response = RAGResponse(
        generated_text=submission.question,
        objects=[{"something": "something"}],
        error=None
    )
    return asdict(rag_response)


@app.post("/arxiv/query/")
def query_chunks(submission: QueryChunks):
    # rag_response = db.query_chunks(
    #     prompt=submission.question,
    #     search_query=submission.search_query,
    #     object_path=submission.url
    # )
    rag_response = RAGResponse(
        generated_text=submission.question,
        objects=[{"something": "something"}],
        error=None
    )
    return asdict(rag_response)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
