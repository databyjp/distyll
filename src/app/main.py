from typing import List

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import distyll

client = distyll.start_db()
chunks = distyll.Collection(client, distyll.COLLECTION_NAME_CHUNKS)
sources = distyll.Collection(client, distyll.COLLECTION_NAME_SOURCES)

app = FastAPI(title="distyll.info", description="Effortlessly distill content into its essence.", version="0.1")


def add_to_weaviate(url: str) -> bool:
    """
    Parse the data and add to Weaviate
    This will take a few seconds; so it is processed as an async background task
    :param url:
    :return:
    """
    print(f"Adding {url} chunks to the database")
    chunks.add_pdf(url)
    print(f"Summarizing {url} chunks")
    summary = chunks.summarize_entry(url)
    print(f"Saving summary to DB")
    sources.add_object({
        "source_path": url,
        "body": summary
    })
    return True


# Mocking the Weaviate interaction function
def add_asset_to_list(url: str) -> str:
    """
    When a url is received, add it to the list of sources in COLLECTION_NAME_SOURCES class
    :param url:
    :return:
    """
    # Check if the resource is in a valid format
    if url.endswith(".pdf"):
        # Check if the data ingestion is complete
        obj_count = sources.get_entry_count(value=url)
        print(obj_count)
        if obj_count > 0:
            return "exists"
        else:
            return "pending"
    else:  # When URL type not supported
        return "not supported"


class URLSubmission(BaseModel):
    url: str


class Question(BaseModel):
    url: str
    question: str


@app.get("/")
async def root():
    obj_count = chunks.get_total_obj_count()
    return {"message": f"Why, hello there - you have {obj_count} objects."}


@app.post("/add/")
async def submit_url(submission: URLSubmission, background_tasks: BackgroundTasks):
    status = add_asset_to_list(submission.url)
    if status == "exists":
        return {"status": f"{submission.url} is already in the database!"}
    elif status == "pending":
        background_tasks.add_task(add_to_weaviate, submission.url)
        return {"status": "URL processing started"}
    elif status == "not supported":
        raise HTTPException(status_code=400, detail="This type of URL is not supported, sorry!")
    else:
        raise HTTPException(status_code=400, detail="URL is already being processed or exists")


@app.post("/ask/")
async def ask_about_content(query: Question):
    content, answer = chunks.ask_object(query.url, query.question)
    return {"content": content, "answer": answer}


@app.post("/summarise/")
def get_summary(submission: URLSubmission):
    response = (
        sources.client.query.get(sources)
        .with_where(distyll.source_filter(submission.url))
        .with_limit(1)
    )
    summary = response["data"]["Get"][sources.chunk_class][0][distyll.COLLECTION_BODY_PROPERTY]
    return {"summary": summary}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
