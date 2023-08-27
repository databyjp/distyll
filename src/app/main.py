from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import db

client = db.start_db()
chunks = db.Collection(client, db.COLLECTION_NAME_CHUNKS)
sources = db.Collection(client, db.COLLECTION_NAME_SOURCES)

app = FastAPI(
    title="distyll.info",
    description="Effortlessly distill content into its essence.",
    version="0.1"
)


def add_pdf_to_kb(url: str) -> bool:
    """
    Parse PDF data and add to knowledge base
    This will take a few seconds; so it is processed as an async background task
    :param url:
    :return:
    """
    print(f"Adding {url} chunks to the database")
    chunks.add_pdf(url)
    print(f"Summarizing {url} chunks")
    summary = chunks.summarize_entry(url)
    sources.add_object({
        "source_path": url,
        "body": summary
    })
    print(f"Saved summary to DB")
    return True


def add_text_to_kb(text: str, url: str) -> bool:
    """
    :param text:
    :param url:
    :return:
    """
    print(f"Adding {url} chunks to the database")
    chunks.add_text(
        source_path=url,
        source_text=text
    )
    print(f"Summarizing {url} chunks")
    summary = chunks.summarize_entry(url)
    print(f"Saving summary to DB")
    sources.add_object({
        "source_path": url,
        "body": summary
    })
    return True


def get_object_count(url: str) -> bool:
    """
    When a url is received, add it to the list of sources in COLLECTION_NAME_SOURCES class
    :param url:
    :return:
    """
    # Check if the resource is in a valid format
    obj_count = sources.get_entry_count(value=url)
    if obj_count > 0:
        return True
    else:
        return False


class URLSubmission(BaseModel):
    url: str


class Question(BaseModel):
    url: str
    question: str


class TextSubmission(BaseModel):
    source: str
    text: str


@app.get("/")
def root():
    obj_count = chunks.get_total_obj_count()
    return {"message": f"Why, hello there - you have {obj_count} objects."}


@app.post("/add_pdf/")
def submit_pdf(submission: URLSubmission, background_tasks: BackgroundTasks):
    status = get_object_count(submission.url)
    if status:
        return {"status": f"{submission.url} is already in the database!"}
    else:
        background_tasks.add_task(add_pdf_to_kb, submission.url)
        return {"status": "URL processing started"}


@app.post("/add_text/")
async def submit_text(submission: TextSubmission, background_tasks: BackgroundTasks):
    status = get_object_count(submission.source)
    if status:
        return {"status": f"{submission.source} is already in the database!"}
    else:
        background_tasks.add_task(add_text_to_kb, submission.text, submission.source)
        return {"status": "Text processing started"}


# @app.post("/ask/")
# async def ask_about_content(query: Question):
#     search_string = db.get_search_string_from_question(query.question)
#     answer, paragraphs = db.ask_object(
#         client=client, source_path=query.url,
#         search_string=search_string, question=query.question
#     )
#     return {"answer": answer, "source_paragraphs": paragraphs}


@app.post("/summarise/")
def get_summary(submission: URLSubmission):
    response = (
        sources.client.query.get(sources.target_class, sources.get_all_property_names())
        .with_where(db.source_filter(submission.url))
        .with_limit(1)
        .do()
    )
    summary = response["data"]["Get"][sources.target_class][0][db.COLLECTION_BODY_PROPERTY]
    return {"summary": summary}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
