from typing import List

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import wkb

client = wkb.start_db()
chunks = wkb.Collection(client, wkb.COLLECTION_NAME_CHUNKS)
sources = wkb.Collection(client, wkb.COLLECTION_NAME_SOURCES)

app = FastAPI(title="distyll.info", description="Effortlessly distill content into its essence.", version="0.1")


# Mocking the Weaviate interaction function
def add_to_weaviate(url: str) -> str:
    """
    When a url is received, add it to the list of sources in COLLECTION_NAME_SOURCES class
    :param url:
    :return:
    """
    if url.endswith(".pdf"):
        chunks.add_pdf(url)
        return "pending"
    else:  # When URL type not supported
        return "not supported"


def mock_summary(url: str) -> str:
    return f"Mocked summary for the content at {url}."


class URLSubmission(BaseModel):
    url: str


class Question(BaseModel):
    url: str
    question: str


@app.get("/")
async def root():
    obj_count = chunks.get_total_obj_count()
    return {"message": f"Why, hello there - you have {obj_count} objects."}


@app.post("/submit/")
async def submit_url(submission: URLSubmission, background_tasks: BackgroundTasks):
    status = add_to_weaviate(submission.url)
    if status == "pending":
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
async def summarise_content(submission: URLSubmission):
    summary = chunks.summarize_entry(submission.url)
    # TODO: Save summaries of long content to Weaviate so that they can be re-used
    return {"summary": summary}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)



# from fastapi import FastAPI
# import wkb

# client = wkb.start_db()
# collection = wkb.Collection(client, wkb.WV_CLASS)
# app = FastAPI()


# @app.get("/")
# async def root():
#     obj_count = chunks.get_total_obj_count()
#     return {"message": f"Why, hello there - you have {obj_count} objects."}


# @app.get("/sample")
# def get_sample_objs():
#     sample_objs = chunks.get_sample_objs()
#     return sample_objs


# @app.get("/objects_by_path/{source_path}")  # e.g. http://127.0.0.1:8000/objects_by_path/xk28RMhRy1U
# def get_all_objs_by_path(source_path):
#     print(source_path)
#     sample_objs = chunks.get_all_objs_by_path(source_path)
#     return sample_objs


# @app.get("/ask_object/{source_path}")  # e.g. http://127.0.0.1:8000/ask_object/xk28RMhRy1U?question=what%20is%20multi%20tenancy
# def ask_object(source_path, question):
#     return {
#         "answer": chunks.ask_object(source_path, question),
#         "object": source_path
#     }


# @app.get("/ask")  # e.g. http://127.0.0.1:8000/ask?question=what%20is%20autocut%20in%20weaviate
# def ask_object(question):
#     return {
#         "answer": chunks.summarize_topic(question),
#     }
