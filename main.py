from fastapi import FastAPI
import wkb

client = wkb.start_db()
collection = wkb.Collection(client, wkb.WV_CLASS)
app = FastAPI()


@app.get("/")
async def root():
    obj_count = collection.get_total_obj_count()
    return {"message": f"Why, hello there - you have {obj_count} objects."}


@app.get("/sample")
def get_sample_objs():
    sample_objs = collection.get_sample_objs()
    return sample_objs


@app.get("/objects_by_path/{source_path}")  # e.g. http://127.0.0.1:8000/objects_by_path/xk28RMhRy1U
def get_all_objs_by_path(source_path):
    print(source_path)
    sample_objs = collection.get_all_objs_by_path(source_path)
    return sample_objs


@app.get("/ask_object/{source_path}")  # e.g. http://127.0.0.1:8000/ask_object/xk28RMhRy1U?question=what%20is%20multi%20tenancy
def ask_object(source_path, question):
    return {
        "answer": collection.ask_object(source_path, question),
        "object": source_path
    }


@app.get("/ask")  # e.g. http://127.0.0.1:8000/ask?question=what%20is%20autocut%20in%20weaviate
def ask_object(question):
    return {
        "answer": collection.summarize_topic(question),
    }
