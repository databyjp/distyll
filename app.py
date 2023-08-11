import streamlit as st
import wkb
import os
import weaviate

local_client = weaviate.Client("http://localhost:8099")

client = wkb.start_db(custom_client=local_client)

collection = wkb.Collection(client, wkb.WV_CLASS, user_agent=f'My Project ({os.getenv("MY_EMAIL")})')
collection.set_apikey(openai_key=os.getenv("OPENAI_APIKEY"))  # The class is configured to use `text2vec-openai` and `generative-openai`

st.title("KnowledgeBuddy")
resource_selection = st.selectbox(
    'Available resources in DB:',
    collection.get_unique_paths())

st.write('You selected:', resource_selection)

youtube_url = st.text_input("Enter a YouTube video URL that you're interested in!")

if len(youtube_url) > 0:
    if collection.check_if_obj_present(youtube_url):
        st.text("Object present!")
        video_title = collection.get_obj_sample(youtube_url)
        st.write(f"{video_title['source_title']} found")
        with st.expander("See the summary", expanded=False):
            st.write(
                collection.summarize_entry(youtube_url)
            )
        question = st.text_input("Ask the video something!")
        if len(question) > 0:
            st.write(
                collection.ask_object(youtube_url, question)
            )
    else:
        st.text("Object not present")
        if st.button("Add it to DB?"):
            st.write(f"Adding {youtube_url} to the database... please wait...")
            collection.add_from_youtube(youtube_url)
            st.experimental_rerun()

