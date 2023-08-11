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

resource_url = st.text_input("Enter a YouTube video URL or PDF url that you're interested in!")

if len(resource_url) > 0:
    if collection.check_if_obj_present(resource_url):
        st.text("Object present!")
        if resource_url.endswith(".pdf"):
            pass
        else:
            video_title = collection.get_obj_sample(resource_url)
            st.write(f"{video_title['source_title']} found")

        # Show resource summary
        with st.expander("See the summary", expanded=False):
            st.write(
                collection.summarize_entry(resource_url)
            )

        # Talk to it
        question = st.text_input("Ask it something!")
        if len(question) > 0:
            st.write(
                collection.ask_object(resource_url, question)
            )

        # Delete it?
        if st.button(f"Delete {resource_url} from DB"):
            st.write(f"Deleting {resource_url} from the database... please wait...")
            collection.remote_from_db(resource_url)
            st.experimental_rerun()
    else:
        st.text("Object not present")
        if st.button("Add it to DB?"):
            st.write(f"Adding {resource_url} to the database... please wait...")

            if resource_url.endswith(".pdf"):
                collection.add_pdf(resource_url)
            else:
                collection.add_from_youtube(resource_url)
            st.experimental_rerun()


