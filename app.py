import distyll  # My personal demo project
import media
import streamlit as st

import rag

st.set_page_config(layout="wide")


# @st.cache
def get_youtube_title(video_url):
    return media.get_youtube_title(video_url)


img_col, _, _ = st.columns([10, 30, 30])
with img_col:
    st.image("media/weaviate-logo-dark-name-transparent-for-light.png")
st.header("Better living through RAG")

video_options = [
    "https://youtu.be/-ebMbqkdQdg",  # Margot Robbie interview
    "https://youtu.be/5p248yoa3oE",  # Andrew Ng interview
    "https://youtu.be/LkV5DTRNxAg",  # Connor Gorilla video
    "https://youtu.be/nMMNkfSQuiU",  # Starfield (video game) review - from a week ago
    "https://youtu.be/enRb6fp5_hw",  # Stanford: NLU Information Retrieval: Guiding Ideas Spring 2023
]


video_title_dict = {
    video_id: get_youtube_title(video_id) for video_id in video_options
}
title_video_dict = {
    v: k for k, v in video_title_dict.items()
}

background, info, tab1, tab2 = st.tabs(["Background", "Source data", "Demo", "Behind the magic"])
with background:
    with st.expander("Problem statement"):
        st.markdown("### There is way too much content out there")
        st.markdown("*****")
    with st.expander("Solution"):
        st.markdown("### What if you didn't *have* to watch a video for its information?")
        st.markdown("- Get a video summary.\n- Ask the video whatever you want.")
        st.markdown("*****")


with info:
    # st.markdown("Ask the video whatever you want.")
    st.subheader("Available videos:")
    a, b, c = st.columns(3)
    columns = [a, b, c]

    for i, video in enumerate(video_options):
        col_index = (i % 3)
        with columns[col_index]:
            st.video(data=video)
            title = video_title_dict[video]
            st.write(title)

with tab1:
    st.subheader("Talk to a video")

    video_selection = st.selectbox("Select a video",
        options=title_video_dict.keys()
    )
    youtube_url = title_video_dict[video_selection]

    import weaviate
    import os

    client = weaviate.Client(
        url='https://lhy7sdrnq0uvf7tgjrgnkw.c0.europe-west2.gcp.weaviate.cloud',  # Demo cluster
        auth_client_secret=weaviate.AuthApiKey('PBZfT3JD11pBGHGp8lX7ALXE3nXvKh15weIy'),  # Read-only key
        additional_headers={
            'X-OpenAI-Api-Key': os.environ['OPENAI_APIKEY']
        }
    )
    db = distyll.DBConnection(client=client)
    db.set_apikey(openai_key=os.environ["OPENAI_APIKEY"])


    def get_summary(source_path):
        summary_out = db.query_summary(
            prompt="""
            In two to three short, succinct points, summarize this material in plain language. 
            Keep each point short and to the point. Each point should be just a few words.
            """,
            object_path=source_path
        )
        return summary_out


    # st.subheader("Get a summary")
    st.markdown("#### Get a summary")
    with st.expander("What is this about?"):
        summary = get_summary(youtube_url)
        st.write(summary.generated_text)

    st.markdown("#### Extract anything from this video")

    user_question = st.text_input("Ask the video anything!")

with tab2:
    st.subheader("How does it all work?")


if len(user_question) > 3:
    with tab1:
        with st.expander("Raw data used:"):
            where_filter = {
                "path": ["source_path"],
                "operator": "Equal",
                "valueText": youtube_url
            }
            raw_response = (
                db.client.query
                .get(db.chunk_class, db.chunk_properties)
                .with_where(where_filter)
                .with_near_text({'concepts': [user_question]})
                .with_limit(rag.MAX_N_CHUNKS)
                .do()
            )
            for resp_obj in raw_response["data"]["Get"][db.chunk_class]:
                st.write(resp_obj)

        response = db.query_chunks(
            prompt=f"""
            Answer the question: {user_question}.
            Feel free to use the text contained here.
            The answer should be well-written, succinct and thoughtful, using plain language even if the source material is technical.
            If there is no information, say 'The source material does not say.'.
            """,
            object_path=youtube_url,
            search_query=user_question
        )
        st.write(response.generated_text)

    with tab2:
        # with st.expander("Raw data used:"):
        #     for resp_obj in response.objects:
        #         st.write(resp_obj)
        with st.expander("Code snippet:"):
            st.code(
                """
            where_filter = {
                "path": ["source_path"],
                "operator": "Equal",
                "valueText": object_path
            }
            response = (
                client.query
                .get(class_name, class_properties)
                .with_where(where_filter)
                .with_near_text({'concepts': [search_query]})
                .with_generate(grouped_task=prompt)
                .with_limit(limit)
                .do()
            )            
                """,
                language="python"
            )
else:
    with tab2:
        st.write('Run a search first and come back! :)')
