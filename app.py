import distyll  # My personal demo project
import media
import streamlit as st

import rag

st.set_page_config(layout="wide")

img_col, _, _ = st.columns([10, 30, 30])
with img_col:
    st.image("media/weaviate-logo-dark-name-transparent-for-light.png")
st.header("Better living with RAG")

video_options = [
    "https://youtu.be/-ebMbqkdQdg",  # Margot Robbie interview
    "https://youtu.be/5p248yoa3oE",  # Andrew Ng interview
    "https://youtu.be/LkV5DTRNxAg",  # Connor Gorilla video
    "https://youtu.be/nMMNkfSQuiU",  # Starfield (video game) review - from a week ago
    "https://youtu.be/enRb6fp5_hw",  # Stanford: NLU Information Retrieval: Guiding Ideas Spring 2023
]

video_title_dict = {
    video_id: media.get_youtube_title(video_id) for video_id in video_options
}
title_video_dict = {
    v: k for k, v in video_title_dict.items()
}

info, tab1, tab2 = st.tabs(["Source data", "Demo", "Behind the magic"])
with info:
    st.markdown("### What if you didn't *have* to watch a video for its information?")
    st.markdown("- Get a video summary.\n- Ask the video whatever you want.")
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

    # for i, col in enumerate([a, b, c]):
    #     with col:
    #         st.video(data=video_options[i])

    # for k, v in title_video_dict.items():
    #     st.markdown(f"- {k} [link]({v})")
    # for i, col in enumerate([a, b, c]):
    #     with col:
    #         title = video_title_dict[video_options[i]]
    #         st.write(title)

with tab1:
    st.subheader("Talk to a video")

    video_selection = st.selectbox("Select a video",
        options=title_video_dict.keys()
    )
    youtube_url = title_video_dict[video_selection]

    import weaviate
    import os

    client = weaviate.Client(
        url=os.environ['JP_WCS_URL'],
        auth_client_secret=weaviate.AuthApiKey(os.environ['JP_WCS_ADMIN_KEY']),
        additional_headers={
            'X-OpenAI-Api-Key': os.environ['OPENAI_APIKEY']
        }
    )
    db = distyll.DBConnection(client=client)


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

    # suggested_query = rag.call_llm(prompt=f"""
    # Return a search string that we could use to search for text that relates to {user_question}.
    # Do not return any explanations. Only return the suggested search string itself.
    #
    # ======
    #
    # Search string:
    # """)

with tab2:
    st.subheader("How does it all work?")


if len(user_question) > 3:
    with tab1:
        # with st.expander("Query string used"):
        #     st.write(suggested_query)
        response = db.query_chunks(
            # prompt=f"""
            # If this text contains information about {user_question}, answer the question: {user_question}.
            # The answer should be well-written, succinct and thoughtful, using plain language even if the source material is technical.
            # If there is no information, say 'The source material does not say.'.
            # """,
            prompt=f"""
            Answer the question: {user_question}.
            Feel free to use the text contained here.
            The answer should be well-written, succinct and thoughtful, using plain language even if the source material is technical.
            If there is no information, say 'The source material does not say.'.
            """,
            object_path=youtube_url,
            search_query=user_question
            # search_query=suggested_query
        )
        st.write(response.generated_text)

    with tab2:
        with st.expander("Raw data used:"):
            for resp_obj in response.objects:
                st.write(resp_obj)
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
                .with_sort({
                    'path': ['chunk_number'],
                    'order': 'asc'
                })
                .do()
            )            
                """,
                language="python"
            )
else:
    with tab2:
        st.write('Run a search first and come back! :)')
