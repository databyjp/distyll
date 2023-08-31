from typing import Union, List

import openai
import os
from dataclasses import dataclass

from weaviate import Client
from weaviate.util import generate_uuid5

import preprocessing
import prompts

openai.api_key = os.environ["OPENAI_APIKEY"]

MAX_CONTEXT_SIZE = 7500  # Max context size in characters
MAX_N_CHUNKS = preprocessing.MAX_CHUNK_CHARS   # Max number of chunks to grab in a set of results


@dataclass
class PROMPTS:
    RAG_PREAMBLE = "Using only the included data here, answer the following question:"
    SUMMARIZE = f"""
        Using plain language, summarize the following as a whole into a paragraph or two of text.
        List the topics it covers, and what the reader might learn from it.
        """


class RAGBase:
    def __init__(
            self,
            source_text: Union[List[str], str],
            use_expensive_model: bool = False,
            # weaviate_client: Union[Client, None] = None,
            # weaviate_propety_name: str = None,
    ):
        if type(source_text) == list:
            self.source_text = ' '.join(source_text)
        else:
            self.source_text = source_text
        # TODO - add ability to save results to Weaviate
        # self.weaviate_client = weaviate_client
        # self.property_name = weaviate_propety_name
        # self.uuid = generate_uuid5(self.source_text)
        self.use_expensive_model = use_expensive_model

    def generate(self, prompt):
        """
        Get object from DB in case the result has been saved,
        and if not, perform the generative task & add to the database
        :param collection_name:
        :param prompt:
        :return:
        """
        generated_text = call_llm(
            prompt=prompt,
            use_expensive_model=self.use_expensive_model
        )
        return generated_text

    def summarize_short(self):
        prompt = prompts.summarize(self.source_text)
        return self.generate(prompt)

    def summarize(self):
        if len(self.source_text) > MAX_CONTEXT_SIZE:
            n_chunks = int(len(self.source_text) // MAX_CONTEXT_SIZE) + 1
            chunk_size = int(len(self.source_text) // n_chunks) + 1
            chunks = [
                self.source_text[chunk_size * i: chunk_size * (i+1) + int(chunk_size * 0.2)]
                for i in range(n_chunks)
            ]
            summaries = list()
            for chunk in chunks:
                rag_base_chunk = RAGBase(chunk)
                summary = rag_base_chunk.summarize_short()
                summaries.append(summary)
            rag_base_summaries = RAGBase(summaries)
            return rag_base_summaries.summarize()
        else:
            prompt = prompts.summarize(self.source_text)
            return self.generate(prompt)


def get_summary(source_text: str):
    rag_base = RAGBase(source_text)
    prompt = prompts.summarize(source_text)
    return rag_base.generate(prompt)


def summarize_multiple_paragraphs(paragraphs: List) -> Union[str, List]:
    """
    Helper function for summarizing multiple paragraphs using an LLM
    :param paragraphs:
    :return:
    """
    paragraph_count = len(paragraphs)
    if paragraph_count < MAX_N_CHUNKS:
        print(f"Summarizing {paragraph_count} paragraphs")
        source_data = RAGBase(paragraphs)
        return source_data.summarize()
    else:
        print(f"{paragraph_count} paragraphs is too many - let's split them up")
        summary_sets = (paragraph_count // MAX_N_CHUNKS) + 1
        subsets = [
            paragraphs[MAX_N_CHUNKS*i:MAX_N_CHUNKS*(i+1)] for i in range(summary_sets)
        ]
        summaries = list()
        for i, subset in enumerate(subsets):
            print(f"Summarizing set {i} of {len(subsets)}")
            source_data = RAGBase(paragraphs)
            summaries.append(source_data.summarize())
        return summarize_multiple_paragraphs(summaries)


def call_llm(prompt: str, use_expensive_model: bool = False) -> str:
    return call_chatgpt(prompt, use_expensive_model)


def call_chatgpt(prompt: str, use_gpt_4: bool = False) -> str:
    """
    Call ChatGPT for all your LLM needs
    :param prompt:
    :param use_gpt_4:
    :return:
    """
    if use_gpt_4 is False:
        model_name = "gpt-3.5-turbo"
    else:
        model_name = "gpt-4"

    completion = openai.ChatCompletion.create(
        model=model_name,
        messages=[
            {"role": "system",
             "content": """
                You are a helpful, intelligent, thoughtful assistant who is a great communicator.

                You can communicate complex ideas and concepts in clear, concise language 
                without resorting to domain-specific jargon unless it is entirely necessary.

                When you do are not sure of what the answer should be, or whether it is grounded in fact,
                you communicate this to the user to help them make informed decisions
                about how much to trust your outputs. 
                """
             },
            {"role": "user", "content": prompt}
        ]
    )
    return completion.choices[0].message["content"]
