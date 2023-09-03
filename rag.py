from typing import Union, List
import openai
import os
from dataclasses import dataclass
import preprocessing
from weaviate import Client
from weaviate.util import generate_uuid5
import logging

logger = logging.getLogger(__name__)

openai.api_key = os.environ["OPENAI_APIKEY"]

MAX_CONTEXT_SIZE = 5000  # Max context size in characters
MAX_N_CHUNKS = int(MAX_CONTEXT_SIZE / preprocessing.MAX_CHUNK_CHARS)   # Max number of chunks to grab in a set of results
summary_size = int(MAX_CONTEXT_SIZE * 0.1)


@dataclass
class PROMPTS:
    summarize: str = f"""
    Using plain language, summarize the following as a whole. 
    It should be around a paragraph or shorter.
    If it would be useful, list the topics it covers, and key points.

    ===== SOURCE TEXT =====\n\n
    """


def get_summarize_prompt(source_text: str):
    task_prompt = PROMPTS.summarize + source_text + '\n\n'
    return task_prompt


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
        :param prompt:
        :return:
        """
        generated_text = call_llm(
            prompt=prompt,
            use_expensive_model=self.use_expensive_model
        )
        return generated_text

    def summarize_short(self):
        prompt = get_summarize_prompt(self.source_text)
        return self.generate(prompt)

    def summarize(self, max_context_size: int = MAX_CONTEXT_SIZE):
        """
        Recursively summarise text to be a size that is less than a maximum allowable size
        :param max_context_size:
        :return:
        """
        text_length = len(self.source_text)
        logger.info(f"Recursively summarizing {text_length}")
        if text_length <= max_context_size:
            # Summarize the text as is with an LLM
            logger.info(f"Summarizing {text_length}")
            llm_summary = self.summarize_short()
            logger.info(f"Summarized to {len(llm_summary)}")
            return llm_summary
        else:
            logger.info(f"{text_length} is too long")
            # Further split the data and summarize each chunk
            n_chunks = min(5, (text_length // max_context_size) + 1)
            chunk_size = int(text_length // n_chunks) + 1
            logger.info(f"Chunking into {n_chunks} chunks")
            chunks = [
                self.source_text[chunk_size * i: chunk_size * (i + 1) + int(chunk_size * 0.05)]
                for i in range(n_chunks)
            ]
            summaries = list()
            for chunk in chunks:
                summarized_chunk = RAGBase(chunk).summarize()
                summaries.append(summarized_chunk)
            combined_summary = ' '.join(summaries)
            logger.info(f"Combined summary {len(combined_summary)}")
            return RAGBase(combined_summary).summarize()


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
