from enum import Enum
from typing import Union, Any, Dict, List
import openai
import os
from weaviate import Client
from weaviate.util import generate_uuid5

openai.api_key = os.environ["OPENAI_APIKEY"]
OUTPUT_PROPERTY_NAME = 'body'


class CollectionNames(Enum):
    """
    Each RAG task will have a collection name associated with it.
    Retrieve it from this class
    """
    REVISION_QUIZ = 'RevisionQuiz'
    GLOSSARY = 'Glossary'
    SUMMARY = 'Summary'


class PromptGenerator:
    def __init__(self, source_text: Union[List[str], str], optional_prompt: str = ""):
        """
        Initialize prompt generation
        :param source_text:
        :param optional_prompt:
        """
        if type(source_text) == List[str]:
            self.source_text = ' '.join(source_text)
        else:
            self.source_text = source_text
        self.optional_prompt = optional_prompt

    def for_revision_quiz(self):
        task_prompt = f"""
        Write a set of multiple-choice quiz questions with three to four options each to review and internalise the following information.
        The quiz should be written into Markdown so that it can be displayed and undertaken by the user.

        The goal of the quiz is to provide a revision exercise, 
        so that the user can internalise the information presented in this passage.
        The quiz questions should only cover information explicitly presented in this passage. 
        The number of questions can be anything from one to 10, depending on the volume of information presented. 

        The output should be in markdown format, and should not include answers. It might look like:

        QUESTION TEXT
        - [ ] Option 1
        - [ ] Option 2
        - [ ] Option 3
        ...

        ======= Source Text =======

        {self.source_text}

        ======= Questions =======

        """
        return task_prompt

    def for_glossary(self):
        task_prompt = f"""
        Return a glossary of key terms or jargon from the source text
        to help someone reading this material understand the text.
        Each explanation should be in as plain and clear language as possible.
        For this task, it is acceptable to rely on information outside of the source text.

        The output should be in the following Markdown format:

        - **TERM A**: EXPLANATION A 
        - **TERM B**: EXPLANATION B
        - ...

        ====== Source text =======

        {self.source_text}

        ====== Glossary =======

        """
        return task_prompt

    def for_search_string(self):
        task_prompt = f"""
        What would be a suitable semantic search query string that would 
        return relevant data to help answer the following question? 
        Remember that semantic search works by comparing the semantic meaning 
        of one object against another.
        Provide the search string only, and nothing else.
         
        The question is: ==========
         
        {self.source_text}
        """
        return task_prompt

    def for_summarisation(self):
        task_prompt = f"""
        Using plain language, summarize the following as a whole into a paragraph or two of text.
        List the topics it covers, and what the reader might learn from it.
        
        ===== SOURCE TEXT =====
        
        {self.source_text}
        
        """
        return task_prompt

    def for_question_answer(self):
        task_prompt = f"""
        Answer this question based on the following text. The question is:
        
        ===== QUESTION ======
        
        {self.optional_prompt}
         
        ===== END QUESTION ======
        
        If the text does not contain required information,
        do not answer the question, and indicate as such to the user.
        
        ===== SOURCE TEXT ======
        
        {self.source_text}
         
        ===== END SOURCE TEXT ======
        
        ===== ANSWER: ======
        
        """
        return task_prompt


class RetrievedData:
    def __init__(
            self,
            source_text: Union[List[str], str],
            weaviate_client: Union[Client, None],
            use_gpt_4: bool = False
    ):
        if type(source_text) == List[str]:
            self.source_text = ' '.join(source_text)
        else:
            self.source_text = source_text
        self.use_gpt_4 = use_gpt_4
        self.prompts = PromptGenerator(source_text)
        self.weaviate_client = weaviate_client
        self.property_name = OUTPUT_PROPERTY_NAME
        self.uuid = generate_uuid5(self.source_text)

    def add_to_db(self, collection_name: str, data_object: Dict[str, str]):
        """
        Add output to DB.
        :param collection_name:
        :param data_object:
        :return:
        """

        self.weaviate_client.data_object.create(
            data_object=data_object,
            class_name=collection_name,
            uuid=self.uuid
        )
        return self.weaviate_client.data_object.get(uuid=self.uuid, class_name=collection_name)

    def get_from_db(self, collection_name: str):
        """
        Get object from DB in case the result has been saved
        :param collection_name:
        :return:
        """
        fetched_object = self.weaviate_client.data_object.get(uuid=self.uuid, class_name=collection_name)
        if fetched_object:
            return fetched_object
        else:
            return None

    def get_or_generate(self, collection_name, prompt):
        """
        Get object from DB in case the result has been saved,
        and if not, perform the generative task & add to the database
        :param collection_name:
        :param prompt:
        :return:
        """
        current_object = self.get_from_db(collection_name=collection_name)

        if current_object is not None:
            return current_object
        else:
            body = call_chatgpt(
                prompt=prompt,
                use_gpt_4=self.use_gpt_4
            )
            data_object = {self.property_name: body}
            return self.add_to_db(collection_name=collection_name, data_object=data_object)

    def get_revision_quiz(self):
        collection_name = CollectionNames.REVISION_QUIZ.value
        return self.get_or_generate(
            collection_name=collection_name,
            prompt=self.prompts.for_revision_quiz(),
        )

    def get_glossary(self):
        collection_name = CollectionNames.GLOSSARY.value
        return self.get_or_generate(
            collection_name=collection_name,
            prompt=self.prompts.for_glossary(),
        )

    def summarize(self):
        collection_name = CollectionNames.SUMMARY.value
        return self.get_or_generate(
            collection_name=collection_name,
            prompt=self.prompts.for_summarisation(),
        )

    def delete_existing(self, collection_name) -> Union[bool, None]:
        if self.weaviate_client.data_object.exists(class_name=collection_name, uuid=self.uuid):
            self.weaviate_client.data_object.delete(
                class_name=collection_name,
                uuid=self.uuid
            )
            return True
        else:
            return None


def call_chatgpt(prompt: str, use_gpt_4: bool = False) -> str:
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
