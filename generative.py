from enum import Enum


class Prompts(Enum):
    SUMMARIZE = 10
    SUMMARIZE_WITH_CONTEXT = 20
    SUB_TOPICS = 40


def get_prompt(name: Prompts, custom_parameter: str = "") -> str:
    if name == Prompts.SUMMARIZE.name:
        prompt = f"""
            Using plain language, summarize the following as a whole into a paragraph or two of text.
            List the topics it covers, and what the reader might learn by listening to it. 
            """
    elif name == Prompts.SUMMARIZE_WITH_CONTEXT.name:
        prompt = f"""
            Based on the following text, summarize any information relating to {custom_parameter} concisely.
            If the text does not contain required information, 
            do not answer the question, and indicate as such to the user.
            """
    elif name == Prompts.SUB_TOPICS.name:
        prompt = f"""
            If the following text does includes information about {custom_parameter},
            extract a list of three to six related sub-topics
            related to {custom_parameter} that the user might learn about.
            Deliver the topics as a short list, each separated by two consecutive newlines like `\n\n`
    
            If the following information does not includes information about {custom_parameter},
            tell the user that not enough information could not be found.
            """
    else:
        ValueError("Error, input not understood")
        return ""

    return prompt
