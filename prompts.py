from dataclasses import dataclass


@dataclass
class PROMPTS:
    summarize: str = """
    Using plain language, summarize the following as a whole. 
    It should be paragraph or two of text at maximum but does not need to be that long.
    If it would be useful, list the topics it covers, and key points.

    ===== SOURCE TEXT =====\n\n
    """


def summarize(source_text: str):
    task_prompt = PROMPTS.summarize + source_text + '\n\n'
    return task_prompt
