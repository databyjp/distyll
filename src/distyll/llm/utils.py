from distyll.utils import get_openai_client, chunk_text
from typing import Dict, Union


def ask_openai(
    prompt: str, system_prompt: Dict[str, str] = None, model: Union[str, None] = None
) -> str:
    """
    Ask OpenAI for a response to a prompt
    Args:
        prompt:
        system_prompt:
        model:

    Returns:
        str: Response from OpenAI
    """
    if not system_prompt:
        system_prompt = {"role": "system", "content": "You are a helpful assistant."}
    oai_client = get_openai_client()

    if not model:
        model = "gpt-4o"

    completion = oai_client.chat.completions.create(
        model=model, messages=[system_prompt, {"role": "user", "content": prompt}]
    )

    return completion.choices[0].message.content


def summarize_text(
    text: str,
    max_chunk_len: int = 1000,
    overlap: float = 0.1,
    summary_prompt: Dict[str, str] = None,
    number_of_points: int = 3,
) -> str:
    """
    Recursively summarise a text by chunking it into smaller parts and summarising each part
    Args:
        text:
        max_chunk_len:
        overlap:
        summary_prompt:

    Returns:
        str: Summarised text
    """

    chunks = chunk_text(
        text, method="words", token_length=max_chunk_len, overlap_fraction=overlap
    )

    if len(chunks) == 1:
        if not summary_prompt:
            summary_prompt = {
                "role": "system",
                "content": f"Summarize the provided text into a maximum of {number_of_points} short key points for the user. ",
            }
        return ask_openai(chunks[0], summary_prompt)
    else:
        chunk_summary_prompt = {
            "role": "system",
            "content": "Summarize the provided text into a succinct set of key points. ",
        }
        summaries = [
            summarize_text(chunk, summary_prompt=chunk_summary_prompt)
            for chunk in chunks
        ]
        return " ".join(summaries)
