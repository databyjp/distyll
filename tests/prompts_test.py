import pytest
import prompts


@pytest.mark.parametrize(
    "source_text",
    [
        "This is a source text",
    ]
)
def test_summarize(source_text):
    prompt = prompts.summarize(source_text)
    assert source_text in prompt
    assert prompts.PROMPTS.summarize in prompt
