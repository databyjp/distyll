import pytest
import rag
from unittest.mock import patch


@pytest.mark.parametrize(
    "source_text",
    [
        ("What is love?"),
        (["What is love?", "Baby don't hurt me."]),
    ]
)
def test_ragbase_init(source_text):
    rag_base = rag.RAGBase(source_text)
    if type(source_text) == list:
        for t in source_text:
            assert t in rag_base.source_text
    else:
        assert source_text in rag_base.source_text


@pytest.mark.parametrize(
    "prompt, answer",
    [
        ("What is 2 + 2?", "4"),
        ("What is the capital of France?", "Paris"),
    ]
)
def test_ragbase_generate(prompt, answer):
    rag_base = rag.RAGBase(prompt)
    generated_text = rag_base.generate(prompt)
    assert answer.lower() in generated_text.lower()


@pytest.mark.parametrize(
    "source_text, answer",
    [
        ("Paris is the capital of France", "Paris"),
    ]
)
def test_ragbase_summarize_short(source_text, answer):
    rag_base = rag.RAGBase(source_text)
    generated_text = rag_base.summarize()
    assert answer.lower() in generated_text.lower()


# Test when source_text length is below MAX_CONTEXT_SIZE
def test_ragbase_summarize_short_patched():
    with patch.object(rag.RAGBase, 'summarize_short', return_value='short_summary'):
        rag_base = rag.RAGBase("This is a short text")
        summary = rag_base.summarize()
        assert summary == 'short_summary'


# Test when the source_text length is greater than MAX_CONTEXT_SIZE
def test_summarize_test_ragbase_summarize_recursion():
    with patch.object(rag.RAGBase, 'summarize_short', return_value='chunk_summary') as mock_summarize_short:
        text = "a" * (rag.MAX_CONTEXT_SIZE * 2 + 1)  # Make sure this is greater than MAX_CONTEXT_SIZE
        rag_base = rag.RAGBase(text)
        summary = rag_base.summarize()

        # Check that summarize_short was called more than once,
        # which indicates that the text was chunked and summarized.
        assert mock_summarize_short.call_count == 4

        # MAYBE DO - Check that the final summary is as expected.


@pytest.mark.parametrize(
    "prompt, answer",
    [
        ("What is 2 + 2?", "4"),
        ("What is the capital of France?", "Paris"),
    ]
)
def test_call_chatgpt(prompt, answer):
    generated_text = rag.call_chatgpt(prompt)
    assert answer.lower() in generated_text.lower()


@pytest.mark.parametrize(
    "prompt, answer",
    [
        ("What is 2 + 2?", "4"),
        ("What is the capital of France?", "Paris"),
    ]
)
def test_call_llm(prompt, answer):
    generated_text = rag.call_llm(prompt)
    assert answer.lower() in generated_text.lower()
