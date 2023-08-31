import pytest
import rag


# @pytest.mark.parametrize(
#     "source_text",
#     [
#         ("What is love?"),
#         (["What is love?", "Baby don't hurt me."]),
#     ]
# )
# def test_ragbase_init(source_text):
#     rag_base = rag.RAGBase(source_text)
#     if type(source_text) == list:
#         for t in source_text:
#             assert t in rag_base.source_text
#     else:
#         assert source_text in rag_base.source_text
#
#
# @pytest.mark.parametrize(
#     "prompt, answer",
#     [
#         ("What is 2 + 2?", "4"),
#         ("What is the capital of France?", "Paris"),
#     ]
# )
# def test_ragbase_generate(prompt, answer):
#     rag_base = rag.RAGBase(prompt)
#     generated_text = rag_base.generate(prompt)
#     assert answer.lower() in generated_text.lower()


@pytest.mark.parametrize(
    "source_text, answer",
    [
        ("Paris is the capital of France", "Paris"),
    ]
)
def test_ragbase_generate(source_text, answer):
    rag_base = rag.RAGBase(source_text)
    generated_text = rag_base.summarize()
    assert answer.lower() in generated_text.lower()


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
