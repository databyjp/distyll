import preprocessing
import pytest


@pytest.mark.parametrize(
    "source_text, max_chunk_words, overlap_fraction, expected_length, expected_first_chunk, expected_last_chunk",
    [
        ("Why hello there", 2, 0, 2, "Why hello", "there"),
        (" ".join(["A"] * 10), 3, 0, 4, "A A A", "A"),
        (" ".join(["A"] * 10), 3, 0.25, 4, "A A A", "A"),
        (" ".join(["A"] * 10), 3, 0.5, 4, "A A A", "A A"),
        (" ".join(["A"] * 10) + " ", 3, 0.5, 4, "A A A", "A A"),
    ]
)
def test_chunk_text_by_num_words(
        source_text, max_chunk_words, overlap_fraction,
        expected_length, expected_first_chunk, expected_last_chunk
):
    chunked_text = preprocessing.chunk_text_by_num_words(source_text, max_chunk_words, overlap_fraction)
    assert len(chunked_text) == expected_length
    assert chunked_text[0] == expected_first_chunk
    assert chunked_text[-1] == expected_last_chunk


@pytest.mark.parametrize(
    "source_text, max_chunk_chars, overlap_fraction, expected_length, expected_first_chunk, expected_last_chunk",
    [
        ("123 456 789 0", 4, 0, 4, "123 ", "0"),
        ("123 456 789 0", 4, 0.1, 4, "123 ", "0"),
        ("123 456 789 0", 4, 0.25, 4, "123 ", " 0"),
        (" 123 456 789 0 ", 4, 0.25, 4, "123 ", " 0"),
    ]
)
def test_chunk_text_by_num_chars(
        source_text, max_chunk_chars, overlap_fraction,
        expected_length, expected_first_chunk, expected_last_chunk
):
    chunked_text = preprocessing.chunk_text_by_num_chars(source_text, max_chunk_chars, overlap_fraction)
    assert len(chunked_text) == expected_length
    assert chunked_text[0] == expected_first_chunk
    assert chunked_text[-1] == expected_last_chunk


@pytest.mark.parametrize(
    "source_text, expected_text",
    [
        ("1  1", "1 1"),
        ("1\n \n \n1", "1 1"),
    ]
)
def test_remove_multiple_whitespaces(source_text, expected_text):
    processed_text = preprocessing.remove_multiple_whitespaces(source_text)
    assert processed_text == expected_text
