import pytest
import main
import utils


@pytest.mark.parametrize(
    ("length", "expected_output"),
    [
        (0, 1),
        (100, 1),
        (1000, 1),
        (1999, 2),
        (11999, 12),
    ],
)
def test_chunker(length, expected_output):
    test_str = "s " * length
    # TODO - add " s " to test case once regex splitting added for multiple whitespaces
    assert len(utils.chunk_text(test_str)) == expected_output


def test_load_txt_file():
    loaded_text = utils.load_txt_file()
    assert type(loaded_text) == str
    assert "kubernetes" in loaded_text


def test_build_weaviate_object():
    chunk = "this text"
    test_dict = {
        "source_location": "https://weaviate.io",
    }
    new_dict = {k: v for k, v in test_dict.items()}
    new_dict["body"] = chunk
    assert utils.build_weaviate_object(chunk, test_dict) == new_dict