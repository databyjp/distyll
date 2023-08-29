from fastapi.testclient import TestClient
from unittest.mock import patch
from old.app import app

client = TestClient(app)


def test_submit_url():
    # Mocking the add_to_weaviate function to always return "pending"
    with patch('app.main.add_to_weaviate', return_value="pending"):
        response = client.post("/submit/", json={"url": "https://www.youtube.com/watch?v=test"})
        assert response.status_code == 200
        assert response.json() == {"status": "URL processing started"}


def test_ask_about_content():
    # Mocking the get_content_from_weaviate and ask_question functions
    with patch('app.main.get_content_from_weaviate', return_value="mocked content"):
        with patch('app.main.ask_question', return_value="mocked answer"):
            response = client.post("/ask/", json={"url": "https://www.youtube.com/watch?v=test", "question": "What's the video about?"})
            assert response.status_code == 200
            assert response.json() == {"content": "mocked content", "answer": "mocked answer"}



# import utils
# import wkb


# @pytest.mark.parametrize(
#     ("length", "expected_output"),
#     [
#         (0, 1),
#         (100, 1),
#         (1000, 1),
#         (1999, 2),
#         (11999, 12),
#     ],
# )
# def test_chunker(length, expected_output):
#     test_str = "s " * length
#     # TODO - add " s " to test case once regex splitting added for multiple whitespaces
#     assert len(utils.chunk_text(test_str)) == expected_output


# def test_load_txt_file():
#     loaded_text = utils.load_txt_file()
#     assert type(loaded_text) == str
#     assert "kubernetes" in loaded_text


# def test_build_weaviate_object():
#     chunk = "this text"
#     test_dict = {
#         "source_location": "https://weaviate.io",
#     }
#     new_dict = {k: v for k, v in test_dict.items()}
#     new_dict["body"] = chunk
#     assert wkb.build_weaviate_object(chunk, test_dict) == new_dict