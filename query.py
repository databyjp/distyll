from typing import Union
import distyll


def generate_on_search(db: distyll.DBConnection, prompt: str, search_query: str, object_path: Union[None, str]):
    """
    Perform a search and then a generative task on those search results
    For specific tasks that should be paired with a search (e.g. what does video AA say about topic BB?)
    :param db:
    :param prompt:
    :param search_query:
    :param object_path: Object path identifier for filtering
    :return:
    """
    pass


def generative_on_summary(db: distyll.DBConnection, prompt: str, object_path: str):
    """
    Perform a generative task on a summary of an object.
    For questions that relate to the entire object (e.g. what does video AA cover?)
    :param db:
    :param prompt:
    :param object_path: Object path identifier for filtering
    :return:
    """
    pass


# def generative_on_all(db: distyll.DBConnection, prompt: str, object_path: str):
#     """
#     Perform a generative task on all objects
#     For questions that relate to the entire object, and where using a summary will not be appropriate
#     :param db:
#     :param prompt:
#     :param object_path: Object path identifier for filtering
#     :return:
#     """
#     pass

