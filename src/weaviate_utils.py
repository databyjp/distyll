def _extract_get_results(res, target_class):
    """
    Extract results from returned GET GraphQL call from Weaviate
    :param res:
    :param target_class:
    :return:
    """
    return res["data"]["Get"][target_class]
