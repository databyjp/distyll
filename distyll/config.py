import json


def load_gen_model() -> str:
    with open('config.json', 'r') as f:
        config = json.load(f)

    model_name = config.get('model_name', None)
    return model_name