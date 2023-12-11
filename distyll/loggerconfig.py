import logging


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
    )


setup_logger()
