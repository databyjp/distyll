# main.py
import logging
import db

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s')


def main():
    client = db.connect_weaviate()
    pass


if __name__ == '__main__':
    main()
