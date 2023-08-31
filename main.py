# main.py
import logging
import distyll

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s')


def main():
    db = distyll.DBConnection()
    pass


if __name__ == '__main__':
    main()
