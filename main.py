import argparse
from utils.db import DB
import os

#  parser = argparse.ArgumentParser(
#         prog='balance-checker',
#         description='Следит за ценами добавленных активов и формирует отчеты'
#     )
#     parser.add_argument('first_file')
#     parser.add_argument('second_file')
#     parser.add_argument(
#         '-f', '--format',
#         help='set format of output',
#         default='stylish'
#     )
#     args = parser.parse_args()
#     # return [args.first_file, args.second_file, args.format]


def main():
    db = DB(db_name="portfolio.db")
    if not os.path.exists("portfolio.db"):
        print("yes")
    db.create_database()
    db.add_assets_from_csv() # Добавить проверку данных
    db.add_crypto_asserts()
    db.add_price_history()
    db.make_portfolio()


if __name__ == '__main__':
    main()
