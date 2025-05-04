import sqlite3
import pandas as pd
import os
from assets import Asset
from datetime import datetime
from cripto import Crypto


class DB:

    def __init__(self, db_name=None):
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)

    def _executer(self, sql, params=()):
        """
        Выполняет SQL-запрос с параметрами.
        """
        try:
            curr = self.conn.cursor()
            curr.execute(sql, params)
            self.conn.commit()
            if sql.strip().upper().startswith("SELECT") or "RETURNING" in sql.upper():
                result = curr.fetchall()
                return result
            elif sql.strip().upper().startswith("INSERT"):
                return curr.lastrowid
        except sqlite3.Error as e:
            print(f"Ошибка выполнения SQL: {e}")
            return False
        finally:
            curr.close()

    def create_database(self):
        """
        Создает базу данных и таблицы для портфеля активов.
        """
        # Таблица активов
        sql = '''
            CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL,
                ticker VARCHAR(10) NOT NULL UNIQUE,
                api VARCHAR(255) NOT NULL,
                type VARCHAR(255) NOT NULL
            )
        '''
        self._executer(sql)
        # # Таблица транзакций
        sql = '''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                amount INTEGER(10) NOT NULL,
                date VARCHAR(255) NOT NULL,
                FOREIGN KEY (asset_id) REFERENCES assets (id)
            )
        '''
        self._executer(sql)

        # Таблица истории цен
        sql = '''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                date VARCHAR(255) NOT NULL,
                price REAL NOT NULL,
                FOREIGN KEY (asset_id) REFERENCES assets (id)
            )
        '''
        self._executer(sql)
        print(f"База данных '{self.db_name}' успешно создана.")
        return True

    def add_assets_from_csv(self, csv_path):
        if not os.path.exists(csv_path):
            print(f"Файл '{csv_path}' не найден.")
            return False

        try:
            data = pd.read_csv(csv_path)
            required_columns = {"name", "ticker", "api", "type"}
            if not required_columns.issubset(data.columns):
                print(f"Ошибка: В файле отсутствуют обязательные столбцы: {required_columns - set(data.columns)}")
                return False
            sql = '''
                INSERT INTO assets (name, ticker, api, type)
                VALUES (?, ?, ?, ?)
            '''
            sql_transactions = '''
                INSERT INTO transactions (asset_id, amount, date) VALUES (?, ?, ?)
            '''
            for _, row in data.iterrows():

                asset = Asset(name=row["name"], ticker=row["ticker"], api=row["api"], type=row["type"])
                id = self._executer(sql, (asset.params))
                if not id:
                    id = self.get_id_asserts(ticker=row["ticker"])
                self._executer(sql_transactions, (id, row["quantity"], datetime.now()))
            print(f"Данные из файла '{csv_path}' успешно добавлены в таблицу 'assets'.")
            return True

        except Exception as e:
            print(f"Произошла ошибка при добавлении активов из CSV: {e}")
            return False

    def add_crypto_asserts(self):
        sql = '''SELECT assets.id as id, assets.name as name, assets.ticker as ticker,
                    SUM(transactions.amount) as amount
                 FROM assets INNER JOIN transactions ON assets.id = transactions.asset_id
                 WHERE assets.type = 'Crypto'
                 GROUP BY
                    assets.id, assets.name
                ORDER BY
                    assets.id, assets.name;
        '''
        sql_asserts = '''
                INSERT INTO assets (name, ticker, api, type)
                VALUES (?, ?, ?, ?)
        '''
        sql_transactions = '''
            INSERT INTO transactions (asset_id, amount, date) VALUES (?, ?, ?)
        '''
        data = self._executer(sql)
        c = Crypto()
        tokens_amount = c.get_balance()
        print(data)
        for item in data:
            if item[2] in tokens_amount and item[3] != tokens_amount.get(item[2])['quantity']:
                trans_amount = tokens_amount.get(item[2])['quantity'] - item[3]
                self._executer(sql_transactions,
                               (item[0], trans_amount, datetime.now()))
                tokens_amount.pop(item[2])
                print(f"значение крипто изменено!{trans_amount}")
        for token in tokens_amount:
            id = self._executer(sql_asserts, (
                tokens_amount[token]['name'],
                token, "Binance", "Crypto"
            ))
            if id:
                self._executer(sql_transactions, (
                    id, tokens_amount[token]['quantity'], datetime.now()
                ))

    def get_id_asserts(self, ticker):
        sql = '''
            SELECT id FROM assets WHERE ticker = ?
        '''
        result = self._executer(sql, (ticker,))
        if result:
            return result[0][0]

    def get_assets(self):
        sql = '''
            SELECT * FROM assets;
        '''
        data = self._executer(sql)
        return data

    def add_price_history(self):
        assets = self.get_assets()
        sql = '''
            INSERT INTO price_history (asset_id, date, price)
                VALUES (?, ?, ?)
        '''
        for asset in assets:
            id = asset[0]
            a = Asset(*(asset[1:]))
            price = a.get_price()
            self._executer(sql, (id, datetime.now(), price))

    def make_portfolio(self):
        sql = '''
            SELECT
                assets.name AS asset_name,
                SUM(transactions.amount) AS total_amount,
                latest_prices.price AS asset_price,
                (SUM(transactions.amount) * latest_prices.price) AS total_value,
                latest_prices.date AS price_date
            FROM
                transactions
            INNER JOIN
                assets ON transactions.asset_id = assets.id
            INNER JOIN
                (SELECT
                        asset_id,
                        price,
                        date
                    FROM
                        price_history
                    WHERE
                        date = (
                            SELECT MAX(date)
                            FROM price_history AS ph
                            WHERE ph.asset_id = price_history.asset_id
                        )
                ) AS latest_prices ON assets.id = latest_prices.asset_id
            GROUP BY
                assets.type, assets.name, latest_prices.price, latest_prices.date;
        '''
        result = self._executer(sql)
        headers = ['asset_name', 'total_amount',
                   'asset_price', 'total_value', 'price_date']
        df = pd.DataFrame(result, columns=headers)
        df.to_excel("portfolio.xlsx", index=False)
        print(f"Результат записан в файл portfolio.xlsx")



if __name__ == "__main__":
    db = DB(db_name="portfolio.db")
    db.create_database()
    csv_file_path = "./CSVs/assets.csv"  # Укажите путь к вашему CSV
    db.add_assets_from_csv(csv_file_path)
    db.add_crypto_asserts()
    db.add_price_history()
    
    db.make_portfolio()