import sqlite3
from utils.assets import Asset
from datetime import datetime


class DB:

    def __init__(self, db_name=None):
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)

    def _executer(self, sql, params=()):
        """
        Выполняет SQL-запрос c параметрами.
        """
        try:
            curr = self.conn.cursor()
            curr.execute(sql, params)
            self.conn.commit()
            if sql.strip().upper().startswith("SELECT") or \
                    "RETURNING" in sql.upper():
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

    def add_assets(self, name, ticker, api, _type):
        sql = '''
                INSERT INTO assets (name, ticker, api, type)
                VALUES (?, ?, ?, ?)
            '''
        asset = Asset(name=name, ticker=ticker, api=api, type=_type)
        _id = self._executer(sql, (asset.params))
        return _id

    def add_transactions(self, _id, quantity):
        sql_transactions = '''
                INSERT INTO transactions (asset_id, amount, date)
                VALUES (?, ?, ?)
            '''
        return self._executer(sql_transactions, (_id, quantity,
                                                 datetime.now()))

    def add_price_history(self, _id, price):
        sql = '''
            INSERT INTO price_history (asset_id, date, price)
                VALUES (?, ?, ?)
        '''
        return self._executer(sql, (id, datetime.now(), price))

    def get_crypto_assets(self):
        sql = '''SELECT assets.id as id, assets.name as name,
                        assets.ticker as ticker,
                    SUM(transactions.amount) as amount
                 FROM assets INNER JOIN transactions
                    ON assets.id = transactions.asset_id
                 WHERE assets.type = 'Crypto'
                 GROUP BY
                    assets.id, assets.name
                ORDER BY
                    assets.id, assets.name;
        '''
        return self._executer(sql)

    def get_all_assets(self):
        sql = '''
            SELECT * FROM assets;
        '''
        return self._executer(sql)

    def get_asset_id(self, ticker):
        sql = '''
            SELECT id FROM assets WHERE ticker = ?
        '''
        result = self._executer(sql, (ticker,))
        if result:
            return result[0][0]

    def get_portfolio_data(self):
        sql = '''
            SELECT
                assets.name AS asset_name,
                SUM(transactions.amount) AS total_amount,
                latest_prices.price AS asset_price,
                (SUM(transactions.amount) * latest_prices.price)
                    AS total_value,
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
                assets.type, assets.name, latest_prices.price,
                latest_prices.date;
        '''
        return self._executer(sql)
