from abc import ABC, abstractmethod
import yfinance as yf
import requests


class AssetABC(ABC):
    types = ("etf", "crypto", "securites", "bond")

    def __init__(self, name: str, ticker: str, api: str, type: str):
        """
        Абстрактный базовый класс для актива.
        """
        self.name = name
        self.ticker = ticker
        self.api = api.lower()
        if type.lower() in self.types:
            self.type = type.lower()
        else:
            raise ValueError(f"Недопустимый тип актива: {type}. Доступные типы: {', '.join(self.types)}")

    @abstractmethod
    def get_price(self) -> float:
        """
        Абстрактный метод для получения цены актива.
        """
        pass

    def __str__(self):
        return f"Asset: {self.name}, Ticker: {self.ticker}, Type: {self.type}"


class YahooFI(AssetABC):
    def get_price(self) -> float:
        """
        Получает цену актива через Yahoo Finance.
        """
        try:
            ticker = yf.Ticker(self.ticker)
            return float(ticker.fast_info["last_price"])
        except Exception as e:
            print(f"Ошибка при получении цены через Yahoo Finance: {e}")
            return 0.0


class Binance(AssetABC):
    def get_price(self) -> float:
        """
        Получает цену актива через Binance API.
        """
        if self.ticker.upper() == "USDT":
            return 1.0
        base_url = "https://api.binance.com/api/v3/ticker/price"
        params = {"symbol": f"{self.ticker.upper()}USDT"}
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            return float(data["price"])
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе к Binance API: {e}")
            return 0.0
        except KeyError:
            print("Ошибка: Неверный формат ответа от Binance API.")
            return 0.0


class Asset():
    APIs = {"yahoofi": YahooFI, "binance": Binance}
    types = ("etf", "crypto", "securites", "bond")

    def __init__(self, name: str, ticker: str, api: str, type: str):
        if api.lower() not in self.APIs:
            raise ValueError(f"Недопустимый API: {api}. Доступные API: {', '.join(self.APIs.keys())}")
        self.type = type
        self.asset = self.APIs[api.lower()](name, ticker, api, type)
        self.params = (name, ticker, api, type)

    def get_price(self) -> float:
        return self.asset.get_price()

    def __str__(self):
        return str(self.asset)
