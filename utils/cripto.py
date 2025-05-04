from web3 import Web3
import pandas


class Crypto:
    ABI = [
        {
            "constant": True,
            "inputs": [{"name": "_owner", "type": "address"}],
            "name": "balanceOf",
            "outputs": [{"name": "balance", "type": "uint256"}],
            "type": "function",
        },
        {
            "constant": True,
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "type": "function",
        }
    ]
    NETWORKS = {
        "Ethereum": {
            "rpc_url": "https://rpc.mevblocker.io",
            "native_currency": "ETH"
        },
        "Binance Smart Chain": {
            "rpc_url": "https://bsc-dataseed.binance.org/",
            "native_currency": "BNB"
        },
        "Arbitrum": {
            "rpc_url": "https://1rpc.io/arb",
            "native_currency": "ETH"
        },
        "Base": {
            "rpc_url": "https://1rpc.io/base",
            "native_currency": "ETH"
        },
        "Linea": {
            "rpc_url": "https://linea.drpc.org",
            "native_currency": "ETH"
        },
        "Polygon": {
            "rpc_url": "https://polygon.api.onfinality.io/public",
            "native_currency": "POL"
        },
        "zkSync": {
            "rpc_url": "https://rpc.ankr.com/zksync_era",
            "native_currency": "ETH"
        },
        "Avalanche": {
            "rpc_url": "https://1rpc.io/avax/c",
            "native_currency": "AVAX"
        },
    }
    wallets = pandas.read_csv("./CSVs/wallets.csv")
    tokens = pandas.read_csv("./CSVs/tokens.csv")

    def get_cripto_balance(self, ticker, token_address, wallet_address, network):
        if network not in self.NETWORKS:
            raise f"Сеть {network} не поддерживается"
        rpc = self.NETWORKS[network]["rpc_url"]
        w3 = Web3(Web3.HTTPProvider(rpc))
        if not w3.is_connected():
            raise "Ошибка соединения с сетью"
        if ticker == self.NETWORKS[network]["native_currency"]:
            balance_wei = w3.eth.get_balance(w3.to_checksum_address(wallet_address))
            balance = float(w3.from_wei(balance_wei, "ether"))
        else:
            contract = w3.eth.contract(
                address=w3.to_checksum_address(token_address),
                abi=self.ABI
            )
            balance = contract.functions.balanceOf(w3.to_checksum_address(wallet_address)).call()
            balance /= 10 ** contract.functions.decimals().call()
        return balance

    def check_wallets_balance(self):
        result = {}
        for _, wallet in self.wallets.iterrows():
            result[wallet["Public address"]] = {}
            for _, token in self.tokens.iterrows():
                result[wallet["Public address"]][token["Ticker"]] = result[wallet["Public address"]].get(token["Ticker"], 0) + self.get_cripto_balance(
                    ticker=token['Ticker'], token_address=token['address'],
                    wallet_address=wallet['Public address'],
                    network=token['Network']
                )
        return result

    def get_balance(self):
        result = {}
        for _, wallet in self.wallets.iterrows():
            for _, token in self.tokens.iterrows():
                if token["Ticker"] not in result:
                    result[token["Ticker"]] = {}
                result[token["Ticker"]] = {"name": token["Token name"],
                                           "quantity": result[token["Ticker"]].get("quantity", 0) + self.get_cripto_balance(
                    ticker=token['Ticker'], token_address=token['address'],
                    wallet_address=wallet['Public address'],
                    network=token['Network']
                )}
        return result


# c = Crypto()
# print(c.get_balance())

