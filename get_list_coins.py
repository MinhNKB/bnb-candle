from binance.client import Client
from binance.enums import *
import json

def get_list_exchanges(configs_path):
    with open(configs_path) as reader:
        configs_dict = json.load(reader)
    print(configs_dict)

    api_configs = configs_dict["api_configs"]
    bnb_client = Client(api_configs["api_key"], api_configs["api_token"])

    writer = open(configs_dict["list_coins_path"], "w")

    if configs_dict["type"] == "spot":
        exchanges = bnb_client.get_exchange_info()
    else:
        exchanges = bnb_client.futures_exchange_info()

    for i in exchanges["symbols"]:
        if configs_dict["type"] == "spot":
            if (i["quoteAsset"] == "USDT") and (i["status"] == "TRADING") and (i["isSpotTradingAllowed"] == True):
                writer.write(i["symbol"] + "\n")
        else:
            if i["contractType"] == "PERPETUAL":
                writer.write(i["symbol"] + "\n")

    writer.close()


if __name__ == '__main__':
    get_list_exchanges("future_configs.json")