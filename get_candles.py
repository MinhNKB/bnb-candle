from binance.enums import *
from binance.client import Client
import pandas as pd
import talib
import numpy as np
from itertools import compress
import json

class CandlesHelper:
    def __init__(self, config_path):
        with open(config_path, "r") as r:
            self.configs_dict = json.load(r)

        with open(self.configs_dict["pattern_ranking_path"], "r") as r:
            self.candle_rankings = json.load(r)

        api_configs = self.configs_dict["api_configs"]
        self.bnb_client = Client(api_configs["api_key"], api_configs["api_token"])

        with open(self.configs_dict["list_coins_path"], "r") as reader:
            self.list_of_symbols = [i.replace("\n", "") for i in reader.readlines()]

        self.crawled_data = None

    def crawl_symbols_data(self):
        count = 0
        for symbol in self.list_of_symbols:
            count += 1
            print(count, "/", len(self.list_of_symbols), ":", symbol)
            symbol_df = self._crawl_and_process_a_symbol(symbol, self.configs_dict["candles_interval"])
            if self.crawled_data is None:
                self.crawled_data = symbol_df
            else:
                self.crawled_data = self.crawled_data.append(symbol_df)
        self.crawled_data.to_csv(self.configs_dict["candles_path"], index=False)

    def _crawl_and_process_a_symbol(self, symbol, interval="1d", limit=10):
        if self.configs_dict["type"] == "spot":
            candles = self.bnb_client.get_klines(symbol=symbol, interval=interval, limit=limit)
        else:
            candles = self.bnb_client.futures_klines(symbol=symbol, interval=interval, limit=limit)

        headers = ["OPEN_TIME", "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "CLOSE_PRICE", "TRADE_VOLUME",
                   "CLOSE_TIME", "QUOTE_ASSET_VOLUME", "TRADE_COUNT", "TAKER_BUY_BASE", "TAKER_BUY_QUOTE", "IGNORE"]
        df = pd.DataFrame(candles, columns=headers)
        df["SYMBOL"] = symbol
        df["OPEN_TIME"] = pd.to_datetime(df["OPEN_TIME"], unit="ms")
        df["CLOSE_TIME"] = pd.to_datetime(df["CLOSE_TIME"], unit="ms")

        if self.configs_dict["exclude_current_point"]:
            df.drop(df.tail(1).index, inplace=True)

        ohlc = ["OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "CLOSE_PRICE"]
        for i in ohlc:
            df[i] = df[i].astype(float)
        self._check_candlestick_ta(df, ohlc=ohlc)
        self._pick_candlestick_ta(df)

        df["CURRENT_TREND"] = df["CLOSE_PRICE"] / df["OPEN_PRICE"] * 100
        df["MAX_PUMP"] = df["HIGH_PRICE"] / df["OPEN_PRICE"] * 100
        df["MAX_DUMP"] = df["LOW_PRICE"] / df["OPEN_PRICE"] * 100

        list_etp = self.configs_dict["list_of_etp"]
        if symbol in list_etp["3"]:
            df["ETP"] = 3
        elif symbol in list_etp["2"]:
            df["ETP"] = 2
        else:
            df["ETP"] = 1

        df = df.iloc[[-1]]
        return df

    def _check_candlestick_ta(self, df, ohlc=["Open", "High", "Low", "Close"]):
        candle_names = talib.get_function_groups()['Pattern Recognition']
        exclude_items = ('CDLCOUNTERATTACK',
                         'CDLLONGLINE',
                         'CDLSHORTLINE',
                         'CDLSTALLEDPATTERN',
                         'CDLKICKINGBYLENGTH')

        candle_names = [candle for candle in candle_names if candle not in exclude_items]

        # extract OHLC
        open_col, high_col, low_col, close_col = ohlc
        op = df[open_col]
        hi = df[high_col]
        lo = df[low_col]
        cl = df[close_col]

        # create columns for each pattern
        for candle in candle_names:
            df[candle] = getattr(talib, candle)(op, hi, lo, cl)

    def _pick_candlestick_ta(self, df):
        candle_names = talib.get_function_groups()['Pattern Recognition']
        exclude_items = ('CDLCOUNTERATTACK',
                         'CDLLONGLINE',
                         'CDLSHORTLINE',
                         'CDLSTALLEDPATTERN',
                         'CDLKICKINGBYLENGTH')

        candle_names = [candle for candle in candle_names if candle not in exclude_items]

        df["PATTERN"] = np.nan
        df["MATCH_COUNT"] = np.nan
        for index, row in df.iterrows():

            # no pattern found
            if len(row[candle_names]) - sum(row[candle_names] == 0) == 0:
                df.loc[index, "PATTERN"] = "NO_PATTERN"
                df.loc[index, "MATCH_COUNT"] = 0
            # single pattern found
            elif len(row[candle_names]) - sum(row[candle_names] == 0) == 1:
                # bull pattern 100 or 200
                if any(row[candle_names].values > 0):
                    pattern = list(compress(row[candle_names].keys(), row[candle_names].values != 0))[0] + '_Bull'
                    df.loc[index, "PATTERN"] = pattern
                    df.loc[index, "MATCH_COUNT"] = 1
                    df.loc[index, "PATTERN_RANK"] = self.candle_rankings[pattern]
                    df.loc[index, "PATTERN_SCORE"] = abs(max(row[candle_names].values))
                # bear pattern -100 or -200
                else:
                    pattern = list(compress(row[candle_names].keys(), row[candle_names].values != 0))[0] + '_Bear'
                    df.loc[index, "PATTERN"] = pattern
                    df.loc[index, "MATCH_COUNT"] = 1
                    df.loc[index, "PATTERN_RANK"] = self.candle_rankings[pattern]
                    df.loc[index, "PATTERN_SCORE"] = abs(min(row[candle_names].values))
            # multiple patterns matched -- select best performance
            else:
                # filter out pattern names from bool list of values
                patterns = list(compress(row[candle_names].keys(), row[candle_names].values != 0))
                container = []
                score = []
                for pattern in patterns:
                    if row[pattern] > 0:
                        container.append(pattern + '_Bull')
                        score.append(abs(row[pattern]))
                    else:
                        container.append(pattern + '_Bear')
                        score.append(abs(row[pattern]))
                rank_list = [self.candle_rankings[p] for p in container]
                if len(rank_list) == len(container):
                    rank_index_best = rank_list.index(min(rank_list))
                    df.loc[index, "PATTERN"] = container[rank_index_best]
                    df.loc[index, "MATCH_COUNT"] = len(container)
                    df.loc[index, "PATTERN_RANK"] = min(rank_list)
                    df.loc[index, "PATTERN_SCORE"] = score[rank_index_best]
        # clean up candle columns
        df.drop(candle_names, axis=1, inplace=True)


if __name__ == '__main__':
    candle_helper = CandlesHelper("future_configs.json")
    candle_helper.crawl_symbols_data()
