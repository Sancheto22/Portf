from binance.client import Client
from binance.um_futures import UMFutures
from binance.exceptions import BinanceAPIException
import api
import pandas as pd
import numpy as np
import datetime
from time import sleep, mktime
import json
import logging
from binance.lib.utils import config_logging
from binance.error import ClientError
import time


#===================================================Balance_bot===================================================================

## Rezalt %, z - denaminator
def ball(z:int, api_key:str, secret_key:str) -> float:
    config_logging(logging, logging.DEBUG)
    try:
        client = Client(api_key=api_key,
                    api_secret=secret_key)
        g = np.around((balance(api_key, secret_key)-z)/z*100,2)
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            ))
        g = np.around((balance(api_key, secret_key)-z)/z*100,2)
    return g

## Margin balance
def balance(api_key:str, secret_key:str) -> float:
    config_logging(logging, logging.DEBUG)
    try:
        client = UMFutures(key=api_key,
                           secret=secret_key)
        
        g = float(client.account()['totalMarginBalance'])
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            ))      
        g = float(client.account()['totalMarginBalance'])
    return np.around(g)

## Sum_open_abs(position)/Balance
def poss(api_key:str, secret_key:str) -> float:
    config_logging(logging, logging.DEBUG)
    try:
        client = UMFutures(key=api_key,
                           secret=secret_key)
        dt = pd.DataFrame(client.get_position_risk(ecvWindow=6000))
        dt = dt.astype({'positionAmt' : 'float', 'entryPrice' : 'float'})
        dt['p']= dt['positionAmt']*dt['entryPrice']
        dt = dt[dt['p']!=0]
        logging.info(dt) 
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            ))
        dt = pd.DataFrame(client.get_position_risk(ecvWindow=6000))
        dt = dt.astype({'positionAmt' : 'float', 'entryPrice' : 'float'})
        dt['p']= dt['positionAmt']*dt['entryPrice']
        dt = dt[dt['p']!=0]
    return np.around(sum(abs(dt['p']))/balance(api_key,secret_key)*100,2)


#====================================================Backtesting&Analysis===================================================================
## Выгрузка исторических данных historical_klines('ETHUSDT', '1h', '1 Dec 2022', '3 Dec 2022'/None)
## Import historical marketdata (klines)
def historical_klines(symbol: str, interval: str, start: str, end: str) -> pd.DataFrame:

    client = Client(api_key=api.api_key, api_secret=api.secret_key)
    klines_futures = pd.DataFrame(client.futures_historical_klines(symbol, interval, start, end))
    klines_futures = klines_futures.rename(columns = {0: 'open_time', 1:'open', 2:'high', 3:'low', 4:'close', 5:'volume'})
    klines_futures = klines_futures.drop([6,7,8,9,10,11], axis='columns')
    klines_futures['open_time'] = pd.to_datetime(klines_futures['open_time'], unit='ms', utc=True)
    klines_futures = klines_futures.astype({'open':'float', 'high':'float','low':'float', 'close':'float','volume':'float'})

    return klines_futures

## Для тестирования фандинга 
def historical_klines_str(symbol: str, interval: str, start: str, end: str) -> pd.DataFrame:

    client = Client(api_key=api.api_key, api_secret=api.secret_key)
    klines_futures = pd.DataFrame(client.futures_historical_klines(symbol, interval, start, end))
    klines_futures = klines_futures.rename(columns = {0: 'open_time', 1:'open', 2:'high', 3:'low', 4:'close', 5:'volume'})
    klines_futures = klines_futures.drop([6,7,8,9,10,11], axis='columns')
    klines_futures['open_time'] = pd.to_datetime(klines_futures['open_time'], unit='ms', utc=True)
    klines_futures['open_time'] = klines_futures['open_time'].apply(lambda x: str(x))
    klines_futures['open_time'] = klines_futures['open_time'].apply(lambda x: x.split('+')[0])
    klines_futures = klines_futures.astype({'open':'float', 'high':'float','low':'float', 'close':'float','volume':'float'})

    return klines_futures


## Import ticker's funding rate history
## format: "01.01.2023 00:00:00"
## limit= 1000, if start - end > 1000 (8h-candels) then function gives: start + 1000(8h-candels) 
def funding_history(symbol: str, start: str, end: str) -> pd.DataFrame:
    start_timestamp = datetime.datetime.strptime(start,'%d.%m.%Y %H:%M:%S').timestamp() * 1000
    end_timestamp = datetime.datetime.strptime(end,'%d.%m.%Y %H:%M:%S').timestamp() * 1000
    client_f = UMFutures(key=api.api_key, secret=api.secret_key) 
    funding_rate_history = pd.DataFrame(client_f.funding_rate(symbol= symbol, startTime= int(start_timestamp), end_timestamp= int(end_timestamp), limit= 1000))
    funding_rate_history["fundingTime"] = pd.to_datetime(funding_rate_history["fundingTime"], unit= "ms", utc= True)
    funding_rate_history = funding_rate_history.astype({'fundingRate':'float'})
    funding_rate_history['fundingRate'] = funding_rate_history.fundingRate.apply(lambda x: x*100)
    funding_rate_history['fundingTime'] = funding_rate_history['fundingTime'].apply(lambda x: str(x))
    funding_rate_history['fundingTime'] = funding_rate_history['fundingTime'].apply(lambda x: x.split('+')[0])
    funding_rate_history['fundingTime'] = funding_rate_history['fundingTime'].apply(lambda x: x.split('.')[0])
    return funding_rate_history

def funding_csv(start, end):
    client = UMFutures(key=api.api_key, secret=api.secret_key)
    ticker_order_book = pd.DataFrame(client.book_ticker())
    ticker_order_book = ticker_order_book[ticker_order_book['symbol'].str.contains('USDT')]
    ticker_order_book = ticker_order_book[ticker_order_book['symbol'] != 'ETHUSDT_230929']
    ticker_order_book = ticker_order_book[ticker_order_book['symbol'] != 'ETHUSDT_231229']
    ticker_order_book = ticker_order_book[ticker_order_book['symbol'] != 'BTCUSDT_230929']
    ticker_order_book = ticker_order_book[ticker_order_book['symbol'] != 'BTCUSDT_231229']
    df = funding_history('BTCUSDT', start, end)
    for row in ticker_order_book.itertuples():
        df = pd.concat([df, funding_history(row.symbol, start, end)], ignore_index= True)
    
    return df.to_csv('History_funding_all.csv')

#=======================================================Real_trading=======================================================================
## Свечи фьючерсы
## Import n - closest bars 75
def futures_klines(symbol: str, interval: str, limit: int) -> pd.DataFrame:

    try: 
        client_f = UMFutures(key=api.api_key, secret=api.secret_key)
        candels = client_f.klines(symbol=symbol, interval=interval , limit=limit)
        candels = pd.DataFrame(candels)

    except ClientError as error: 
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message
            ))
        #error_df = pd.DataFrame({"error_time": datetime.datetime.utcnow(), "status_code": error.status_code, "error_code": error.error_code, "error_message": error.error_message})
        #error_df.to_json("C:/trading/library/error.json")
        client_f = UMFutures(key=api.api_key, secret=api.secret_key)
        candels = client_f.klines(symbol=symbol, interval=interval , limit=limit)
        candels = pd.DataFrame(candels)
    
    candels = pd.DataFrame(candels)
    candels = candels.rename(columns = {0: 'open_time', 1:'open', 2:'high', 3:'low', 4:'close', 5:'volume', 6: 'close_time'})
    candels = candels.drop([7,8,9,10,11], axis='columns')
    candels = candels.astype({'open':'float', 'high':'float','low':'float', 'close':'float','volume':'float'})
    candels["open_time"] = pd.to_datetime(candels['open_time'], unit= "ms", utc= True)
    candels["close_time"] = pd.to_datetime(candels['close_time'], unit= "ms", utc= True)

    return candels

## Прибыль по открытой позиции 
## Information about current position
def current_trade(symbol: str) -> pd.DataFrame:
    client_f = UMFutures(key=api.api_key, secret=api.secret_key)
    error_bool = True
    while error_bool == True:
        try:
            position = pd.DataFrame(client_f.get_position_risk(symbol= symbol, 
                                                               recvWindow=10000, 
                                                               timestamp= datetime.datetime.utcnow().timestamp()))

        except ClientError as error: 
            logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message
                ))
        else:
            error_bool = False
    position = position.drop(columns=["maxNotionalValue", "isolatedMargin", "isAutoAddMargin", 
                                      "isolatedWallet", "markPrice", "marginType", "liquidationPrice"])
    position = position.astype({'entryPrice' : 'float', 'leverage' : 'float', 'unRealizedProfit': 'float', 'positionAmt': 'float' })
    position = position.loc[position['positionSide']!='BOTH']
    position['Sum_poss'] = abs(position['positionAmt'] * position['entryPrice'])
    position['PNL%'] = position['unRealizedProfit']/position['Sum_poss'] *100
    position["updateTime"] = pd.to_datetime(position['updateTime'], unit= "ms", utc= True)
    position = position.loc[position.last_valid_index()-1: position.last_valid_index()].reset_index()

    return position.drop(columns="index")

## Import ticker's current funding rate
def current_funding(symbol: str) -> pd.DataFrame:
    client_f = UMFutures(key=api.api_key, secret=api.secret_key) 
    try:       
        data = client_f.mark_price(symbol)
    
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(error.status_code, error.error_code, error.error_message
            ))
        data = client_f.mark_price(symbol)

    funding_rate = pd.DataFrame([data["symbol"], float(data["lastFundingRate"]), float(data["interestRate"]),
                                 int(data["nextFundingTime"]), int(data["time"])]).T
    funding_rate = funding_rate.rename(columns={0: "symbol", 1: "lastFundingRate", 2: "interestRate", 
                                 3: "nextFundingTime", 4: "time"})

    return funding_rate

## Import ticker's current funding rate_all
def current_funding_all(ticker_order_book):
    client = UMFutures(key=api.api_key, secret=api.secret_key)
    ticker_order_book = ticker_order_book
    funding_rate = current_funding('BLZUSDT')
    for row in ticker_order_book:
        funding_rate = pd.concat([funding_rate, current_funding(row)], ignore_index= True)
    funding_rate = funding_rate[funding_rate['symbol'].str.contains('USDT')]
    funding_rate['lastFundingRate'] = funding_rate['lastFundingRate'].apply(lambda x: x*100)
    return funding_rate

