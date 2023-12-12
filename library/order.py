from binance.um_futures import UMFutures
from binance.exceptions import BinanceAPIException
from binance.lib.utils import config_logging
from binance.error import ClientError
import pandas as pd
import numpy as np
import api
import logging
import datetime
from time import sleep

client = UMFutures(key=api.api_key, secret=api.secret_key)

def GET_OPEN_ORDERS(TICKER: str) -> pd.DataFrame:
    config_logging(logging, logging.DEBUG)
    error_bool = True
    while error_bool == True:
        try:
            open_orders = client.get_orders(symbol= TICKER, recvWindow= 2000)
            logging.info(open_orders)

        except ClientError as error:
            logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
            error.status_code, error.error_code, error.error_message
            ))
            sleep(10) 
        
        else:
            error_bool = False
    return pd.DataFrame(open_orders)

def SEARCH_ORDER(TICKER: str, ORDER_TYPE: str) -> bool:
    open_orders = GET_OPEN_ORDERS(TICKER= TICKER)
    searched = open_orders[open_orders["type"] == ORDER_TYPE]
    return not searched.empty 

def MARKET_ORDER(TICKER: str, SIDE: str, QTY: float, POSITION: str) -> None:
    config_logging(logging, logging.DEBUG)
    error_bool = True
    while error_bool == True:
        try:
            order = client.new_order(symbol=TICKER, side=SIDE, type='MARKET', quantity=QTY, positionSide= POSITION)
            logging.info(order)
        
        except ClientError as error:
            logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
            error.status_code, error.error_code, error.error_message
            ))
            sleep(10)
        
        else:
            error_bool = False
            print("MARKET_ORDER_PLACED")
    return None

def LIMIT_ORDER(TICKER: str, SIDE: str, PRICE: float, QTY: float, POSITION: str) -> None:
    config_logging(logging, logging.DEBUG)
    error_bool = True
    while error_bool == True:
        try:
            order = client.new_order(symbol=TICKER, side=SIDE, type='LIMIT', price = PRICE, quantity=QTY, positionSide= POSITION, timeInForce = 'GTC')
            logging.info(order)

        except ClientError as error:
            logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            ))
            sleep(10)
        
        else:
            error_bool = False
            print("LIMIT_ORDER_PLACED")
    return None

def STOP_ORDER(TICKER: str, SIDE: str, PRICE: float, QTY: float, POSITION: str) -> None:
    config_logging(logging, logging.DEBUG)
    error_bool = True
    while error_bool == True:
        try:
            order = client.new_order(symbol=TICKER, side=SIDE, type='STOP_MARKET', stopPrice= PRICE, quantity= QTY, positionSide= POSITION, timeInForce = 'GTC')
            logging.info(order)

        except ClientError as error:
            logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            ))
            sleep(10)
        
        else:
            error_bool = False
            print("STOP_ORDER_PLACED")
    return None

def TAKE_ORDER(TICKER: str, SIDE: str, PRICE: float, QTY: float, POSITION: str) -> None:
    config_logging(logging, logging.DEBUG)
    error_bool = True
    while error_bool == True:
        try:
            order = client.new_order(symbol=TICKER, side=SIDE, type='TAKE_PROFIT_MARKET', stopPrice= PRICE, quantity= QTY, positionSide= POSITION, timeInForce = 'GTC')
            logging.info(order)

        except ClientError as error:
            logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            ))
            sleep(10)
        
        else:    
            error_bool = False
            print("TAKE_ORDER_PLACED")
    return None

def TRAIL_ORDER(TICKER: str, SIDE: str, ACTIVATION_PRICE: float, CALLBACK: float, QTY: float, POSITION: str) -> None:
    config_logging(logging, logging.DEBUG)
    error_bool = True
    while error_bool == True:
        try:
            order = client.new_order(symbol=TICKER, side=SIDE, positionSide= POSITION, type='TRAILING_STOP_MARKET', 
                                     activationPrice= ACTIVATION_PRICE, callbackRate= CALLBACK, quantity= QTY, 
                                     timeInForce = 'GTC', timestamp= datetime.datetime.utcnow().timestamp())
            logging.info(order)

        except ClientError as error:
            logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            ))
            sleep(10)
        
        else:    
            error_bool = False
            print("TRAIL_ORDER_PLACED")
    return None

def CANCEL_ORDER(TICKER: str, ORDER_ID: int) -> None:
    config_logging(logging, logging.DEBUG)
    error_bool = True
    while error_bool == True:
        try:
            order = client.cancel_order(symbol= TICKER, orderId= ORDER_ID)
            logging.info(order)

        except ClientError as error:
            logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            ))
            sleep(10)

        else:
            error_bool = False
            print("ORDER_CANCELED")
    return None

  
def CANCEL_ALL(TICKER: str) -> None:
    config_logging(logging, logging.DEBUG)
    error_bool = True
    while error_bool == True:
        try:
            orders = client.cancel_open_orders(symbol=TICKER, recvWindow=2000)
            logging.info(orders)

        except ClientError as error:
            logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
                ))
            sleep(10)
        
        else:
            error_bool = False
            print("ALL_ORDERS_CANCELED")
           
    return None




