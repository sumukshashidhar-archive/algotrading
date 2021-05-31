"""
Data streaming module to stream data and store to a csv using the Zerodha KiteConnect API
"""

# IMPORTS
## INSTALLED MODULE IMPORTS
from dotenv import load_dotenv
from kiteconnect import KiteTicker
from collections import deque
from datetime import datetime
from pytz import timezone
import os
import threading
import logging


## FILE IMPORTS
from subroutines.get_token import get_token


# DOTENV LOADING
load_dotenv()


## LOADING ENV VARS FROM .env
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
LOGIN = os.getenv("LOGIN")
PASSW = os.getenv("PASSW")
PIN = os.getenv("PIN")


def processor(data):
    # make the string format for the date
    strdate = datetime.now(timezone('UTC')).astimezone(timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
    for i in data:
        print(f'{strdate}:{i["instrument_token"]}:{i["last_price"]}')
        with open(f'./realtime/{i["instrument_token"]}.csv', 'a+') as f:
            f.write(f"{strdate},{i['last_price']}\n")
    return


# load access token from a temp file if it exists. else, set the value to None
try:
    with open("__TOKEN_STORE.txt", "r") as f:
        ACCESS_TOKEN = f.read()
except FileNotFoundError:
    ACCESS_TOKEN = None


# Check first if we have an access token
if ACCESS_TOKEN is not None:
    # if we have the token, try to init, else, try to use our details to make a new token, and make the kws object.
    try:
        kws = KiteTicker(API_KEY, ACCESS_TOKEN)
    except:
        ACCESS_TOKEN = get_token(API_KEY, API_SECRET, LOGIN, PASSW, PIN)
        with open("__TOKEN_STORE.txt", "w") as f:
            f.write(ACCESS_TOKEN)
        kws = KiteTicker(API_KEY, ACCESS_TOKEN)
else:
    ACCESS_TOKEN = get_token(API_KEY, API_SECRET, LOGIN, PASSW, PIN)
    with open("__TOKEN_STORE.txt", "w") as f:
        f.write(ACCESS_TOKEN)
    kws = KiteTicker(API_KEY, ACCESS_TOKEN)


def on_ticks(ws, ticks):
    # Callback to receive ticks.
    data = ticks
    threading.Thread(target=processor, args=(data,)).start()
    #logging.debug("Ticks: {}".format(ticks))


def on_connect(ws, response):
    # Callback on successful connect.
    ws.subscribe([2707457, 2928385, 1270529, 779521, 348929, 415745, 2977281, 633601, 5215745, 1207553, 969473, 2714625, 424961, 3834113, 134657, 3735553])

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_QUOTE, [2707457, 2928385, 1270529, 779521, 348929, 415745, 2977281, 633601, 5215745, 1207553, 969473, 2714625, 424961, 3834113, 134657, 3735553])


def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()


# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.


kws.connect()