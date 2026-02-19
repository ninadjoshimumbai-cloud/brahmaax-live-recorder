import time
import csv
import requests

TOKEN_FILE = "token.txt"
CANDLE_FILE = "database/brahmaax_candles.csv"

UPSTOX_KEY = "NSE_INDEX|Nifty 50"

USE_UPSTOX = True


def get_upstox_token():
    return open(TOKEN_FILE).read().strip()


def get_upstox_price():

    try:

        url = "https://api.upstox.com/v2/market-quote/ltp"

        headers = {
            "Authorization": "Bearer " + get_upstox_token(),
            "Accept": "application/json"
        }

        params = {
            "instrument_key": UPSTOX_KEY
        }

        r = requests.get(url, headers=headers, params=params, timeout=3)

        data = r.json()

        if data["status"] == "success":

            key = list(data["data"].keys())[0]

            price = data["data"][key]["last_price"]

            return price

    except:

        return None

    return None


def get_nse_price():

    try:

        url = "https://www.nseindia.com/api/quote-equity?symbol=NIFTY%2050"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        }

        session = requests.Session()

        session.get("https://www.nseindia.com", headers=headers)

        r = session.get(url, headers=headers, timeout=3)

        data = r.json()

        price = data["priceInfo"]["lastPrice"]

        return price

    except:

        return None


def save(price):

    with open(CANDLE_FILE, "a", newline="") as f:

        csv.writer(f).writerow([
            int(time.time()),
            price,
            price,
            price,
            price
        ])

    print("Saved:", price)


print("BRAHMAAX LIVE FEED ENGINE STARTED")


while True:

    price = None

    # Try Upstox first
    price = get_upstox_price()

    if price is not None:

        print("Feed: UPSTOX")

        save(price)

        USE_UPSTOX = True

    else:

        print("Upstox failed. Switching to NSE feed...")

        price = get_nse_price()

        if price is not None:

            print("Feed: NSE")

            save(price)

            USE_UPSTOX = False

        else:

            print("Both feeds failed.")

    time.sleep(5)
