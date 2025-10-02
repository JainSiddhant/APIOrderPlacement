import time
import requests
import argparse
import os

# -----------------------------
# CONFIGURATION
# -----------------------------
API_KEY = os.getenv('API_KEY')
BASE_URL = "https://api.dhan.co/v2"
DHAN_CLIENT_ID = "1108558883"  # fixed
MAX_RETRIES = 25
RETRY_DELAY_SEC = 0.001 # 10 ms between retries

# -----------------------------
# ORDER FUNCTION
# -----------------------------
session = requests.Session()


def place_order(transactionType, exchangeSegment, productType, orderType, securityId, quantity, price):
    payload = {
        "dhanClientId": DHAN_CLIENT_ID,
        # "correlationId": "143549_003",
        "transactionType": transactionType,
        "exchangeSegment": exchangeSegment,
        "productType": productType,
        "orderType": orderType,
        "validity": "DAY",
        "securityId": securityId,
        "quantity": quantity,
        "disclosedQuantity": 0,
        "price": price,
        # "afterMarketOrder": True,
        # "amoTime": "OPEN"
    }

    attempt = 0
    while attempt <= MAX_RETRIES:
        start_ns = time.time_ns()
        try:
            resp = session.post(
                f"{BASE_URL}/orders",
                headers={"access-token": API_KEY, "Content-Type": "application/json"},
                json=payload,
                timeout=1
            )
            end_ns = time.time_ns()
            data = resp.json()
            latency_ns = end_ns - start_ns
            # Retry if status == REJECTED OR says invalid script id
            if data.get("errorMessage") == "Market is Closed! Want to place an offline order?":
                attempt += 1
                print(f'Attempt count{attempt}')
                time.sleep(RETRY_DELAY_SEC)
                continue

            return {
                "response": data,
                "trigger_ns": start_ns,
                "response_ns": end_ns,
                "latency_ns": latency_ns
            }

        except Exception:
            attempt += 1
            time.sleep(RETRY_DELAY_SEC)

    return None  # All retries failed





# -----------------------------
# TIME SYNC FUNCTION
# -----------------------------


def wait_for_target(hour, minute, second, ms=0):
    """
    Busy-wait until the exact target time (HH:MM:SS.milliseconds)
    """
    t = time.localtime()
    year, month, day = t.tm_year, t.tm_mon, t.tm_mday
    target_epoch = time.mktime((year, month, day, hour, minute, second, 0, 0, -1))
    target_ns = int(target_epoch * 1_000_000_000) + ms * 1_000_000

    while True:
        now_ns = time.time_ns()
        if now_ns >= target_ns:
            return now_ns
        # busy-wait for microsecond precision
        diff_ns = target_ns - now_ns
        if diff_ns > 1_000_000:  # >1 ms
            time.sleep(0.0005)
        elif diff_ns > 50_000:  # >50 µs
            time.sleep(0.00005)
        else:
            pass  # busy wait final few µs

# -----------------------------
# MAIN EXECUTION
# -----------------------------


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dhan order CLI")
    parser.add_argument("--transactionType", required=True, choices=["BUY", "SELL"])
    parser.add_argument("--exchangeSegment", required=True, choices=["NSE_EQ", "BSE_EQ"])
    parser.add_argument("--productType", required=True, choices=["CNC", "MIS", "CO", "NRML"])
    parser.add_argument("--orderType", required=True, choices=["LIMIT", "MARKET", "STOP_LOSS", "STOP_LOSS_MARKET"])
    parser.add_argument("--securityId", required=True)
    parser.add_argument("--quantity", required=True, type=int)
    parser.add_argument("--price", required=True, type=float)
    parser.add_argument("--hour", default=9, type=int)
    parser.add_argument("--minute", default=0, type=int)
    parser.add_argument("--second", default=0, type=int)
    parser.add_argument("--ms", default=0, type=int)

    args = parser.parse_args()

    trigger_ns = wait_for_target(args.hour, args.minute, args.second, args.ms)
    result = place_order(
        transactionType=args.transactionType,
        exchangeSegment=args.exchangeSegment,
        productType=args.productType,
        orderType=args.orderType,
        securityId=args.securityId,
        quantity=args.quantity,
        price=args.price
    )

    if result:
        print(result["latency_ns"], result["trigger_ns"], result["response_ns"], result['response'])
    else:
        print("Order failed after retries")
