import time
import requests
import argparse
import hashlib
import json
import os
import pyotp
from datetime import datetime, timezone

# -----------------------------
# CONFIGURATION
# -----------------------------
BASE_URL = "https://api.shoonya.com/NorenWClientTP"
USER_ID = os.getenv("SHOONYA_UID")  # Your login UID
PASSWORD = os.getenv("SHOONYA_PWD")  # Your password (to be SHA256 hashed)
FACTOR2 = os.getenv("SHOONYA_2FA", "")  # OTP or TOTP
VC = os.getenv("SHOONYA_VC")  # Vendor code
API_KEY = os.getenv("SHOONYA_API_KEY")  # API key
IMEI = "520112004940064"  # or machine/mac
MAX_RETRIES = 20
RETRY_DELAY_SEC = 0.005  # 5 ms

session = requests.Session()

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def sha256(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def ns_to_str(ns):
    """Convert ns timestamp to local time string with ms precision"""
    dt = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
    return dt.astimezone().strftime("%H:%M:%S.%f")[:-3]

def login():
    appkey = sha256(f"{USER_ID}|{API_KEY}")
    payload = {
        "apkversion": "1.0.0",
        "uid": USER_ID,
        "pwd": sha256(PASSWORD),
        "factor2": pyotp.TOTP(FACTOR2).now(),
        "vc": VC,
        "appkey": appkey,
        "imei": "P6D7Y9DLXM",
        "source": "API"
    }
    response = session.post(
        f"{BASE_URL}/QuickAuth",
        data= 'jData=' + json.dumps(payload),
        timeout=2
    )
    data = response.json()
    if data.get("stat") == "Ok":
        return data["susertoken"], data
    else:
        raise Exception(f"Login failed: {data.get('emsg', 'Unknown error')}")

def place_order(
        usertoken, uid, actid, exch, tsym, qty, prc,
        prd="C", trantype="B", prctyp="LMT", ret="DAY", remarks=""
):
    payload = {
        "uid": uid,
        "actid": actid,
        "exch": exch,
        "tsym": tsym,
        "qty": str(qty),
        "dscqty": '0',
        "prc": str(prc),
        "prd": prd,
        "trantype": trantype,
        "prctyp": prctyp,
        "ret": ret,
        "amo": "YES"
    }

    attempt = 0
    while attempt <= MAX_RETRIES:
        start_ns = time.time_ns()
        try:
            resp = session.post(
                f"{BASE_URL}/PlaceOrder",
                data= 'jData=' + json.dumps(payload) + f'&jKey={usertoken}',
                timeout=2
            )
            end_ns = time.time_ns()
            latency_ns = end_ns - start_ns
            data = resp.json()

            if data.get("stat") == "Not_Ok":
                attempt += 1
                print(f"Order attempt {attempt} failed: {data.get('emsg')}")
                time.sleep(RETRY_DELAY_SEC)
                continue

            return {
                "response": data,
                "trigger_ns": start_ns,
                "response_ns": end_ns,
                "latency_ns": latency_ns
            }

        except Exception as e:
            attempt += 1
            print(f"Exception during order: {e}, retry {attempt}")
            time.sleep(RETRY_DELAY_SEC)

    return None

# -----------------------------
# TIME SYNC FUNCTION
# -----------------------------
def wait_for_target(hour=9, minute=0, second=0, ms=0):
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
        diff_ns = target_ns - now_ns
        if diff_ns > 1_000_000:   # >1 ms
            time.sleep(0.0005)
        elif diff_ns > 50_000:    # >50 µs
            time.sleep(0.00005)
        else:
            pass  # busy wait

# -----------------------------
# MAIN EXECUTION
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Shoonya order CLI")
    parser.add_argument("--exch", required=True, choices=["NSE", "NFO", "BSE", "MCX"])
    parser.add_argument("--tsym", required=True)
    parser.add_argument("--qty", required=True, type=int)
    parser.add_argument("--prc", required=True, type=float)
    parser.add_argument("--prd", default="C", choices=["C", "M", "I", "B", "H"])
    parser.add_argument("--trantype", default="B", choices=["B", "S"])
    parser.add_argument("--prctyp", default="LMT", choices=["LMT", "MKT", "SL-LMT", "SL-MKT"])
    parser.add_argument("--ret", default="DAY", choices=["DAY", "EOS", "IOC"])
    parser.add_argument("--hour", default=23, type=int)
    parser.add_argument("--minute", default=54, type=int)
    parser.add_argument("--second", default=0, type=int)
    parser.add_argument("--ms", default=0, type=int)

    args = parser.parse_args()

    usertoken, user_details = login()
    actid = user_details["actid"]  # Take account ID from login response

    trigger_ns = wait_for_target(args.hour, args.minute, args.second, args.ms)
    result = place_order(
        usertoken=usertoken,
        uid=USER_ID,
        actid=actid,
        exch=args.exch,
        tsym=args.tsym,
        qty=args.qty,
        prc=args.prc,
        prd=args.prd,
        trantype=args.trantype,
        prctyp=args.prctyp,
        ret=args.ret,
    )

    if result:
        print(
            f"Latency: {result['latency_ns']/1e6:.3f} ms | "
            f"Trigger: {ns_to_str(result['trigger_ns'])} | "
            f"Response: {ns_to_str(result['response_ns'])} | "
            f"Resp: {result['response']}"
        )
    else:
        print("Order failed after retries")
