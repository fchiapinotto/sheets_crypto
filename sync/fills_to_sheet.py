import os, time, json, hmac, hashlib, base64
from urllib.parse import urlencode
import requests, gspread
from google.oauth2.service_account import Credentials

BASE = "https://api.bitget.com"

def map_symbol(sym):  # BTCUSDT -> BTCUSDT_UMCBL (perp USDT da Bitget)
    s = sym.upper().replace("_","").replace("-","")
    return f"{s}_UMCBL" if s.endswith("USDT") else s

def sign(ts, method, path, query="", body=""):
    prehash = f"{ts}{method}{path}{query}{body}"
    sec = os.environ["BITGET_API_SECRET"].encode()
    return base64.b64encode(hmac.new(sec, prehash.encode(), hashlib.sha256).digest()).decode()

def h(ts, s):  # headers
    return {
        "ACCESS-KEY": os.environ["BITGET_API_KEY"],
        "ACCESS-SIGN": s,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": os.environ["BITGET_PASSPHRASE"],
        "Content-Type": "application/json"
    }

def get_fills(symbol, start_ms, end_ms):
    # tenta v1 e v2 (Bitget alterna versões)
    candidates = [
        ("/api/mix/v1/order/fills", {"symbol": symbol, "startTime": start_ms, "endTime": end_ms}),
        ("/api/mix/v2/order/fills", {"symbol": symbol, "startTime": start_ms, "endTime": end_ms}),
    ]
    for path, params in candidates:
        ts = str(int(time.time()*1000))
        q = "?" + urlencode(params)
        r = requests.get(BASE+path, params=params, headers=h(ts, sign(ts,"GET",path,q,"")), timeout=20)
        try:
            data = r.json()
        except Exception:
            continue
        if r.status_code==200 and isinstance(data,dict) and isinstance(data.get("data"),list):
            return data["data"]
    raise RuntimeError(f"Erro ao buscar fills para {symbol}: {r.status_code} {r.text[:120]}")

def open_sheet():
    info = json.loads(os.environ["GOOGLE_SA_JSON"])
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    name = os.getenv("SHEET_TRADES_NAME","Trades")
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=2000, cols=20)
        ws.append_row(["DataHora","Par","Direção","Setup","TF","Entrada","Stop","Saída","Qty_BTC","Taxas_USDT","PnL_USDT","R_USDT_real","R_múltiplos","orderId","tradeId"])
    return ws

def existing_ids(ws):
    try:
        col = ws.col_values(15)  # tradeId (coluna O)
        return set(col[1:])
    except Exception:
        return set()

def to_rows(fills, par):
    rows=[]
    for f in fills:
        trade_id = f.get("tradeId") or f.get("fillId") or ""
        order_id = f.get("orderId","")
        side = (f.get("posSide") or f.get("side","")).lower()
        direcao = "Long" if "long" in side or "buy" in side else "Short"
        price = float(f.get("price",0))
        size  = float(f.get("size",0) or f.get("baseVolume",0) or 0)
        fee   = float(f.get("fee",0) or 0)
        pnl   = float(f.get("pnl",0) or 0)
        tsms  = int(f.get("ctime", f.get("timestamp", int(time.time()*1000))))
        dt    = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(tsms/1000))
        rows.append([dt, par, direcao, "", "", "", "", f"{price}", f"{size}", f"{fee}", f"{pnl}", "", "", order_id, trade_id])
    return rows

def main():
    ws = open_sheet()
    seen = existing_ids(ws)
    end_ms = int(time.time()*1000)
    start_ms = end_ms - 24*60*60*1000
    symbols = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",") if s.strip()]
    new=[]
    for par in symbols:
        try:
            fills = get_fills(map_symbol(par), start_ms, end_ms)
            rows = [r for r in to_rows(fills, par) if r[-1] and r[-1] not in seen]
            new.extend(rows)
        except Exception as e:
            print("[WARN]", par, e)
    if new:
        ws.append_rows(new, value_input_option="USER_ENTERED")
    print("Novas linhas:", len(new))

if __name__=="__main__":
    main()
