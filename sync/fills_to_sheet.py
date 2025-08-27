import os, time, json, hmac, hashlib, base64
from urllib.parse import urlencode
import requests
import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================
BITGET_BASE = "https://api.bitget.com"
PRODUCT_TYPE = os.getenv("PRODUCT_TYPE", "umcbl").lower()  # usdt-m perp = umcbl; usdc-m = cmcbl; coin-m = dmcbl
SHEET_NAME_ENV = os.getenv("SHEET_TRADES_NAME", "Trades")
SYMBOLS_ENV = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT")
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

# =========================
# Bitget auth helpers
# =========================
def sign_bitget(ts: str, method: str, path: str, query: str = "", body: str = "") -> str:
    # Prehash = timestamp + method + requestPath + queryString + body (sem espaços)
    prehash = f"{ts}{method}{path}{query}{body}"
    secret = os.environ["BITGET_API_SECRET"].encode()
    return base64.b64encode(hmac.new(secret, prehash.encode(), hashlib.sha256).digest()).decode()

def headers(ts: str, signature: str) -> dict:
    return {
        "ACCESS-KEY": os.environ["BITGET_API_KEY"],
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": os.environ["BITGET_PASSPHRASE"],
        "Content-Type": "application/json"
    }

def map_symbol(sym: str) -> str:
    """
    BTCUSDT -> BTCUSDT_UMCBL (padrão perp USDT na Bitget)
    """
    s = sym.upper().replace("_", "").replace("-", "")
    suffix = {"umcbl": "UMCBL", "cmcbl": "CMCBl", "dmcbl": "DMCBL"}.get(PRODUCT_TYPE, "UMCBL")
    # corrigir CMCBl para maiúsculo uniforme
    suffix = suffix.upper()
    if s.endswith("USDT"):
        return f"{s}_{suffix}"
    return s

# =========================
# Fetch privado: ALL FILLS (com paginação)
# =========================
def bitget_get_all_fills(product_type: str, start_ms: int, end_ms: int):
    """
    Endpoint correto (privado): /api/mix/v1/order/allFills
    Params obrigatórios: productType=umcbl|cmcbl|dmcbl, startTime, endTime
    Paginação: lastEndId
    Retorna lista de fills (trades executados) da conta.
    """
    path = "/api/mix/v1/order/allFills"
    out = []
    last_end_id = None
    page = 0

    while True:
        page += 1
        params = {"productType": product_type, "startTime": start_ms, "endTime": end_ms}
        if last_end_id:
            params["lastEndId"] = last_end_id

        ts = str(int(time.time() * 1000))
        query = "?" + urlencode(params)
        sig = sign_bitget(ts, "GET", path, query, "")
        r = requests.get(BITGET_BASE + path, params=params, headers=headers(ts, sig), timeout=25)

        # Parse básico
        try:
            data = r.json()
        except Exception:
            raise RuntimeError(f"Resposta não JSON: {r.status_code} {r.text[:200]}")

        if r.status_code != 200 or not isinstance(data, dict) or "data" not in data:
            raise RuntimeError(f"allFills falhou: {r.status_code} {str(data)[:200]}")

        batch = data.get("data") or []
        out.extend(batch)

        # Controle de página
        if not batch:
            break
        # pega id do último fill
        last = batch[-1]
        last_end_id = last.get("tradeId") or last.get("fillId") or last.get("id")
        # Se não veio id ou a página está “curta”, encerra
        if not last_end_id or len(batch) < 100:
            break

    return out

# =========================
# Google Sheets helpers
# =========================
def open_sheet():
    info = json.loads(os.environ["GOOGLE_SA_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.environ["SHEET_ID"])

    target_env = SHEET_NAME_ENV
    target_norm = target_env.strip().lower()

    # 1) tenta exato
    try:
        return sh.worksheet(target_env)
    except gspread.WorksheetNotFound:
        pass

    # 2) tenta normalizado (ignora espaços/caixa)
    for ws in sh.worksheets():
        if ws.title.strip().lower() == target_norm:
            return ws

    # 3) cria de fato
    ws = sh.add_worksheet(title=target_env, rows=2000, cols=20)
    ws.append_row([
        "DataHora","Par","Direção","Setup","TF","Entrada","Stop","Saída",
        "Qty_BTC","Taxas_USDT","PnL_USDT","R_USDT_real","R_múltiplos","orderId","tradeId"
    ])
    return ws

def existing_trade_ids(ws):
    try:
        col = ws.col_values(15)  # tradeId (coluna O)
        return set(col[1:])      # ignora cabeçalho
    except Exception:
        return set()

def append_rows(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")

# =========================
# Transformação: fills -> linhas do Sheet
# =========================
def to_rows_from_fills(fills, sym_clean):
    rows = []
    for f in fills:
        trade_id = f.get("tradeId") or f.get("fillId") or f.get("id") or ""
        order_id = f.get("orderId", "")
        # preço pode vir como price ou priceAvg
        price = float(f.get("price", f.get("priceAvg", 0)) or 0)
        # size pode vir como size/baseVolume/fillQty
        size = float(f.get("size", f.get("baseVolume", f.get("fillQty", 0))) or 0)
        fee  = float(f.get("fee", 0) or 0)
        pnl  = float(f.get("pnl", 0) or 0)
        tsms = int(f.get("ctime", f.get("timestamp", int(time.time()*1000))))
        dt_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(tsms/1000))

        # Direção: prioriza posSide long/short; fallback side buy/sell
        side_raw = (f.get("posSide") or f.get("side", "")).lower()
        if "long" in side_raw:
            direcao = "Long"
        elif "short" in side_raw:
            direcao = "Short"
        elif "buy" in side_raw:
            direcao = "Long"
        elif "sell" in side_raw:
            direcao = "Short"
        else:
            direcao = ""

        rows.append([
            dt_iso, sym_clean, direcao, "", "",  # DataHora, Par, Direção, Setup, TF
            "", "", f"{price}",                  # Entrada, Stop (vazio), Saída = price exec
            f"{size}", f"{fee}", f"{pnl}",       # Qty, Taxas, PnL_USDT
            "", "",                              # R_USDT_real, R_múltiplos (calculadas no Sheet)
            order_id, trade_id
        ])
    return rows

# =========================
# MAIN
# =========================
def main():
    ws = open_sheet()
    seen = existing_trade_ids(ws)

    # Janela de 3 dias para garantir captação (pode ajustar para 7)
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - 3 * 24 * 60 * 60 * 1000

    # Busca UMA vez todos os fills do produto (umcbl) e filtra por símbolo
    fills_all = bitget_get_all_fills(PRODUCT_TYPE, start_ms, end_ms)

    symbols = [s.strip() for s in SYMBOLS_ENV.split(",") if s.strip()]
    new_rows = []
    for par in symbols:
        sym_full = map_symbol(par)  # ex.: BTCUSDT_UMCBL
        # filtra o que é do par
        f_par = [f for f in fills_all if (f.get("symbol") or "").upper() == sym_full]
        rows = to_rows_from_fills(f_par, par)
        # dedup por tradeId (última coluna)
        rows = [r for r in rows if r[-1] and r[-1] not in seen]
        new_rows.extend(rows)
        print(f"[INFO] {par}: total={len(f_par)} novos={len(rows)}")

    if DRY_RUN:
        print(f"[DRY_RUN] Não escreveu. Iria append {len(new_rows)} linhas.")
        return

    append_rows(ws, new_rows)
    print(f"Novas linhas: {len(new_rows)}")

if __name__ == "__main__":
    main()
