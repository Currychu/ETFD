import requests
import pandas as pd
import re
import time
import yfinance as yf
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


# 隱藏 yfinance 找不到股價時的預設錯誤訊息
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ── 1. 參數與寫死資料設定 ────────────────────────────────────
DATE_START = "2025/01/01"
DATE_END = (datetime.now() + timedelta(days=90)).strftime("%Y/%m/%d")

FC_URL  = "https://www.fundclear.com.tw/api/etf/dividend/query-dividend"
MDJ_URL = "https://www.moneydj.com/jsondata/ETF/ETFJsonData.xdjjson?x=Tool0004&a=TW"
PAGE_SIZE = 800

# 從最新上傳檔案匯出的固定配息頻率資料
HARDCODED_FREQ_DICT = {
    '0050': '半年配', '0051': '半年配', '0052': '年配', '0053': '年配', '0055': '年配',
    '0056': '季配', '006201': '年配', '006203': '半年配', '006204': '年配', '006208': '半年配',
    '00679B': '季配', '00687B': '季配', '00690': '季配', '00692': '年配', '00694B': '季配',
    '00695B': '季配', '00696B': '季配', '00697B': '季配', '00701': '半年配', '00702': '半年配',
    '00710B': '季配', '00711B': '季配', '00712': '季配', '00713': '季配', '00714': '季配',
    '00717': '季配', '00719B': '季配', '00720B': '季配', '00722B': '季配', '00723B': '季配',
    '00724B': '季配', '00725B': '季配', '00726B': '季配', '00727B': '季配', '00728': '季配',
    '00730': '月配', '00731': '季配', '00733': '半年配', '00734B': '季配', '00735': '半年配',
    '00736': '半年配', '00739': '年配', '00740B': '月配', '00741B': '月配', '00746B': '季配',
    '00749B': '季配', '00750B': '季配', '00751B': '季配', '00754B': '季配', '00755B': '季配',
    '00756B': '季配', '00758B': '季配', '00759B': '季配', '00760B': '季配', '00761B': '季配',
    '00764B': '季配', '00768B': '季配', '00770': '年配', '00771': '季配', '00772B': '月配',
    '00773B': '月配', '00775B': '季配', '00777B': '月配', '00778B': '月配', '00779B': '季配',
    '00780B': '季配', '00781B': '季配', '00782B': '季配', '00785B': '季配', '00786B': '季配',
    '00787B': '季配', '00788B': '季配', '00789B': '季配', '00791B': '季配', '00792B': '季配',
    '00793B': '季配', '00795B': '季配', '00799B': '季配', '00830': '年配', '00834B': '季配',
    '00836B': '季配', '00840B': '月配', '00841B': '季配', '00842B': '季配', '00844B': '季配',
    '00845B': '季配', '00846B': '季配', '00847B': '季配', '00848B': '季配', '00849B': '季配',
    '00850': '季配', '00851': '年配', '00853B': '季配', '00856B': '季配', '00857B': '季配',
    '00858': '半年配', '00859B': '季配', '00860B': '季配', '00862B': '季配', '00863B': '季配',
    '00864B': '季配', '00867B': '季配', '00870B': '季配', '00878': '季配', '00881': '半年配',
    '00882': '半年配', '00883B': '月配', '00884B': '季配', '00888': '季配', '00890B': '月配',
    '00891': '季配', '00892': '半年配', '00894': '季配', '00896': '季配', '00900': '月配',
    '00901': '年配', '00904': '季配', '00905': '季配', '00907': '雙月配', '00908': '季配',
    '00909': '年配', '00911': '半年配', '00912': '季配', '00913': '半年配', '00915': '季配',
    '00916': '半年配', '00917': '年配', '00918': '季配', '00919': '季配', '00920': '年配',
    '00921': '季配', '00922': '半年配', '00923': '半年配', '00926': '年配', '00927': '季配',
    '00928': '半年配', '00929': '月配', '00930': '雙月配', '00931B': '季配', '00932': '季配',
    '00933B': '月配', '00934': '月配', '00935': '半年配', '00936': '月配', '00937B': '月配',
    '00938': '季配', '00939': '月配', '00940': '月配', '00942B': '月配', '00943': '月配',
    '00944': '月配', '00945B': '月配', '00946': '月配', '00947': '季配', '00948B': '月配',
    '00950B': '月配', '00951': '年配', '00952': '月配', '00953B': '月配', '00956': '季配',
    '00957B': '月配', '00958B': '月配', '00959B': '月配', '00960': '季配', '00961': '月配',
    '00962': '月配', '00963': '月配', '00964': '月配', '00966B': '月配', '00967B': '月配',
    '00968B': '月配', '00970B': '月配', '00971': '年配', '00972': '季配', '009802': '季配',
    '009803': '季配', '009804': '半年配', '009808': '季配', '00980A': '季配', '00980B': '月配',
    '00980D': '月配', '00981A': '季配', '00981B': '月配', '00981D': '月配', '00981T': '月配',
    '00982A': '季配', '00982B': '月配', '00982D': '月配', '00983B': '月配', '00983D': '月配',
    '00984A': '季配', '00984B': '月配', '00985B': '月配', '00986B': '月配'
}

COLUMN_MAP = {
    "證券代號": "證券代號", "ETF名稱": "ETF名稱", "ETF分類": "ETF分類", "配息頻率": "配息頻率",
    "除息前一日收盤價": "除息前一日收盤價", "當次配息率(%)": "當次配息率(%)", "預估年化配息率(%)": "預估年化配息率(%)",
    "除息交易日": "除息交易日", "收益分配基準日": "收益分配基準日", "收益分配發放日": "收益分配發放日",
    "收益分配金額(每1受益權益單位)": "收益分配金額(每1受益權益單位)",
    "divDet1": "股利所得占比(%)", "divDet2": "利息所得占比(%)",
    "divDet3": "收益平準金占比(%)", "divDet4": "已實現資本利得占比(%)",
    "divDet5": "其他所得占比(%)",
    "股利所得(元)": "股利所得(元)", "利息所得(元)": "利息所得(元)",
    "收益平準金(元)": "收益平準金(元)", "已實現資本利得(元)": "已實現資本利得(元)",
    "其他所得(元)": "其他所得(元)", "divWarn": "警語", "annYear": "公告年度",
}

# ── 2. 工具函式與分類邏輯 ───────────────────────────────────
def _clean_ticker(raw: str) -> str:
    return re.sub(r"[^0-9A-Z]", "", str(raw).split(".")[0]).upper()

def _to_float(val):
    try:
        v = float(str(val).replace(",", ""))
        return 0.0 if (v != v or v <= -9000) else v
    except: return 0.0

def _parse_date(text: str) -> str | None:
    m = re.search(r"(\d{4})[/\-]?(\d{2})[/\-]?(\d{2})", str(text))
    return "".join(m.groups()) if m else None

def get_etf_category(ticker: str, name: str) -> str:
    """依據證券代號後綴與名稱判斷分類"""
    ticker = str(ticker).strip().upper()
    name_upper = str(name).strip().upper()

    if ticker.endswith('B'): return "債券ETF"
    if ticker.endswith('A'): return "股票主動式ETF"
    if ticker.endswith('D'): return "債券主動式ETF"
    if ticker.endswith('T'): return "多重或平衡ETF"

    foreign_kws = [
        "美國", "美股", "北美", "日本", "日股", "韓國", "韓股", "中國", "中概",
        "香港", "港股", "印度", "越南", "歐洲", "全球", "世界", "新興", "費城",
        "費半", "那斯達克", "標普", "道瓊", "滬深", "上証", "恒生", "跨國",
        "SP", "NASDAQ", "NYSE", "大中華", "海外", "亞太", "東協"
    ]
    domestic_kws = ["台", "臺", "富邦科技", "中小型"]

    for d_kw in domestic_kws:
        if d_kw in name_upper: return "國內股票ETF"

    for f_kw in foreign_kws:
        if f_kw in name_upper: return "國外股票ETF"

    return "國內股票ETF"

def guess_frequency(ticker, mdj_freq, name):
    """優先使用寫死的頻率資料，找不到再依靠 MDJ 或名稱推測"""
    ticker = str(ticker).strip().upper()
    if ticker in HARDCODED_FREQ_DICT:
        return HARDCODED_FREQ_DICT[ticker]

    if pd.notna(mdj_freq) and str(mdj_freq).strip() != "":
        return str(mdj_freq)

    name_str = str(name)
    if "月配" in name_str: return "月配"
    if "雙月配" in name_str: return "雙月配"
    if "季配" in name_str: return "季配"
    if "半年" in name_str: return "半年配"
    if "年配" in name_str: return "年配"

    return "不定期"

# ── 3. 階段一：基富通全量同步 ──────────────────────────────────
def get_fundclear_master():
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.fundclear.com.tw/etf/dividend?type=domestic",
        "X-Requested-With": "XMLHttpRequest",
    }
    all_rows, page_num, total_records = [], 1, 1
    print(f"📡 啟動「基富通全量同步」 (範圍: {DATE_START} 至 {DATE_END})...")

    try:
        while len(all_rows) < total_records:
            payload = {
                "_pageNum": page_num, "_pageSize": PAGE_SIZE,
                "column": "", "asc": True, "etfType": "",
                "exDividendDateStart": DATE_START, "exDividendDateEnd": DATE_END,
                "payDateStart": "", "payDateEnd": "", "searchName": "",
            }
            res = requests.post(FC_URL, json=payload, headers=headers, timeout=30)
            res.raise_for_status()
            data = res.json()
            curr = data.get('list', [])
            if not curr: break

            all_rows.extend(curr)
            total_records = data.get('total', 0)
            print(f"    ✅ 已載入 {len(all_rows)} / {total_records} 筆紀錄")
            page_num += 1
            time.sleep(1)
    except Exception as e:
        print(f"    ❌ 基富通連線失敗: {e}")

    if not all_rows: return pd.DataFrame()
    df = pd.DataFrame(all_rows).rename(columns={
        "stockNo": "證券代號", "exDivDate": "除息交易日",
        "disAmount": "基富通金額", "name": "ETF名稱",
        "benchDate": "收益分配基準日", "payDate": "收益分配發放日",
        "divWarn": "警語", "annYear": "公告年度"
    })
    df["證券代號"] = df["證券代號"].astype(str).str.strip().str.upper()
    df["除息交易日"] = df["除息交易日"].apply(lambda v: _parse_date(v) or "")
    return df[df["除息交易日"] != ""].drop_duplicates(subset=["證券代號", "除息交易日"])

# ── 4. 階段二：天網加速股價抓取 ───────────────────────────────
def fetch_yahoo_single(ticker):
    for suffix in [".TW", ".TWO"]:
        symbol = f"{ticker}{suffix}"
        try:
            tkr = yf.Ticker(symbol)
            hist = tkr.history(start="2024-12-01", interval="1d")

            if not hist.empty and 'Close' in hist.columns:
                if hist.index.tz is not None:
                    hist.index = hist.index.tz_localize(None)

                prices = hist['Close'].to_dict()
                return ticker, {k.strftime('%Y%m%d'): round(float(v), 2) for k, v in prices.items()}
        except:
            continue
    return ticker, None

def get_price_db_parallel(tickers):
    price_db = {}
    print(f"📡 啟動「天網加速股價掃描」 (執行緒並行處理中)...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_yahoo_single, t): t for t in tickers}
        for future in as_completed(futures):
            ticker, result = future.result()
            if result: price_db[ticker] = result
    print(f"    ✅ 股價資料庫建立完成，共獲取 {len(price_db)} 檔價格庫。")
    return price_db

# ── 5. 執行數據整合與精密計算 ──────────────────────────────────
def run_final_sync():
    df_fc = get_fundclear_master()
    if df_fc.empty:
        print("❌ 錯誤：無法取得基富通有效資料。")
        return

    print("📡 啟動「MoneyDJ 公告對接」...")
    df_mdj = pd.DataFrame()
    try:
        res = requests.get(MDJ_URL, headers={"User-Agent":"Mozilla/5.0"}, timeout=20)
        records = res.json().get("ResultSet", {}).get("Result", [])
        year = records[0].get("V3", "2026")
        mdj_list = []
        for r in records:
            ticker = _clean_ticker(r.get("V1", ""))
            for i in range(4, 16):
                amt = _to_float(r.get(f"V{i}", ""))
                if amt > 0:
                    mdj_list.append({"證券代號": ticker, "年月": f"{year}{str(i-3).zfill(2)}", "MDJ金額": amt, "MDJ頻率": r.get("V19", "")})
        df_mdj = pd.DataFrame(mdj_list)
    except: pass

    unique_tickers = df_fc["證券代號"].unique()
    price_db = get_price_db_parallel(unique_tickers)

    print("🔄 執行數據精算與除息日對齊...")
    df_fc["年月"] = df_fc["除息交易日"].str[:6]
    merged = df_fc.merge(df_mdj, on=["證券代號", "年月"], how="left")

    merged["收益分配金額(每1受益權益單位)"] = merged.apply(
        lambda r: _to_float(r.get("MDJ金額", 0)) if _to_float(r.get("MDJ金額", 0)) > 0 else _to_float(r.get("基富通金額", 0)),
        axis=1
    )

    merged["ETF分類"] = merged.apply(lambda r: get_etf_category(r["證券代號"], r["ETF名稱"]), axis=1)

    # 這裡會優先利用寫死的字典來取得頻率
    merged["配息頻率"] = merged.apply(lambda r: guess_frequency(r["證券代號"], r.get("MDJ頻率"), r["ETF名稱"]), axis=1)

    def calculate_metrics(r):
        t, ex_date = r["證券代號"], r["除息交易日"]
        amt = r["收益分配金額(每1受益權益單位)"]
        if t not in price_db or amt <= 0: return 0.0, 0.0, 0.0

        prices = price_db[t]
        ex_dt = datetime.strptime(str(ex_date), "%Y%m%d")
        today_dt = datetime.now()
        pre_price = 0.0

        if ex_dt > today_dt:
            if prices:
                latest_date = sorted(prices.keys())[-1]
                pre_price = prices[latest_date]
        else:
            for i in range(1, 11):
                target_d = (ex_dt - timedelta(days=i)).strftime("%Y%m%d")
                if target_d in prices:
                    pre_price = prices[target_d]
                    break

        if pre_price <= 0: return 0.0, 0.0, 0.0
        cur_yield = round((amt / pre_price) * 100, 3)
        freq = str(r["配息頻率"])
        # 依照頻率計算年化乘數，若為雙月配則乘以 6
        mult = 12 if "月" in freq and "雙月" not in freq else (6 if "雙月" in freq else (4 if "季" in freq else (2 if "半年" in freq else 1)))
        return pre_price, cur_yield, round(cur_yield * mult, 3)

    res_yields = merged.apply(calculate_metrics, axis=1)
    merged["除息前一日收盤價"] = [x[0] for x in res_yields]
    merged["當次配息率(%)"] = [x[1] for x in res_yields]
    merged["預估年化配息率(%)"] = [x[2] for x in res_yields]

    for i, label in enumerate(["股利", "利息", "收益平準金", "已實現資本利得", "其他"], 1):
        c_pct = f"divDet{i}"
        merged[f"{label}所得(元)"] = (merged["收益分配金額(每1受益權益單位)"] * pd.to_numeric(merged.get(c_pct, 0), errors='coerce').fillna(0) / 100).round(4)

    available_cols = [c for c in COLUMN_MAP if c in merged.columns]
    final_df = merged[available_cols].rename(columns=COLUMN_MAP)
    final_df = final_df.sort_values(["除息交易日", "證券代號"], ascending=[False, True])

    output_file = "ETF_Full_Synced_Final_2026.xlsx"
    final_df.to_excel(output_file, index=False)
    print(f"\n✨ 任務完成！檔案已產出：{output_file}")
    

if __name__ == "__main__":
    run_final_sync()
