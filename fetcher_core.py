"""
fetcher_core.py — Lógica de extração sem UI
Usado pelo main.py (Kivy) e pelo market_data_fetcher_v7.py (terminal)
"""

import os, json, time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests

# ─── API KEYS ────────────────────────────────────────────────────────────────
BINANCE_API_KEY    = os.environ.get("BINANCE_API_KEY", "")
COINALYZE_API_KEY  = os.environ.get("COINALYZE_API_KEY", "")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")

BINANCE_BASE    = "https://api.binance.com"
BYBIT_BASE      = "https://api.bybit.com"
TWELVEDATA_BASE = "https://api.twelvedata.com"
COINALYZE_BASE  = "https://api.coinalyze.net/v1"

# ─── TIMEFRAME PRESETS ───────────────────────────────────────────────────────
TF_BASE = {
    "1w": {"bn":"1w",  "bb":"W",   "td":"1week", "minutes":10080, "out":"1W"},
    "1d": {"bn":"1d",  "bb":"D",   "td":"1day",  "minutes":1440,  "out":"1D"},
    "4h": {"bn":"4h",  "bb":"240", "td":"4h",    "minutes":240,   "out":"4H"},
    "1h": {"bn":"1h",  "bb":"60",  "td":"1h",    "minutes":60,    "out":"1H"},
}

TF_PRESETS = {
    "equity_otimizado": {"1w":120,  "1d":170,  "4h":300,  "1h":250},
    "equity_original":  {"1w":210,  "1d":730,  "4h":900,  "1h":840},
    "crypto_otimizado": {"1w":210,  "1d":365,  "4h":500,  "1h":500},
    "crypto_original":  {"1w":210,  "1d":730,  "4h":900,  "1h":840},
}

PROTOCOL_TFS = ["1w","1d","4h","1h"]
COINALYZE_IV = {"1w":"daily","1d":"daily","4h":"4hour","1h":"1hour","30m":"30min"}
TF_LB        = {"1w":3,"1d":3,"4h":3,"1h":3}

COINALYZE_MAP = {
    ("BTCUSDT","perp"):"BTCUSDT_PERP.A",  ("BTCUSDT","spot"):"BTCUSDT.6",
    ("ETHUSDT","perp"):"ETHUSDT_PERP.A",  ("ETHUSDT","spot"):"ETHUSDT.6",
    ("SOLUSDT","perp"):"SOLUSDT_PERP.A",  ("SOLUSDT","spot"):"SOLUSDT.6",
    ("BNBUSDT","perp"):"BNBUSDT_PERP.A",  ("BNBUSDT","spot"):"BNBUSDT.6",
    ("XRPUSDT","perp"):"XRPUSDT_PERP.A",  ("XRPUSDT","spot"):"XRPUSDT.6",
    ("DOGEUSDT","perp"):"DOGEUSDT_PERP.A", ("DOGEUSDT","spot"):"DOGEUSDT.6",
    ("ADAUSDT","perp"):"ADAUSDT_PERP.A",  ("ADAUSDT","spot"):"ADAUSDT.6",
    ("AVAXUSDT","perp"):"AVAXUSDT_PERP.A", ("AVAXUSDT","spot"):"AVAXUSDT.6",
    ("LINKUSDT","perp"):"LINKUSDT_PERP.A", ("LINKUSDT","spot"):"LINKUSDT.6",
    ("DOTUSDT","perp"):"DOTUSDT_PERP.A",  ("NEARUSDT","perp"):"NEARUSDT_PERP.A",
    ("SUIUSDT","perp"):"SUIUSDT_PERP.A",  ("APTUSDT","perp"):"APTUSDT_PERP.A",
    ("MATICUSDT","perp"):"MATICUSDT_PERP.A",("LTCUSDT","perp"):"LTCUSDT_PERP.A",
    ("WIFUSDT","perp"):"WIFUSDT_PERP.A",  ("PEPEUSDT","perp"):"PEPEUSDT_PERP.A",
}

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def build_tf_config(preset_name, custom_mins=None):
    mins = custom_mins if custom_mins else TF_PRESETS[preset_name]
    return {tf: {**base, "min": mins[tf]} for tf, base in TF_BASE.items()}

def _log(msg, cb):
    if cb: cb(msg)
    else:  print(msg)

def _get(url, params, headers=None, timeout=20):
    for attempt in range(3):
        r = requests.get(url, params=params, headers=headers or {}, timeout=timeout)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 5)))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"Falha após 3 tentativas: {url}")

def _now_ms(): return int(datetime.now(timezone.utc).timestamp() * 1000)
def _now_s():  return int(datetime.now(timezone.utc).timestamp())

def _raw_to_df(rows, ts_col, cols, ts_unit="ms"):
    df = pd.DataFrame(rows, columns=cols)
    df["timestamp"] = pd.to_datetime(df[ts_col].astype(int), unit=ts_unit, utc=True)
    for c in ["open","high","low","close","volume"]:
        df[c] = df[c].astype(float)
    return df.set_index("timestamp")[["open","high","low","close","volume"]].sort_index()

# ─── OHLCV FETCHERS ──────────────────────────────────────────────────────────
def fetch_binance(symbol, tf, n, tf_config):
    url     = f"{BINANCE_BASE}/api/v3/klines"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    mins    = tf_config[tf]["minutes"]
    rows, end_ms, rem = [], _now_ms(), n
    while rem > 0:
        lim  = min(rem, 1000)
        data = _get(url, {"symbol":symbol.upper(),"interval":tf_config[tf]["bn"],
                          "startTime":end_ms-lim*mins*60000,"endTime":end_ms,"limit":lim}, headers)
        if not data: break
        rows   = data + rows
        end_ms = int(data[0][0]) - 1
        rem   -= len(data)
        if len(data) < lim: break
        time.sleep(0.08)
    if not rows: return pd.DataFrame()
    cols = ["timestamp","open","high","low","close","volume","ct","qv","nt","tbb","tbq","ig"]
    return _raw_to_df(rows, "timestamp", cols)

def fetch_bybit(symbol, tf, n, tf_config, category="linear"):
    url = f"{BYBIT_BASE}/v5/market/kline"
    rows, end_ms, rem = [], _now_ms(), n
    while rem > 0:
        lim  = min(rem, 1000)
        data = _get(url, {"category":category,"symbol":symbol.upper(),
                          "interval":tf_config[tf]["bb"],"end":end_ms,"limit":lim})
        if data.get("retCode") != 0: raise ValueError(data.get("retMsg"))
        r = data["result"]["list"]
        if not r: break
        rows   = r + rows
        end_ms = int(r[-1][0]) - 1
        rem   -= len(r)
        if len(r) < lim: break
        time.sleep(0.08)
    if not rows: return pd.DataFrame()
    cols = ["timestamp","open","high","low","close","volume","turnover"]
    return _raw_to_df(rows, "timestamp", cols)

def fetch_twelvedata(symbol, tf, n, tf_config):
    data = _get(f"{TWELVEDATA_BASE}/time_series",
                {"symbol":symbol,"interval":tf_config[tf]["td"],
                 "outputsize":min(n,5000),"format":"JSON","apikey":TWELVEDATA_API_KEY})
    if "values" not in data: raise ValueError(data.get("message", data))
    df = pd.DataFrame(data["values"])
    df["timestamp"] = pd.to_datetime(df["datetime"], utc=True)
    df = df.set_index("timestamp").sort_index()
    for c in ["open","high","low","close","volume"]:
        df[c] = df[c].astype(float) if c in df.columns else np.nan
    return df[["open","high","low","close","volume"]]

# ─── CVD ─────────────────────────────────────────────────────────────────────
def fetch_cvd_spot_binance(symbol, tf="1h", days=90, tf_config=None):
    if tf_config is None: tf_config = build_tf_config("equity_otimizado")
    url     = f"{BINANCE_BASE}/api/v3/klines"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    mins    = tf_config.get(tf, tf_config["1h"])["minutes"]
    n       = int(days * 1440 / mins)
    rows, end_ms, rem = [], _now_ms(), n
    while rem > 0:
        lim  = min(rem, 1000)
        data = _get(url, {"symbol":symbol.upper(),"interval":tf_config[tf]["bn"],
                          "startTime":end_ms-lim*mins*60000,"endTime":end_ms,"limit":lim}, headers)
        if not data: break
        rows   = data + rows
        end_ms = int(data[0][0]) - 1
        rem   -= len(data)
        if len(data) < lim: break
        time.sleep(0.08)
    if not rows: return pd.DataFrame()
    cols = ["t","open","high","low","close","volume","ct","qv","nt","tbb","tbq","ig"]
    df = pd.DataFrame(rows, columns=cols)
    df["timestamp"] = pd.to_datetime(df["t"].astype(int), unit="ms", utc=True)
    df = df.set_index("timestamp").sort_index()
    df["buy_vol"]  = df["tbb"].astype(float)
    df["sell_vol"] = df["volume"].astype(float) - df["buy_vol"]
    df["delta"]    = df["buy_vol"] - df["sell_vol"]
    df["cvd"]      = df["delta"].cumsum()
    return df[["buy_vol","sell_vol","delta","cvd"]]

def _coin_get(endpoint, symbol, interval, days=90):
    to_ts = _now_s()
    data  = _get(f"{COINALYZE_BASE}/{endpoint}",
                 {"symbols":symbol,"interval":COINALYZE_IV.get(interval,"1hour"),
                  "from":to_ts-days*86400,"to":to_ts,"api_key":COINALYZE_API_KEY})
    if not data or not isinstance(data, list): return []
    return data[0].get("history", [])

def fetch_cvd_perp_coinalyze(symbol, tf, days=90):
    rows = _coin_get("ohlcv-history", symbol, tf, days)
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["t"], unit="s", utc=True)
    df = df.set_index("timestamp").sort_index()
    df["buy_vol"]  = df["bv"].astype(float)
    df["sell_vol"] = (df["v"] - df["bv"]).astype(float)
    df["delta"]    = df["buy_vol"] - df["sell_vol"]
    df["cvd"]      = df["delta"].cumsum()
    return df[["buy_vol","sell_vol","delta","cvd"]]

def fetch_funding(symbol, days=90):
    rows = _coin_get("funding-rate-history", symbol, "1hour", days)
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"]    = pd.to_datetime(df["t"], unit="s", utc=True)
    df["funding_rate"] = df["c"].astype(float)
    return df.set_index("timestamp")[["funding_rate"]].sort_index()

def fetch_lsr(symbol, tf, days=90):
    rows = _coin_get("long-short-ratio-history", symbol, tf, days)
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["t"], unit="s", utc=True)
    df = df.set_index("timestamp").rename(
        columns={"l":"long_pct","s":"short_pct","r":"ls_ratio"}).sort_index()
    cols = [c for c in ["long_pct","short_pct","ls_ratio"] if c in df.columns]
    return df[cols]

# ─── INDICADORES ─────────────────────────────────────────────────────────────
def _r(x, d=2):
    try:
        v = float(x)
        return None if np.isnan(v) else round(v, d)
    except: return None

def _rsi(c, p):
    d  = c.diff()
    g  = d.clip(lower=0).ewm(com=p-1, min_periods=p).mean()
    lo = (-d).clip(lower=0).ewm(com=p-1, min_periods=p).mean()
    return 100 - 100/(1+g/lo.replace(0,np.nan))

def _ema(c, s): return c.ewm(span=s, adjust=False).mean()

def _macd(c, f=12, sl=26, sg=9):
    m = _ema(c,f) - _ema(c,sl)
    s = m.ewm(span=sg, adjust=False).mean()
    return m, s, m-s

def _stoch(h, l, c, k=14, d=3):
    ll = l.rolling(k).min(); hh = h.rolling(k).max()
    K  = 100*(c-ll)/(hh-ll+1e-10)
    return K, K.rolling(d).mean()

def _bb(c, p=20, n=2):
    m  = c.rolling(p).mean(); s = c.rolling(p).std()
    u  = m+n*s; lo = m-n*s
    return u, m, lo, (u-lo)/m*100

def _atr(h, l, c, p=14):
    tr = pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
    return tr.ewm(com=p-1, min_periods=p).mean()

def _adx(h, l, c, p=14):
    tr  = pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
    up  = h.diff(); dn = (-l.diff())
    dmp = up.where((up>dn)&(up>0), 0.0)
    dmn = dn.where((dn>up)&(dn>0), 0.0)
    atr_v = tr.ewm(com=p-1,min_periods=p).mean()
    di_p  = 100*dmp.ewm(com=p-1,min_periods=p).mean()/atr_v
    di_n  = 100*dmn.ewm(com=p-1,min_periods=p).mean()/atr_v
    dx    = 100*(di_p-di_n).abs()/(di_p+di_n+1e-10)
    return dx.ewm(com=p-1,min_periods=p).mean(), di_p, di_n

def _obv(c, v): return (np.sign(c.diff()).fillna(0)*v).cumsum()
def _rvol(v, p=20): return v/v.rolling(p).mean()
def _avwap(h, l, c, v):
    tp = (h+l+c)/3; return (tp*v).cumsum()/v.cumsum()

def compute_indicators_last(df):
    c,h,l,v       = df.close, df.high, df.low, df.volume
    bb_u,bb_m,bb_lo,bb_w = _bb(c)
    m,ms,mh       = _macd(c)
    sk,sd         = _stoch(h,l,c)
    adx_v,dip,din = _adx(h,l,c)
    return {
        "rsi14":        _r(_rsi(c,14).iloc[-1]),
        "rsi9":         _r(_rsi(c, 9).iloc[-1]),
        "ema9":         _r(_ema(c, 9).iloc[-1]),
        "ema21":        _r(_ema(c,21).iloc[-1]),
        "ema50":        _r(_ema(c,50).iloc[-1]),
        "sma200":       _r(c.rolling(200).mean().iloc[-1]),
        "macd":         _r(m.iloc[-1],4),
        "macd_signal":  _r(ms.iloc[-1],4),
        "macd_hist":    _r(mh.iloc[-1],4),
        "bb_upper":     _r(bb_u.iloc[-1]),
        "bb_mid":       _r(bb_m.iloc[-1]),
        "bb_lower":     _r(bb_lo.iloc[-1]),
        "bb_width_pct": _r(bb_w.iloc[-1],2),
        "atr14":        _r(_atr(h,l,c).iloc[-1]),
        "stoch_k":      _r(sk.iloc[-1],2),
        "stoch_d":      _r(sd.iloc[-1],2),
        "adx":          _r(adx_v.iloc[-1],2),
        "di_plus":      _r(dip.iloc[-1],2),
        "di_minus":     _r(din.iloc[-1],2),
        "obv":          _r(_obv(c,v).iloc[-1],0),
        "rvol":         _r(_rvol(v).iloc[-1],2),
        "avwap":        _r(_avwap(h,l,c,v).iloc[-1]),
    }

# ─── VRVP ────────────────────────────────────────────────────────────────────
def compute_vrvp(df, n_bins=50):
    lo, hi = df.low.min(), df.high.max()
    if hi == lo:
        return {"poc":_r(lo),"vah":_r(hi),"val":_r(lo),"hvns":[],"lvns":[]}
    bins  = np.linspace(lo, hi, n_bins+1)
    bctrs = (bins[:-1]+bins[1:])/2
    vp    = np.zeros(n_bins)
    for _, row in df.iterrows():
        rng = row.high - row.low
        if rng == 0:
            i = min(max(np.searchsorted(bins,row.close,"right")-1,0),n_bins-1)
            vp[i] += row.volume; continue
        for b in range(n_bins):
            ov = max(0, min(row.high,bins[b+1]) - max(row.low,bins[b]))
            if ov > 0: vp[b] += row.volume*(ov/rng)
    poc_i = int(np.argmax(vp)); poc = bctrs[poc_i]
    tgt   = vp.sum()*0.70
    hi_i, lo_i, va_v = poc_i, poc_i, vp[poc_i]
    while va_v < tgt:
        eu = hi_i+1 < n_bins; ed = lo_i-1 >= 0
        if not eu and not ed: break
        vu = vp[hi_i+1] if eu else -1
        vd = vp[lo_i-1] if ed else -1
        if vu >= vd: hi_i+=1; va_v+=vp[hi_i]
        else: lo_i-=1; va_v+=vp[lo_i]
    hvn = sorted([i for i in range(1,n_bins-1) if vp[i]>vp[i-1] and vp[i]>vp[i+1]],
                 key=lambda i:-vp[i])[:5]
    lvn = sorted([i for i in range(1,n_bins-1) if vp[i]<vp[i-1] and vp[i]<vp[i+1]],
                 key=lambda i: vp[i])[:5]
    return {"poc":_r(poc),"vah":_r(bctrs[hi_i]),"val":_r(bctrs[lo_i]),
            "hvns":[_r(bctrs[i]) for i in hvn],
            "lvns":[_r(bctrs[i]) for i in lvn]}

# ─── SMC ─────────────────────────────────────────────────────────────────────
def detect_swings(df, lb=3):
    sh, sl = [], []
    n = len(df)
    for i in range(lb, n-lb):
        h = float(df.high.iloc[i]); l = float(df.low.iloc[i])
        ts = int(df.index[i].timestamp())
        if all(h >= float(df.high.iloc[i+k]) for k in range(-lb,lb+1) if k!=0):
            sh.append({"price":h,"index":i,"time":ts})
        if all(l <= float(df.low.iloc[i+k]) for k in range(-lb,lb+1) if k!=0):
            sl.append({"price":l,"index":i,"time":ts})
    return sh, sl

def detect_structure(sh, sl):
    if len(sh)<2 or len(sl)<2: return "undefined"
    sp = [s["price"] for s in sh[-3:]]; lp = [s["price"] for s in sl[-3:]]
    hh = len(sp)>=2 and all(sp[i]>sp[i-1] for i in range(1,len(sp)))
    hl = len(lp)>=2 and all(lp[i]>lp[i-1] for i in range(1,len(lp)))
    lh = len(sp)>=2 and all(sp[i]<sp[i-1] for i in range(1,len(sp)))
    ll = len(lp)>=2 and all(lp[i]<lp[i-1] for i in range(1,len(lp)))
    if hh and hl: return "bullish"
    if lh and ll: return "bearish"
    if hh and ll: return "distribution"
    if lh and hl: return "accumulation"
    return "ranging"

def detect_bos_choch(df, sh, sl, structure):
    bos = choch = None
    if not sh or not sl: return bos, choch
    last_close = float(df.close.iloc[-1])
    if sh and last_close > sh[-1]["price"]:
        lv  = sh[-1]["price"]; bos = {"direction":"bullish","level":lv,"confirmed":True}
        if "bearish" in structure: choch = {"direction":"bullish","level":lv,"confirmed":True}
    elif sl and last_close < sl[-1]["price"]:
        lv  = sl[-1]["price"]; bos = {"direction":"bearish","level":lv,"confirmed":True}
        if "bullish" in structure: choch = {"direction":"bearish","level":lv,"confirmed":True}
    return bos, choch

def detect_fvgs(df, min_gap_pct=0.01, keep=6):
    fvgs = []; n = len(df)
    for i in range(2, n):
        h_pm2=float(df.high.iloc[i-2]); l_pm2=float(df.low.iloc[i-2])
        h_cur=float(df.high.iloc[i]);   l_cur=float(df.low.iloc[i])
        ts=int(df.index[i].timestamp()); mid_p=float(df.close.iloc[i])
        if mid_p==0: continue
        if l_cur > h_pm2:
            gap=round(l_cur-h_pm2,4)
            if gap/mid_p >= min_gap_pct/100:
                filled=bool((df.low.iloc[i+1:]<l_cur).any())
                fvgs.append({"type":"bullish","high":_r(l_cur,4),"low":_r(h_pm2,4),
                             "mid":_r((l_cur+h_pm2)/2,4),"time":ts,"filled":filled,"gap":_r(gap,4)})
        elif h_cur < l_pm2:
            gap=round(l_pm2-h_cur,4)
            if gap/mid_p >= min_gap_pct/100:
                filled=bool((df.high.iloc[i+1:]>h_cur).any())
                fvgs.append({"type":"bearish","high":_r(l_pm2,4),"low":_r(h_cur,4),
                             "mid":_r((l_pm2+h_cur)/2,4),"time":ts,"filled":filled,"gap":_r(gap,4)})
    return [f for f in fvgs if not f["filled"]][-keep:]

def detect_obs(df, keep=10):
    n=len(df); atr_v=float(_atr(df.high,df.low,df.close).iloc[-1])
    if atr_v==0: atr_v=1
    obs=[]; sub=df.iloc[max(0,n-300):]; ns=len(sub)
    for i in range(10, ns):
        c=sub.iloc[i]; body=abs(float(c.close)-float(c.open))
        if body < atr_v*0.5: continue
        is_bull = c.close > c.open
        for j in range(i-1, max(0,i-12), -1):
            p=sub.iloc[j]; is_opp=(p.close<p.open) if is_bull else (p.close>p.open)
            if not is_opp: continue
            ob_h=float(p.high); ob_l=float(p.low); fut=sub.iloc[j+1:]
            mit_b=bool((fut.low<=ob_h).any()) if is_bull else False
            mit_e=bool((fut.high>=ob_l).any()) if not is_bull else False
            mitigated=mit_b or mit_e
            brkr=(bool((fut.low<ob_l).any()) if is_bull else bool((fut.high>ob_h).any())) if mitigated else False
            pb=abs(float(p.close)-float(p.open))
            q="high" if pb>atr_v else ("medium" if pb>atr_v*0.5 else "low")
            obs.append({"type":"bullish" if is_bull else "bearish",
                        "high":_r(ob_h,4),"low":_r(ob_l,4),"price":_r((ob_h+ob_l)/2,4),
                        "quality":q,"isBreaker":brkr}); break
    unique, seen = [], set()
    for ob in reversed(obs):
        k=round(ob["price"],1)
        if k not in seen: seen.add(k); unique.append(ob)
        if len(unique)>=keep: break
    return list(reversed(unique))

def detect_idm(sh, sl, bos):
    idm=[]
    if bos:
        if bos["direction"]=="bullish":
            for s in sh[-3:]:
                if s["price"]<bos["level"]: idm.append({"type":"bearish_idm","level":s["price"],"time":s["time"]})
            for s in sl[-2:]: idm.append({"type":"bullish_idm","level":s["price"],"time":s["time"]})
        else:
            for s in sl[-3:]:
                if s["price"]>bos["level"]: idm.append({"type":"bullish_idm","level":s["price"],"time":s["time"]})
            for s in sh[-2:]: idm.append({"type":"bearish_idm","level":s["price"],"time":s["time"]})
    else:
        if sh: idm.append({"type":"bearish_idm","level":sh[-1]["price"],"time":sh[-1]["time"]})
        if sl: idm.append({"type":"bullish_idm","level":sl[-1]["price"],"time":sl[-1]["time"]})
    return idm[-3:]

def compute_smc(df, tf):
    lb=TF_LB.get(tf,3); sh,sl=detect_swings(df,lb)
    structure=detect_structure(sh,sl); bos,chch=detect_bos_choch(df,sh,sl,structure)
    return {"structure":structure,"swingHighs":sh,"swingLows":sl,
            "orderBlocks":detect_obs(df),"fvgs":detect_fvgs(df),
            "idm":detect_idm(sh,sl,bos),"bos":bos,"choch":chch}

# ─── ORQUESTRADOR ────────────────────────────────────────────────────────────
def fetch_ohlcv(symbol, tf, asset_type, tf_config, log_cb=None, bybit_cat="linear"):
    n = tf_config[tf]["min"]
    if asset_type == "crypto":
        try:
            df = fetch_binance(symbol, tf, n, tf_config); src = "binance"
            _log(f"  ✅ Binance: {len(df)} candles", log_cb)
        except Exception as e:
            _log(f"  ⚠️  Binance ({e}) → Bybit...", log_cb)
            try:
                df = fetch_bybit(symbol, tf, n, tf_config, bybit_cat); src = "bybit"
                _log(f"  ✅ Bybit: {len(df)} candles", log_cb)
            except Exception as e2:
                _log(f"  ❌ Bybit: {e2}", log_cb); return pd.DataFrame(), "error"
    else:
        try:
            df = fetch_twelvedata(symbol, tf, n, tf_config); src = "twelvedata"
            _log(f"  ✅ TwelveData: {len(df)} candles", log_cb)
        except Exception as e:
            _log(f"  ❌ TwelveData: {e}", log_cb); return pd.DataFrame(), "error"
    return df, src

def build_ohlcv_row(df, symbol, tf_key, source, tf_config):
    df2 = df.copy().reset_index()
    df2["asset"]    = symbol
    df2["tf"]       = tf_config[tf_key]["out"]
    df2["time_iso"] = df2["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    df2["unix"]     = df2["timestamp"].astype(np.int64)//1_000_000_000
    df2["source"]   = source
    return df2[["asset","tf","time_iso","unix","open","high","low","close","volume","source"]]

def run_all(symbol, contract, asset_type, coinalyze_sym, tf_config, log_cb=None, bybit_cat="linear"):
    is_perp = contract == "perp"
    is_spot = contract == "spot"
    all_ohlcv_frames=[]; ind_data={}; warnings_out={}
    cvd_df=fund_df=lsr_df=pd.DataFrame()

    if is_spot and asset_type == "crypto":
        _log("\n[CVD Spot — Binance taker buy]", log_cb)
        try:
            cvd_df = fetch_cvd_spot_binance(symbol, "1h", days=90, tf_config=tf_config)
            _log(f"  ✅ CVD Spot: {len(cvd_df)} rows", log_cb)
        except Exception as e:
            _log(f"  ⚠️  CVD Spot: {e}", log_cb)

    if is_perp and coinalyze_sym:
        _log("\n[Coinalyze — Derivativos]", log_cb)
        try:
            cvd_df = fetch_cvd_perp_coinalyze(coinalyze_sym, "1h")
            _log(f"  ✅ CVD Perp: {len(cvd_df)} rows", log_cb)
        except Exception as e: _log(f"  ⚠️  CVD: {e}", log_cb)
        try:
            fund_df = fetch_funding(coinalyze_sym)
            _log(f"  ✅ Funding: {len(fund_df)} rows", log_cb)
        except Exception as e: _log(f"  ⚠️  Funding: {e}", log_cb)
        try:
            lsr_df = fetch_lsr(coinalyze_sym,"1h")
            _log(f"  ✅ LSR: {len(lsr_df)} rows", log_cb)
        except Exception as e: _log(f"  ⚠️  LSR: {e}", log_cb)

    for tf in PROTOCOL_TFS:
        tf_out = tf_config[tf]["out"]
        _log(f"\n[{tf_out}]", log_cb)
        df, src = fetch_ohlcv(symbol, tf, asset_type, tf_config, log_cb, bybit_cat)
        if df.empty:
            warnings_out[tf_out]={"got":0,"want":tf_config[tf]["min"],"status":"error"}; continue
        got=len(df); want=tf_config[tf]["min"]
        if got < want*0.9:
            warnings_out[tf_out]={"got":got,"want":want,"status":"insufficient"}
            _log(f"  ⚠️  {got}/{want} candles — cobertura insuficiente", log_cb)
        all_ohlcv_frames.append(build_ohlcv_row(df, symbol, tf, src, tf_config))
        inds=compute_indicators_last(df); vrvp=compute_vrvp(df); smc=compute_smc(df,tf)
        tf_block={"tf":tf_out,"candle_count":got,
                  "last_close":_r(float(df.close.iloc[-1]),4),
                  "indicators":inds,"vrvp":vrvp,"smc":smc}
        if not cvd_df.empty:
            lv=cvd_df.iloc[-1]
            tf_block["cvd"]={"source":"binance_spot" if is_spot else "coinalyze_perp",
                             "cvd_last":_r(float(lv.cvd),2),"delta_last":_r(float(lv.delta),2),
                             "buy_vol_last":_r(float(lv.buy_vol),2),"sell_vol_last":_r(float(lv.sell_vol),2)}
        if is_perp and coinalyze_sym:
            deriv={"source":"coinalyze","symbol":coinalyze_sym}
            if not fund_df.empty: deriv["funding_rate_last"]=_r(float(fund_df.funding_rate.iloc[-1]),6)
            if not lsr_df.empty and "ls_ratio" in lsr_df.columns:
                deriv["ls_ratio_last"]=_r(float(lsr_df.ls_ratio.iloc[-1]),4)
            tf_block["derivativos"]=deriv
        ind_data[tf_out]=tf_block

    ohlcv_df = pd.concat(all_ohlcv_frames, ignore_index=True) if all_ohlcv_frames else pd.DataFrame()
    indicators_json = {
        "ticker":       symbol,
        "type":         asset_type,
        "contract":     contract,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "indicators":   ind_data,
        "warnings":     warnings_out or {},
    }
    return ohlcv_df, indicators_json, cvd_df, fund_df, lsr_df

def save_outputs(symbol, contract, ohlcv_df, ind_json, cvd_df, fund_df, lsr_df,
                 output_dir, log_cb=None):
    tag       = f"{symbol}_{contract.upper()}"
    asset_dir = os.path.join(output_dir, tag)
    os.makedirs(asset_dir, exist_ok=True)
    saved_files = []

    def _save(df, name, label):
        if df is None or df.empty: return
        p = os.path.join(asset_dir, name)
        df.to_csv(p, index=True)
        _log(f"  💾 {label} → {name}", log_cb)
        saved_files.append(p)

    if not ohlcv_df.empty:
        p = os.path.join(asset_dir, f"{tag}_ohlcv.csv")
        ohlcv_df.to_csv(p, index=False)
        _log(f"  💾 OHLCV ({len(ohlcv_df)} rows) → {tag}_ohlcv.csv", log_cb)
        saved_files.append(p)

    p = os.path.join(asset_dir, f"{tag}_indicators.json")
    with open(p,"w") as f:
        json.dump(ind_json, f, indent=2, ensure_ascii=False, default=str)
    _log(f"  💾 Indicators → {tag}_indicators.json", log_cb)
    saved_files.append(p)

    _save(cvd_df,  f"{tag}_cvd.csv",     "CVD")
    if contract == "perp":
        _save(fund_df, f"{tag}_funding.csv", "Funding Rate")
        _save(lsr_df,  f"{tag}_lsr.csv",     "Long/Short Ratio")

    _log(f"\n✅ Arquivos salvos em: {asset_dir}", log_cb)
    return saved_files

def run_extraction(symbol, contract, asset_type, tf_config,
                   output_dir, log_cb=None):
    """Ponto de entrada único para a UI Kivy e para o CLI."""
    coinalyze_sym = ""
    if asset_type == "crypto" and contract == "perp":
        coinalyze_sym = COINALYZE_MAP.get((symbol, contract), "")

    _log(f"\n{'='*50}", log_cb)
    _log(f"Buscando: {symbol} {contract.upper()} [{asset_type}]", log_cb)
    mins = {tf: tf_config[tf]["min"] for tf in PROTOCOL_TFS}
    _log(f"Candles: 1W={mins['1w']} · 1D={mins['1d']} · 4H={mins['4h']} · 1H={mins['1h']}", log_cb)
    _log(f"{'='*50}", log_cb)

    ohlcv_df, ind_json, cvd_df, fund_df, lsr_df = run_all(
        symbol, contract, asset_type, coinalyze_sym or None, tf_config, log_cb)

    _log(f"\n{'='*50}", log_cb)
    _log("SALVANDO", log_cb)
    _log(f"{'='*50}", log_cb)

    files = save_outputs(symbol, contract, ohlcv_df, ind_json,
                         cvd_df, fund_df, lsr_df, output_dir, log_cb)
    return files
