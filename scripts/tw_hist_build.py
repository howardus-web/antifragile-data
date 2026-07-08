import requests, time, json, datetime, calendar, os, sys

H = {'User-Agent':'Mozilla/5.0'}
EXCL = {'2412','3045','4904'}
IND = {'24':'半導體','25':'電腦週邊','26':'光電','27':'通信網路','28':'電子零組件',
       '29':'電子通路','30':'資訊服務','31':'其他電子'}
OUT = 'tw_universe_history.json'
hist = json.load(open(OUT)) if os.path.exists(OUT) else {}

def get(url, **params):
    for attempt in range(6):
        try:
            r = requests.get(url, params=dict(response='json', **params), headers=H, timeout=30)
            j = r.json(); time.sleep(3.2); return j
        except Exception:
            wait = 30 * (attempt+1)
            print(f"  [backoff {wait}s]", flush=True); time.sleep(wait)
    raise RuntimeError('連續重試失敗')

def find_trading_day(y, m):
    d = datetime.date(y, m, calendar.monthrange(y,m)[1])
    for _ in range(8):
        if d.weekday() < 5:
            ds = d.strftime('%Y%m%d')
            j = get('https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX', date=ds, type='24')
            if j.get('stat')=='OK' and any(len(t.get('data',[]))>0 for t in j.get('tables',[])):
                return ds
        d -= datetime.timedelta(days=1)
    return None

def fetch_shares(ds):
    d = datetime.datetime.strptime(ds,'%Y%m%d').date()
    for _ in range(5):
        dd = d.strftime('%Y%m%d')
        j = get('https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS', date=dd, selectType='ALLBUT0999')
        tb = j.get('tables',[j])[0] if 'tables' in j else j
        data = tb.get('data') or j.get('data')
        if data:
            sh, nm = {}, {}
            for row in data:
                c = row[0].strip()
                try: sh[c] = int(row[3].replace(',','')); nm[c] = row[1].strip()
                except (ValueError, IndexError): pass
            return sh, nm, dd
        d -= datetime.timedelta(days=1)
        while d.weekday() >= 5: d -= datetime.timedelta(days=1)
    return None, None, None

def build(ds):
    closes, ind_of = {}, {}
    for code in IND:
        j = get('https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX', date=ds, type=code)
        for t in j.get('tables', []):
            if '每日收盤行情' in t.get('title','') or '收盤行情' in t.get('title',''):
                for row in t['data']:
                    c = row[0].strip()
                    try: closes[c] = float(row[8].replace(',','')); ind_of[c] = code
                    except (ValueError, IndexError): pass
    if not closes: return None, 'MI_INDEX 產業表全空'
    shares, names, sdate = fetch_shares(ds)
    if not shares: return None, '股數缺漏'
    cap = {c: closes[c]*shares[c] for c in closes if c in shares and c not in EXCL and shares[c]>0}
    top = sorted(cap, key=cap.get, reverse=True)[:20]
    if len(top) < 20: return None, f'候選不足({len(top)})'
    snap = [dict(rank=i+1, code=c, name=names.get(c,''), industry=IND[ind_of[c]],
                 close=closes[c], shares=shares[c], mktcap=round(cap[c])) for i,c in enumerate(top)]
    return dict(shares_date=sdate, top20=snap), None

dates = [(y,m) for y in range(2007,2027) for m in (2,8) if (2007,8) <= (y,m) <= (2026,2)]
processed = 0
for y,m in dates:
    if processed >= 4: print("CHUNK_DONE", flush=True); break
    if any(k.startswith(f"{y}{m:02d}") for k in hist): continue
    ds = find_trading_day(y,m)
    if not ds: print(f"{y}-{m:02d} 無交易日", flush=True); continue
    snap, err = build(ds)
    if err: print(f"{ds} FAIL: {err}", flush=True); continue
    hist[ds] = snap; processed += 1
    json.dump(hist, open(OUT,'w'), ensure_ascii=False, indent=1)
    print(f"{ds} OK  #1 {snap['top20'][0]['name']}  #20 {snap['top20'][19]['code']} {snap['top20'][19]['name']}", flush=True)
print('DONE', len(hist), flush=True)
