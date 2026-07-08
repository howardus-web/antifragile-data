import requests, time, json, os, sys

H = {'User-Agent':'Mozilla/5.0'}
SPANS = {'2311':('2006-07','2018-05'), '2325':('2006-07','2018-05'),
         '3474':('2006-07','2017-01'), '3009':('2006-07','2010-04'),
         '3697':('2010-12','2014-03')}
STATE = 'delisted_px_state.json'
state = json.load(open(STATE)) if os.path.exists(STATE) else {}
BUDGET = 68

def get(url, **params):
    for a in range(6):
        try:
            r = requests.get(url, params=dict(response='json', **params), headers=H, timeout=30)
            j = r.json(); time.sleep(3.2); return j
        except Exception:
            w = 40*(a+1); print(f'[backoff {w}s]', flush=True); time.sleep(w)
    raise RuntimeError('fail')

def months(a, b):
    y,m = map(int, a.split('-')); Y,M = map(int, b.split('-'))
    out = []
    while (y,m) <= (Y,M):
        out.append(f'{y}{m:02d}'); m += 1
        if m == 13: y, m = y+1, 1
    return out

used = 0
for code, (a,b) in SPANS.items():
    px = state.setdefault(code, {})
    done = set(k[:6] for k in px)  # 已有該月資料
    fetched_months = state.setdefault('_done_months', {}).setdefault(code, [])
    for ym in months(a,b):
        if ym in fetched_months: continue
        if used >= BUDGET: print('CHUNK_DONE', flush=True); json.dump(state, open(STATE,'w')); sys.exit(0)
        j = get('https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY', date=ym+'01', stockNo=code)
        used += 1
        data = j.get('data') or []
        n = 0
        for row in data:
            p = str(row[0]).strip().split('/')
            try:
                date = f"{int(p[0])+1911}-{int(p[1]):02d}-{int(p[2]):02d}"
                close = float(str(row[6]).replace(',',''))
                px[date] = close; n += 1
            except (ValueError, IndexError): pass
        fetched_months.append(ym)
        json.dump(state, open(STATE,'w'))
        print(f"{code} {ym}: {n} 天", flush=True)
json.dump(state, open(STATE,'w'))
print('ALL_DONE', flush=True)
