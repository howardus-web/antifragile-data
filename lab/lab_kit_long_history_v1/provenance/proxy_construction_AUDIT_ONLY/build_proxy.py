import pandas as pd, numpy as np, io, json
from pathlib import Path
R = Path('/home/claude/corpus/repo'); K = R/'lab/lab_kit_2026-07-07'; W = Path('/home/claude/btal_proxy')
def rd(p):
    df = pd.read_csv(p, skiprows=3, header=None, names=['d','v'])
    s = pd.Series(pd.to_numeric(df.v, errors='coerce').values, index=pd.to_datetime(df.d)).dropna()
    return s[~s.index.duplicated()].sort_index()
def mret(px):
    m = px.groupby(px.index.to_period('M')).last(); return m.pct_change().dropna()
# --- real BTAL (monthly, first full month 2011-10)
btal_px = rd(R/'us/BTAL.csv'); btal = mret(btal_px)
# --- FF daily: Mkt-RF, RF -> monthly
ff = pd.read_csv(W/'ff/F-F_Research_Data_Factors_daily.csv', skiprows=4, skipfooter=2, engine='python')
ff.columns = ['d','mktrf','smb','hml','rf']; ff = ff[ff.d.astype(str).str.match(r'^\d{8}$')]
ff.index = pd.to_datetime(ff.d.astype(str)); ff = ff[['mktrf','smb','hml','rf']].astype(float)/100
per = ff.index.to_period('M')
rf = (1+ff.rf).groupby(per).prod()-1
mkt = (1+ff.mktrf+ff.rf).groupby(per).prod()-1
mktrf = mkt - rf
# --- AQR BAB monthly
bab = pd.read_csv(K/'data/aqr/bab_usa_monthly.csv', parse_dates=['date']).set_index('date')['usa_bab']; bab.index = bab.index.to_period('M')
# --- French beta-sorted portfolios (monthly, VW and EW)
txt = (W/'ff/Portfolios_Formed_on_BETA.csv').read_text().splitlines()
def block(title):
    i = [k for k,l in enumerate(txt) if l.strip().startswith(title)][0]
    rows = []
    for l in txt[i+2:]:
        if not l.strip() or not l.strip()[0].isdigit(): break
        rows.append(l)
    df = pd.read_csv(io.StringIO('\n'.join([txt[i+1]]+rows)), index_col=0)
    df.columns = [c.strip() for c in df.columns]; df.index = pd.PeriodIndex(df.index.astype(str), freq='M')
    return df.astype(float)/100
vw = block('Value Weighted Returns -- Monthly'); ew = block('Equal Weighted Returns -- Monthly')
lmh_vw = vw['Lo 20']-vw['Hi 20']; lmh_ew = ew['Lo 20']-ew['Hi 20']; lmh_vw10 = vw['Lo 10']-vw['Hi 10']
D = pd.concat({'btal':btal,'rf':rf,'mktrf':mktrf,'mkt':mkt,'bab':bab,'lmh_vw':lmh_vw,'lmh_ew':lmh_ew,'lmh_vw10':lmh_vw10}, axis=1)
L = D.dropna()                       # live overlap
print('live overlap', L.index[0], L.index[-1], len(L))
y = L.btal - L.rf
def ols(X, y):
    X1 = np.column_stack([np.ones(len(X)), X]); b = np.linalg.lstsq(X1, y, rcond=None)[0]; return b
def fit_eval(cols, name, intercept=True):
    b = ols(L[cols].values, y.values)
    pred = L.rf + b[0] + L[cols].values @ b[1:]
    return name, b, pred
cands = {}
cands['P0 BAB only (現行)'] = (None, L.bab)                              # as used in kit: BTAL return := BAB return
cands['S1 rf + (Lo20-Hi20) VW 無擬合'] = (None, L.rf + L.lmh_vw)
cands['S2 rf + (Lo20-Hi20) EW 無擬合'] = (None, L.rf + L.lmh_ew)
for cols, name in [(['bab','mktrf'],'F1 rf+a+b*BAB+c*MktRF'), (['lmh_vw'],'F2 rf+a+k*LMH_VW'), (['lmh_vw','bab'],'F3 rf+a+k*LMH_VW+b*BAB'), (['lmh_vw','bab','mktrf'],'F4 rf+a+k*LMH_VW+b*BAB+c*MktRF')]:
    n, b, pred = fit_eval(cols, name); cands[name] = (dict(zip(['a']+cols, b.round(4))), pred)

def sig(r):   # monthly analogue of RS signal: avg of 6m and 12m log return
    lr = np.log1p(r); return (lr.rolling(6).sum() + lr.rolling(12).sum())/2
def stats(pred):
    e = pred - L.btal; s_a, s_p = sig(L.btal), sig(pred); ok = s_a.notna()
    bm = lambda s: np.polyfit(L.mkt.values, s.values, 1)[0]
    return {'corr': np.corrcoef(pred, L.btal)[0,1], 'R2': 1-e.var()/L.btal.var(), 'TE_ann': e.std()*12**.5, 'bias_ann': e.mean()*12,
            'vol_ann': pred.std()*12**.5, 'beta_mkt': bm(pred), 'sig_corr': np.corrcoef(s_a[ok], s_p[ok])[0,1], 'sig_mae': (s_a-s_p)[ok].abs().mean()}
rows = {k: stats(v[1]) for k, v in cands.items()}
rows['(真 BTAL)'] = {'corr':1,'R2':1,'TE_ann':0,'bias_ann':0,'vol_ann':L.btal.std()*12**.5,'beta_mkt':np.polyfit(L.mkt.values,L.btal.values,1)[0],'sig_corr':1,'sig_mae':0}
T = pd.DataFrame(rows).T; pd.set_option('display.width',200); print(T.round(3).to_string())
for k,v in cands.items():
    if v[0]: print(k, v[0])
# --- out-of-sample check for fitted candidates: fit on one half, evaluate on the other
h = len(L)//2; halves = [(L.index[:h], L.index[h:]), (L.index[h:], L.index[:h])]
print('\nOOS (fit one half -> evaluate other half): corr / R2 / TE / bias')
for cols, name in [(['bab','mktrf'],'F1'), (['lmh_vw'],'F2'), (['lmh_vw','bab'],'F3'), (['lmh_vw','bab','mktrf'],'F4')]:
    out = []
    for tr, te in halves:
        b = ols(L.loc[tr, cols].values, (L.btal-L.rf).loc[tr].values)
        p = L.rf.loc[te] + b[0] + L.loc[te, cols].values @ b[1:]; e = p - L.btal.loc[te]
        out.append((round(np.corrcoef(p, L.btal.loc[te])[0,1],3), round(1-e.var()/L.btal.loc[te].var(),3), round(e.std()*12**.5,4), round(e.mean()*12,4), b.round(3).tolist()))
    print(name, out)
for nm, s in [('P0', L.bab), ('S1', L.rf+L.lmh_vw)]:
    out = []
    for tr, te in halves:
        p = s.loc[te]; e = p - L.btal.loc[te]; out.append((round(np.corrcoef(p, L.btal.loc[te])[0,1],3), round(1-e.var()/L.btal.loc[te].var(),3), round(e.std()*12**.5,4), round(e.mean()*12,4)))
    print(nm, out)
D.to_pickle(W/'D.pkl'); L.to_pickle(W/'L.pkl')
json.dump({k:(v[0] if v[0] is None else {a:float(b) for a,b in v[0].items()}) for k,v in cands.items()}, open(W/'coefs.json','w'), ensure_ascii=False, indent=1)
