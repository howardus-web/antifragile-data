#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""verify_kit.py — standalone integrity check for lab_kit_long_history_v1 (stdlib only).

RESEARCH_ONLY · LAB_LONG_HISTORY · PROXY_CONTAINING · NOT_PRODUCTION_EQUIVALENT

Checks, without importing anything from the MAS corpus:
  1. every file listed in KIT_MANIFEST.json exists and has the recorded SHA-256;
  2. the payload identity (hash over the data/ file list) equals the manifest's own value
     and, if --expect is given, the identity registered in long_history_universe.REGISTERED_KITS;
  3. the 2011-10 true-BTAL splice: F1 / S3 rows before 2011-10 are labelled proxy, rows from
     2011-10 are labelled "real BTAL" and equal the true-BTAL column of btal_proxy_monthly_returns.csv;
  4. prints the common monthly support that the frozen series imply (derived, not hard-coded).

It validates frozen bytes. It does NOT fit F1, rebuild S3, download or repair anything.
The loader / constructor used by the Research Harness is long_history_universe.py in the MAS corpus.

usage:  python3 verify_kit.py [--kit <dir>] [--expect <payload_identity_sha256>]
"""
import argparse, csv, hashlib, json, sys
from pathlib import Path

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def month_of(s): return s[:7]

def last_month_series(path):          # daily Date,AdjClose (3 header rows) -> {YYYY-MM: last close}
    out = {}
    with open(path, newline='') as f:
        for i, row in enumerate(csv.reader(f)):
            if i < 3 or len(row) < 2 or not row[1]: continue
            out[month_of(row[0])] = float(row[1])
    return out

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--kit', default=str(Path(__file__).resolve().parent)); ap.add_argument('--expect')
    a = ap.parse_args(); kit = Path(a.kit); man = json.loads((kit / 'KIT_MANIFEST.json').read_text(encoding='utf-8'))
    bad = []
    for rel, meta in man['files'].items():
        p = kit / rel
        if not p.is_file(): bad.append(f'MISSING {rel}')
        elif sha(p) != meta['sha256']: bad.append(f'HASH MISMATCH {rel}')
    payload = sorted((k, v['sha256']) for k, v in man['files'].items() if v.get('class') == 'data')
    identity = hashlib.sha256('\n'.join(f'{k}  {h}' for k, h in payload).encode()).hexdigest()
    if identity != man['payload_identity_sha256']: bad.append('payload identity differs from manifest')
    if a.expect and identity != a.expect: bad.append('payload identity differs from --expect')
    roles = man['roles']; true_btal = {}
    with open(kit / roles['BTAL']['true_btal_overlap_file'], newline='') as f:
        rd = csv.reader(f); head = next(rd); j = head.index(roles['BTAL']['true_btal_overlap_column'])
        for row in rd:
            if row and row[j] != '': true_btal[row[0]] = float(row[j])
    splice = roles['BTAL']['splice_month']; btal = {}
    for v in man['btal_variants']:
        ret = {}; worst = 0.0; n = 0
        with open(kit / roles['BTAL']['variants'][v]['file'], newline='') as f:
            rd = csv.reader(f); next(rd)
            for m, r, src in rd:
                ret[m] = float(r)
                if m < splice and src != f'proxy {v}': bad.append(f'{v}: {m} before splice not labelled proxy')
                if m >= splice:
                    if src != 'real BTAL': bad.append(f'{v}: {m} from splice not labelled real BTAL')
                    if m in true_btal: worst = max(worst, abs(ret[m] - true_btal[m])); n += 1
        if worst > 1e-12: bad.append(f'{v}: differs from true BTAL after splice by {worst}')
        btal[v] = ret
        print(f'splice {v}: first real-BTAL month {min(m for m in ret if m >= splice)}; {n} true-BTAL months checked; max |diff| {worst:.1e}')
    px = {r: last_month_series(kit / roles[r]['file']) for r in ('QQQ', 'TLT', 'GLD', 'XLE')}
    mf = {}
    with open(kit / roles['MF']['file'], newline='') as f:
        rd = csv.reader(f); next(rd)
        for row in rd:
            if len(row) >= 2 and row[0] and row[1]: mf[month_of(row[0])] = float(row[1])
    for v in man['btal_variants']:
        months = set(mf) & set(btal[v])
        for r in px: months &= set(sorted(px[r])[1:])            # first month of a price series has no return
        ms = sorted(months); print(f'common monthly support {v}: {ms[0]} .. {ms[-1]} ({len(ms)} return months; first full 12M signal month = month 12, holding from month 13)')
    print('payload identity:', identity); print('labels:', ', '.join(man['labels']))
    if bad:
        print('FAIL'); [print('  ', b) for b in bad]; sys.exit(1)
    print(f"ALL CHECKS PASS — {len(man['files'])} files")

if __name__ == '__main__':
    main()
