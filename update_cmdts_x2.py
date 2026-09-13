"""
update_cmdts_x2.py — Staged acquisition script for the CMD-TS 32-market
sensor universe.

STATUS: staged (Phase 2). NOT wired into any scheduled workflow. Intended
to be added to the antifragile-data operational repository as a sibling
script to the existing update.py, and invoked by whatever process later
runs that script (an operational decision explicitly out of scope here --
see docs/acquisition_contract.md).

Structurally mirrors update.py::download_adj_close() exactly: same vendor
(yfinance), same auto_adjust=True convention, same 3-header-row CSV save
format. The only difference is the destination folder (x2_sensor/, not
us/) and the fixed 32-symbol roster, which is the exact recovered
canonical universe from CMD_TS_X2_CANONICAL_RESEARCH_SOURCE_v1 -- not
retyped from memory, reproduced from that package's own artifact.

Usage (staged / manual invocation only, not scheduled by this commit):
    python update_cmdts_x2.py [--repo-root PATH]
"""

import argparse
import os
import sys

import yfinance as yf

# Exact recovered canonical 32-market roster. Do not add, remove, or
# substitute any symbol here without a new WTC-level research decision --
# this list is frozen research identity, not an operational convenience list.
TICKERS_CMDTS_X2 = [
    "ES=F", "NQ=F", "YM=F", "NKD=F",             # Equity
    "ZT=F", "ZF=F", "ZN=F", "ZB=F",              # Rates
    "6E=F", "6J=F", "6B=F", "6C=F", "6A=F", "6S=F", "6N=F", "6M=F",  # FX
    "CL=F", "NG=F", "GC=F", "SI=F", "HG=F", "PL=F", "PA=F",          # Commodity (part 1)
    "ZC=F", "ZS=F", "ZW=F", "KC=F", "CC=F", "CT=F", "SB=F", "LE=F", "HE=F",  # Commodity (part 2)
]
assert len(TICKERS_CMDTS_X2) == 32, "staged roster must be exactly 32 symbols"


def _safe_filename(ticker: str) -> str:
    return ticker.replace("=", "_")


def download_adj_close_x2(ticker: str, folder: str) -> None:
    """Structurally identical to update.py::download_adj_close(), applied
    to the x2_sensor/ folder instead of us/. auto_adjust=True => Close is
    already adjusted; verified in docs/acquisition_contract.md that this
    is semantically identical to the research's own Close field for these
    (dividend/split-free) futures instruments."""
    print("Downloading", ticker)
    df = yf.download(ticker, start="2000-01-01", auto_adjust=True)
    if df.empty:
        print("WARNING: no data returned for", ticker)
        return
    df = df[["Close"]]
    df.rename(columns={"Close": "AdjClose"}, inplace=True)
    save_name = _safe_filename(ticker)
    out_path = os.path.join(folder, f"{save_name}.csv")
    df.to_csv(out_path)
    print("Saved:", out_path)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo-root", default=".",
                     help="Root of the antifragile-data repository checkout. "
                          "x2_sensor/ is created as a sibling of us/ and tw/.")
    args = ap.parse_args()

    folder = os.path.join(args.repo_root, "x2_sensor")
    os.makedirs(folder, exist_ok=True)

    failures = []
    for ticker in TICKERS_CMDTS_X2:
        try:
            download_adj_close_x2(ticker, folder)
        except Exception as e:  # pragma: no cover -- network/vendor failure path
            print(f"ERROR downloading {ticker}: {e}")
            failures.append(ticker)

    if failures:
        print(f"\n{len(failures)}/32 symbols failed to download: {failures}")
        sys.exit(1)
    print(f"\nAll {len(TICKERS_CMDTS_X2)} CMD-TS sensor symbols acquired successfully.")


if __name__ == "__main__":
    main()
