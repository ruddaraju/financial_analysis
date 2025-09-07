import os
import requests
import pandas as pd
from typing import Dict, Any
from config import AV_API_KEY, AV_BASE, UNIVERSE
from utils import rate_limit_sleep


def _get_json(params: Dict[str, Any]) -> Dict[str,Any]:
    params['apikey'] = AV_API_KEY
    r = requests.get(AV_BASE, params=params, timeout = 30)
    r.raise_for_status()
    return r.json()

def fetch_overview(ticker: str) ->  pd.DataFrame:
    data = _get_json({'function': 'OVERVIEW', 'symbol': ticker})


    if not data or "Symbol" not in data:
        return pd.DataFrame()
    df = pd.DataFrame([data])
    df['ticker'] = ticker

    keep = [
        "ticker","Symbol","Name","Sector","Industry","Exchange","Currency",
        "MarketCapitalization","SharesOutstanding","PERatio","PriceToBookRatio",
        "DividendYield","EBITDA","52WeekHigh","52WeekLow","AnalystTargetPrice",
        "FiscalYearEnd","LatestQuarter"
    ]
    df = df[[c for c in keep if c in df.columns]]

    num_cols = ["MarketCapitalization","SharesOutstanding","PERatio","PriceToBookRatio",
                "DividendYield","EBITDA","52WeekHigh","52WeekLow","AnalystTargetPrice"]
    
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    return df

def _normalize_statement(data: Dict[str,Any],ticker: str, key: str) -> pd.DataFrame:
    raw = data.get(key, [])
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    df['ticker'] = ticker

    if "fiscalDateEnding" in df.columns:
        df["fiscalDateEnding"] = pd.to_datetime(df["fiscalDateEnding"], errors="coerce")
    for c in df.columns:
        if c not in ("ticker", "reportedCurrency", "fiscalDateEnding", "fiscalQuarterEnding"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def fetch_income_qtr(ticker: str) -> pd.DataFrame:
    data = _get_json({"function": "INCOME_STATEMENT", "symbol": ticker})
    return _normalize_statement(data, ticker, "quarterlyReports")

def fetch_balance_qtr(ticker: str) -> pd.DataFrame:
    data = _get_json({"function": "BALANCE_SHEET", "symbol": ticker})
    return _normalize_statement(data, ticker, "quarterlyReports")

def fetch_cashflow_qtr(ticker: str) -> pd.DataFrame:
    data = _get_json({"function": "CASH_FLOW", "symbol": ticker})
    return _normalize_statement(data, ticker, "quarterlyReports")


def run_fundamentals_ingestion():
    ov_frames, inc_frames, bal_frames, cf_frames = [], [], [], []
    for t in UNIVERSE:
        try:
            ov = fetch_overview(t)
            if not ov.empty: ov_frames.append(ov)
            rate_limit_sleep()

            inc = fetch_income_qtr(t)
            if not inc.empty: inc_frames.append(inc)
            rate_limit_sleep()

            bal = fetch_balance_qtr(t)
            if not bal.empty: ov_frames.append(bal)
            rate_limit_sleep()

            cf = fetch_cashflow_qtr(t)
            if not cf.empty: ov_frames.append(cf)
            rate_limit_sleep()
        except Exception as e:
            print(f"[WARN] {t}: {e}")

    if ov_frames:
        pd.concat(ov_frames, ignore_index=True).to_csv("../data_raw/company_overview.csv", index=False)
        print("Saved: data_raw/company_overview.csv")
    if inc_frames:
        pd.concat(inc_frames, ignore_index=True).to_csv("../data_raw/fund_income_qtr.csv", index=False)
        print("Saved: data_raw/fund_income_qtr.csv")
    if bal_frames:
        pd.concat(bal_frames, ignore_index=True).to_csv("../data_raw/fund_balance_qtr.csv", index=False)
        print("Saved: data_raw/fund_balance_qtr.csv")
    if cf_frames:
        pd.concat(cf_frames, ignore_index=True).to_csv("../data_raw/fund_cashflow_qtr.csv", index=False)
        print("Saved: data_raw/fund_cashflow_qtr.csv")

if __name__ == "__main__":

    all_data = []
    for ticker in UNIVERSE:
        df = fetch_overview(ticker)
        if not df.empty:
            all_data.append(df)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        os.makedirs("data_raw", exist_ok=True)

        final_df.to_csv("data_raw/fundamentals.csv", index=False)
        print("Data saved to data_raw/fundamentals.csv")
    else:
        print("No data fetched")
