import  pandas as pd
import yfinance as yf
from datetime import datetime
from config import UNIVERSE

def get_prices(ticker: str, period = '5y', interval = '1d') -> pd.DataFrame:
    df = yf.download(ticker, period = period, interval = interval, auto_adjust=False, progress = False)
    if df.empty:
        return pd.DataFrame()
    
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]


    df = df.reset_index().rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })
    df['ticker'] = ticker
    numeric_cols = ["open", "high", "low", "close", "adj_close", "volume"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    return df[["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]]



def run_prices_ingestion():
    dataframes = []
    for t in UNIVERSE:
        try:
            df = get_prices(t)
            if not df.empty:
                dataframes.append(df)
        except Exception as e:
            print(f"[WARN] {t}: {e}")
    if not dataframes:
        print("No data fetched.")
        return
    all_prices = pd.concat(dataframes, ignore_index = True)
    all_prices["ingested_at"] = pd.Timestamp.utcnow()
    out_path = "data_raw/prices_daily.csv"
    all_prices.to_csv(out_path, index = False)
    print(f"Saved: {out_path} ({len(all_prices):,} rows)")

if __name__== "__main__":
    run_prices_ingestion()
