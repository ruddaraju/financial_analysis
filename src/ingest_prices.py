import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from config import UNIVERSE

def get_prices(ticker: str, start = None, end = None, interval = '1d') -> pd.DataFrame:
    df = yf.download(ticker, start = start, end = end, interval = interval, auto_adjust=False, progress = False)

    if df.empty:
        return pd.DataFrame()


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
    out_path = "data_raw/prices_daily.csv"

    if os.path.exists(out_path):
        existing = pd.read_csv(out_path, parse_dates = ['date'])
    else:
        existing = pd.DataFrame()

    new_dataframes = []
    for t in UNIVERSE:
        try:
            if not existing.empty and t in existing['ticker'].unique():
                last_date = existing.loc[existing['ticker'] == t, "date"].max()
                start_date = pd.to_datetime(last_date) + pd.Timedelta(days = 1)
                today = pd.Timestamp.today().normalize()

                if start_date >= today:
                    print(f"[INFO] No new data available for {t} (already up to date).")
                    continue
            else:
                start_date = '2020_01_01'


            df = get_prices(t, start = start_date)
            if not df.empty:
                new_dataframes.append(df)
        except Exception as e:
            print(f"[WARN] {t}: {e}")
    if not new_dataframes:
        print("No data fetched.")
        return
    
    new_prices = pd.concat(new_dataframes, ignore_index = True)
    combined = (
        pd.concat([existing, new_prices], ignore_index = True)
        .drop_duplicates(subset = ['ticker','date'])
        .sort_values(['ticker', 'date'])
    )

    combined.to_csv(out_path, index = False)
    print(f'Updated: {out_path} ({len(combined):,} rows)')


if __name__== "__main__":
    run_prices_ingestion()
