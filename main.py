import os
import yfinance as yf
from datetime import datetime, timedelta
import pytz
import pandas as pd

# Base local storage
BASE_DATA_DIR = "Market_60"

# 60 companies: symbol ➝ (sector, company name)
symbol_map = {
    "^NSEI": ("Nifty", "Nifty 50"),
    "RELIANCE.NS": ("Nifty", "Reliance Industries"),
    "HDFCBANK.NS": ("Nifty", "HDFC Bank"),
    "ICICIBANK.NS": ("Nifty", "ICICI Bank"),
    "INFY.NS": ("Nifty", "Infosys"),
    "ITC.NS": ("Nifty", "ITC"),
    "TCS.NS": ("Nifty", "TCS"),
    "LT.NS": ("Nifty", "Larsen & Toubro"),
    "BHARTIARTL.NS": ("Nifty", "Bharti Airtel"),
    "AXISBANK.NS": ("Nifty", "Axis Bank"),
    "SBIN.NS": ("Nifty", "State Bank of India"),

    "KOTAKBANK.NS": ("Financial Services", "Kotak Mahindra Bank"),
    "BAJFINANCE.NS": ("Financial Services", "Bajaj Finance"),
    "BAJAJFINSV.NS": ("Financial Services", "Bajaj Finserv"),
    "HDFCLIFE.NS": ("Financial Services", "HDFC Life"),
    "SBILIFE.NS": ("Financial Services", "SBI Life"),

    "HCLTECH.NS": ("Information Technology", "HCL Technologies"),
    "WIPRO.NS": ("Information Technology", "Wipro"),
    "TECHM.NS": ("Information Technology", "Tech Mahindra"),
    "LTIM.NS": ("Information Technology", "LTIMindtree"),
    "TATAELXSI.NS": ("Information Technology", "Tata Elxsi"),

    "HINDUNILVR.NS": ("Consumer Goods", "Hindustan Unilever"),
    "NESTLEIND.NS": ("Consumer Goods", "Nestle India"),
    "BRITANNIA.NS": ("Consumer Goods", "Britannia"),
    "TATACONSUM.NS": ("Consumer Goods", "Tata Consumer Products"),
    "DABUR.NS": ("Consumer Goods", "Dabur India"),

    "MARUTI.NS": ("Automobiles", "Maruti Suzuki"),
    "M&M.NS": ("Automobiles", "Mahindra & Mahindra"),
    "TATAMOTORS.NS": ("Automobiles", "Tata Motors"),
    "BAJAJ-AUTO.NS": ("Automobiles", "Bajaj Auto"),
    "HEROMOTOCO.NS": ("Automobiles", "Hero MotoCorp"),

    "ULTRACEMCO.NS": ("Construction", "UltraTech Cement"),
    "GRASIM.NS": ("Construction", "Grasim Industries"),
    "SHREECEM.NS": ("Construction", "Shree Cement"),
    "DLF.NS": ("Construction", "DLF"),
    "AMBUJACEM.NS": ("Construction", "Ambuja Cements"),

    "NTPC.NS": ("Energy", "NTPC"),
    "POWERGRID.NS": ("Energy", "Power Grid"),
    "ONGC.NS": ("Energy", "ONGC"),
    "BPCL.NS": ("Energy", "BPCL"),
    "IOC.NS": ("Energy", "IOC"),

    "SUNPHARMA.NS": ("Pharmaceuticals", "Sun Pharma"),
    "DRREDDY.NS": ("Pharmaceuticals", "Dr. Reddy's"),
    "CIPLA.NS": ("Pharmaceuticals", "Cipla"),
    "DIVISLAB.NS": ("Pharmaceuticals", "Divi's Labs"),
    "APOLLOHOSP.NS": ("Pharmaceuticals", "Apollo Hospitals"),

    "TATASTEEL.NS": ("Metals", "Tata Steel"),
    "JSWSTEEL.NS": ("Metals", "JSW Steel"),
    "HINDALCO.NS": ("Metals", "Hindalco"),
    "COALINDIA.NS": ("Metals", "Coal India"),
    "VEDL.NS": ("Metals", "Vedanta"),

    "IDEA.NS": ("Telecommunication", "Vodafone Idea"),
    "TATACOMM.NS": ("Telecommunication", "Tata Communications"),
    "INDUSTOWER.NS": ("Telecommunication", "Indus Towers"),
    "TEJASNET.NS": ("Telecommunication", "Tejas Networks"),
}

def main():
    ist = pytz.timezone("Asia/Kolkata")
    ist = pytz.timezone("Asia/Kolkata")
    target_date = datetime.now(ist)

    target_date_str = target_date.strftime("%Y-%m-%d")

    for symbol, (sector, company) in symbol_map.items():
        print(f"\n📥 {company} ({symbol}) | {sector} | {target_date_str}")

        df = yf.download(
            symbol,
            interval="1m",
            start=target_date_str,
            end=(target_date + timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False
        )

        if df.empty:
            print("⚠️ No data")
            continue

        if df.index.tzinfo is None:
            df.index = df.index.tz_localize("UTC").tz_convert(ist)
        else:
            df.index = df.index.tz_convert(ist)

        df.reset_index(inplace=True)

        folder_path = os.path.join(BASE_DATA_DIR, sector, company)
        os.makedirs(folder_path, exist_ok=True)

        filename = f"{symbol.replace('^','').replace('.','_')}_{target_date.strftime('%Y_%m_%d')}.parquet"
        file_path = os.path.join(folder_path, filename)

        df.to_parquet(file_path, index=False)
        print(f"✅ Saved → {file_path}")

if __name__ == "__main__":
    main()
