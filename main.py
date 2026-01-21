import os
import io
import json
import pytz
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


# =========================
# STOCK UNIVERSE
# =========================
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


# =========================
# GOOGLE DRIVE HELPERS
# =========================
def get_drive_service():
    creds_json = os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON")
    if not creds_json:
        raise RuntimeError("Missing GDRIVE_SERVICE_ACCOUNT_JSON")

    creds_dict = json.loads(creds_json)

    credentials = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive"]
    )

    return build("drive", "v3", credentials=credentials)


def get_or_create_folder(service, name, parent_id):
    query = (
        f"name='{name}' and "
        f"mimeType='application/vnd.google-apps.folder' and "
        f"'{parent_id}' in parents and trashed=false"
    )

    result = service.files().list(q=query, fields="files(id)").execute()
    files = result.get("files", [])

    if files:
        return files[0]["id"]

    folder_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }

    folder = service.files().create(
        body=folder_metadata, fields="id"
    ).execute()

    return folder["id"]


# =========================
# MAIN PIPELINE
# =========================
def main():
    ist = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist)

    print("=" * 60)
    print("JOB STARTED AT (IST):", now_ist)
    print("=" * 60)

    target_date_str = now_ist.strftime("%Y-%m-%d")

    drive_service = get_drive_service()
    root_folder_id = os.environ.get("DRIVE_FOLDER_ID")

    if not root_folder_id:
        raise RuntimeError("Missing DRIVE_FOLDER_ID")

    for symbol, (sector, company) in symbol_map.items():
        print(f"\n📥 {company} ({symbol}) | {sector} | {target_date_str}")

        df = yf.download(
            symbol,
            interval="1m",
            start=target_date_str,
            end=(now_ist + timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False
        )

        if df.empty:
            print("⚠️ No data available")
            continue

        if df.index.tzinfo is None:
            df.index = df.index.tz_localize("UTC").tz_convert(ist)
        else:
            df.index = df.index.tz_convert(ist)

        df.reset_index(inplace=True)

        sector_id = get_or_create_folder(drive_service, sector, root_folder_id)
        company_id = get_or_create_folder(drive_service, company, sector_id)

        filename = f"{symbol.replace('^','').replace('.','_')}_{now_ist.strftime('%Y_%m_%d')}.parquet"

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        file_metadata = {
            "name": filename,
            "parents": [company_id]
        }

        media = MediaIoBaseUpload(
            buffer,
            mimetype="application/octet-stream",
            resumable=False
        )

        drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()

        print(f"✅ Uploaded → {sector}/{company}/{filename}")


if __name__ == "__main__":
    main()

