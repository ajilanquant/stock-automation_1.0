import os
import io
import json
import pytz
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

from google.oauth2.credentials import Credentials
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
# GOOGLE DRIVE (OAUTH)
# =========================
def get_drive_service():
    token_json = os.environ.get("GDRIVE_OAUTH_TOKEN")
    client_secret_json = os.environ.get("GDRIVE_CLIENT_SECRET")

    if not token_json or not client_secret_json:
        raise RuntimeError("Missing OAuth secrets")

    token_info = json.loads(token_json)
    client_info = json.loads(client_secret_json)

    creds = Credentials(
        token=token_info.get("token"),
        refresh_token=token_info.get("refresh_token"),
        token_uri=client_info["installed"]["token_uri"],
        client_id=client_info["installed"]["client_id"],
        client_secret=client_info["installed"]["client_secret"],
        scopes=["https://www.googleapis.com/auth/drive"],
    )

    return build("drive", "v3", credentials=creds)


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

    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }

    folder = service.files().create(body=metadata, fields="id").execute()
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

    # ROOT FOLDER NAME IN MY DRIVE
    ROOT_FOLDER_NAME = "Market_60_TEST"

    # Ensure root folder exists in My Drive
    root_query = (
        f"name='{ROOT_FOLDER_NAME}' and "
        f"mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    root_result = drive_service.files().list(
        q=root_query, fields="files(id)"
    ).execute()

    if root_result["files"]:
        root_folder_id = root_result["files"][0]["id"]
    else:
        root_folder_id = drive_service.files().create(
            body={
                "name": ROOT_FOLDER_NAME,
                "mimeType": "application/vnd.google-apps.folder",
            },
            fields="id",
        ).execute()["id"]

    for symbol, (sector, company) in symbol_map.items():
        print(f"\n📥 {company} ({symbol}) | {sector} | {target_date_str}")

        df = yf.download(
            symbol,
            interval="1m",
            start=target_date_str,
            end=(now_ist + timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False,
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

        media = MediaIoBaseUpload(
            buffer, mimetype="application/octet-stream", resumable=False
        )

        drive_service.files().create(
            body={"name": filename, "parents": [company_id]},
            media_body=media,
            fields="id",
        ).execute()

        print(f"✅ Uploaded → {sector}/{company}/{filename}")


if __name__ == "__main__":
    main()
