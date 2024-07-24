import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from zipfile import ZipFile
import os

# Google Sheets API setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)

st.title("Google Sheets to CSV Converter")

# Input for Google Sheets URLs
sheet_urls = st.text_area("Enter the Google Sheet URLs (one per line):")

if st.button("Convert to CSV"):
    st.info("Starting the conversion process...")
    progress_bar = st.progress(0)
    urls = sheet_urls.strip().split("\n")
    csv_dir = "csv_files"

    # Create the directory if it doesn't exist
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    for i, url in enumerate(urls):
        if not url.strip():
            continue
        try:
            sheet = client.open_by_url(url.strip())
            worksheet = sheet.get_worksheet(0)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            csv_file = os.path.join(csv_dir, f"{sheet.title}.csv")
            df.to_csv(csv_file, index=False)
            st.write(f"Converted: {sheet.title}")
        except Exception as e:
            st.error(f"Error processing {url.strip()}: {e}")
        progress_bar.progress((i + 1) / len(urls))

    # Creating a Zip file
    with ZipFile('all_csv_files.zip', 'w') as zipf:
        for root, dirs, files in os.walk(csv_dir):
            for file in files:
                zipf.write(os.path.join(root, file))
    
    # Provide download button
    with open("all_csv_files.zip", "rb") as f:
        st.download_button("Download All CSVs", f, "all_csv_files.zip")

    # Clean up the CSV files and directory
    for file in os.listdir(csv_dir):
        os.remove(os.path.join(csv_dir, file))
    os.rmdir(csv_dir)