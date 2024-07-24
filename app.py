import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from zipfile import ZipFile
import os
import logging

# Set up logging
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Google Sheets API setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
try:
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    logging.info("Successfully authenticated with Google Sheets API.")
except Exception as e:
    logging.error(f"Failed to authenticate with Google Sheets API: {e}")
    st.error("Failed to authenticate with Google Sheets API. Check the logs for more details.")

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
        logging.info(f"Created directory {csv_dir} for storing CSV files.")

    for i, url in enumerate(urls):
        url = url.strip()
        if not url:
            continue
        try:
            sheet = client.open_by_url(url)
            worksheet = sheet.get_worksheet(0)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            csv_file = os.path.join(csv_dir, f"{sheet.title}.csv")
            df.to_csv(csv_file, index=False)
            st.write(f"Converted: {sheet.title}")
            logging.info(f"Successfully converted {sheet.title} to CSV.")
        except gspread.exceptions.SpreadsheetNotFound:
            st.error(f"Spreadsheet not found: {url}")
            logging.error(f"Spreadsheet not found for URL: {url}")
        except gspread.exceptions.APIError as e:
            st.error(f"API error while accessing {url}: {e}")
            logging.error(f"API error for URL {url}: {e}")
        except Exception as e:
            st.error(f"An error occurred: {e}")
            logging.error(f"Error processing {url}: {e}")
        finally:
            progress_bar.progress((i + 1) / len(urls))

    # Creating a Zip file
    try:
        with ZipFile('all_csv_files.zip', 'w') as zipf:
            for root, dirs, files in os.walk(csv_dir):
                for file in files:
                    zipf.write(os.path.join(root, file))
        logging.info("Successfully created zip archive all_csv_files.zip.")
    except Exception as e:
        st.error(f"Error creating zip file: {e}")
        logging.error(f"Error creating zip file: {e}")

    # Provide download button
    try:
        with open("all_csv_files.zip", "rb") as f:
            st.download_button("Download All CSVs", f, "all_csv_files.zip")
    except FileNotFoundError:
        st.error("The zip file was not found.")
        logging.error("The zip file all_csv_files.zip was not found.")
    except Exception as e:
        st.error(f"Error offering zip file download: {e}")
        logging.error(f"Error offering zip file download: {e}")

    # Clean up the CSV files and directory
    try:
        for file in os.listdir(csv_dir):
            os.remove(os.path.join(csv_dir, file))
        os.rmdir(csv_dir)
        logging.info(f"Cleaned up directory {csv_dir}.")
    except Exception as e:
        logging.error(f"Error during cleanup: {e}")