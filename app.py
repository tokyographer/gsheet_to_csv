import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
import logging
import os
from zipfile import ZipFile
import time

# Set up a unique log file for each session
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"app_{current_time}.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
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

# Initialize session state for stopping the process
if "stop_process" not in st.session_state:
    st.session_state.stop_process = False

def stop_processing():
    st.session_state.stop_process = True

# Stop button
st.button("Stop Processing", on_click=stop_processing)

def make_unique_headers(headers):
    """Make headers unique by appending an index to duplicates."""
    seen = {}
    result = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            result.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            result.append(h)
    return result

if st.button("Convert to CSV"):
    st.session_state.stop_process = False
    st.info("Starting the conversion process...")
    progress_bar = st.progress(0)
    urls = sheet_urls.strip().split("\n")
    csv_dir = "csv_files"
    
    # Create the directory if it doesn't exist
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
        logging.info(f"Created directory {csv_dir} for storing CSV files.")

    def fetch_sheet_data(url, retries=3, delay=5):
        """Fetch sheet data with retry logic and log authentication status."""
        for attempt in range(retries):
            try:
                sheet = client.open_by_url(url)
                worksheet = sheet.get_worksheet(0)
                
                # Fetch the headers and make them unique if necessary
                headers = worksheet.row_values(1)
                unique_headers = make_unique_headers(headers)
                data = worksheet.get_all_records(expected_headers=unique_headers)
                
                logging.info(f"Successfully authenticated and fetched data from: {sheet.title}")
                return pd.DataFrame(data), sheet.title
            except gspread.exceptions.APIError as e:
                logging.error(f"API error on attempt {attempt + 1} for URL {url}: {e}")
                if '403' in str(e):
                    logging.error(f"Authentication failed for URL {url}. Check permissions.")
                if attempt < retries - 1:
                    time.sleep(delay * (attempt + 1))
                else:
                    raise
            except gspread.exceptions.SpreadsheetNotFound:
                logging.error(f"Spreadsheet not found for URL: {url}")
                raise
            except Exception as e:
                logging.error(f"Error accessing sheet at URL {url}: {e}")
                raise

    for i, url in enumerate(urls):
        url = url.strip()
        if not url:
            continue
        if st.session_state.stop_process:
            st.warning("Processing stopped.")
            logging.info("Processing stopped by user.")
            break
        try:
            df, sheet_title = fetch_sheet_data(url)
            csv_file = os.path.join(csv_dir, f"{sheet_title}.csv")
            df.to_csv(csv_file, index=False)
            st.write(f"Converted: {sheet_title}")
            logging.info(f"Successfully converted {sheet_title} to CSV.")
        except gspread.exceptions.SpreadsheetNotFound as e:
            st.error(f"Spreadsheet not found: {url}")
            logging.error(f"Spreadsheet not found for URL {url}: {e}")
        except gspread.exceptions.APIError as e:
            st.error(f"API error while accessing {url}: {e}")
            logging.error(f"API error for URL {url}: {e}")
        except Exception as e:
            st.error(f"An error occurred: {e}")
            logging.error(f"Error processing {url}: {e}")
        finally:
            progress_bar.progress((i + 1) / len(urls))

    # Creating a Zip file
    if not st.session_state.stop_process:
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