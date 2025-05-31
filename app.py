import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import time
import re
from urllib.parse import urljoin
import hashlib
import logging
import threading
import queue
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Streamlit app configuration
st.set_page_config(page_title="Delhi High Court PDF Crawler", layout="wide")

# Google Drive API setup
SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = 'crawl-461510-5bb4c49fe7e2.json'
GOOGLE_DRIVE_FOLDER_ID = '1R_CAGMvLzncwOf5J_GLJThv75uBqDF4t'

def get_drive_service():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Error initializing Google Drive service: {e}")
        st.error(f"Error initializing Google Drive service: {e}")
        raise

def upload_to_drive(service, filename, file_content, folder_id):
    try:
        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        media = MediaIoBaseUpload(io.BytesIO(file_content), mimetype='application/pdf')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')
    except Exception as e:
        logger.error(f"Error uploading to Google Drive: {e}")
        st.error(f"Error uploading to Google Drive: {e}")
        raise

# Crawling setup
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
}

base_url = "https://dhcbkp.nic.in/FreeText/Casecatsearch.do?scode=31&fflag=1"

def load_combinations(file_path):
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Excel file not found at {file_path}")
        df = pd.read_excel(file_path)
        required_columns = ['Category_Value', 'Category_Name', 'Year']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        combinations = df[required_columns].drop_duplicates().to_dict('records')
        logger.info(f"Loaded {len(combinations)} unique category combinations")
        if len(combinations) == 0:
            raise ValueError("No valid combinations found in Excel file")
        if len(combinations) > 10000:
            logger.warning(f"Excessive combinations ({len(combinations)}). Consider cleaning Excel file.")
            st.warning(f"Excessive combinations ({len(combinations)}). Consider cleaning Excel file.")
        return combinations
    except Exception as e:
        logger.error(f"Error reading Excel file {file_path}: {e}")
        st.error(f"Error reading Excel file {file_path}: {e}")
        return []

def save_debug_html(content, category_name, year, page_no, progress_queue):
    try:
        safe_category = re.sub(r'[^\w\s-]', '', category_name).replace(' ', '_')
        debug_dir = f"Debug_HTML/{safe_category}/{year}"
        os.makedirs(debug_dir, exist_ok=True)
        debug_file = f"{debug_dir}/page_{page_no}.html"
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(content)
        progress_queue.put(f"Saved debug HTML: {debug_file}")
        logger.info(f"Saved debug HTML: {debug_file}")
    except Exception as e:
        progress_queue.put(f"Error saving debug HTML for page {page_no}: {e}")
        logger.error(f"Error saving debug HTML for page {page_no}: {e}")

def get_total_pages(soup):
    try:
        page_info = soup.find('div', class_='row justify-content-center')
        if page_info:
            text = page_info.text.strip()
            match = re.search(r'total (\d+) records', text)
            if match:
                total_records = int(match.group(1))
                records_per_page = 10
                return (total_records + records_per_page - 1) // records_per_page
        total_no_page = soup.find('input', {'id': 'total_no_page'})
        if total_no_page and total_no_page.get('value'):
            return int(total_no_page.get('value'))
        logger.warning("No pagination info found, assuming 1 page")
        return 1
    except Exception as e:
        logger.error(f"Error determining total pages: {e}")
        return 1

def fetch_page(category_value, year, page_no, progress_queue):
    try:
        session = requests.Session()
        form_data = {
            'cat': str(category_value),
            'judgementyr': str(year),
            'Selected_page': str(page_no),
            'orderby': 'desc',
            'disp': time.strftime('%H:%M:%S')
        }
        response = session.post(base_url, data=form_data, headers=headers, timeout=10)
        response.raise_for_status()
        progress_queue.put(f"Status code for page {page_no}: {response.status_code}")
        logger.info(f"Fetched page {page_no} for category {category_value}, year {year}")
        return BeautifulSoup(response.text, 'lxml'), response.text
    except requests.RequestException as e:
        progress_queue.put(f"Error fetching page {page_no} for category {category_value}, year {year}: {e}")
        logger.error(f"Error fetching page {page_no} for category {category_value}, year {year}: {e}")
        return None, None

def extract_pdfs(soup, category_name, year, html_content, page_no, progress_queue):
    try:
        if "no records found" in html_content.lower() or "no matching records" in html_content.lower():
            progress_queue.put(f"No records found for category {category_name}, year {year}, page {page_no}")
            logger.info(f"No records found for {category_name}, year {year}, page {page_no}")
            return []

        table = soup.find('table', class_='table table-hover table-bordered text-center')
        if not table:
            progress_queue.put(f"No results table found for category {category_name}, year {year}, page {page_no}")
            logger.warning(f"No results table found for {category_name}, year {year}, page {page_no}")
            save_debug_html(html_content, category_name, year, page_no, progress_queue)
            return []

        rows = table.find_all('tr')[1:]
        pdfs = []
        case_info = []
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 5:
                case_no = cols[1].text.strip()
                title = cols[2].text.strip()
                judgement_date = cols[3].text.strip()
                pdf_link = cols[4].find('a')
                if pdf_link and pdf_link.get('href'):
                    pdf_url = urljoin('https://dhccaseinfo.nic.in/', pdf_link['href'])
                    pdfs.append({
                        'case_no': case_no,
                        'title': title,
                        'judgement_date': judgement_date,
                        'pdf_url': pdf_url
                    })
                    case_info.append(f"{case_no} ({title}, {judgement_date})")
            else:
                progress_queue.put(f"Insufficient columns in row for category {category_name}, year {year}, page {page_no}")
                logger.warning(f"Insufficient columns in row for {category_name}, year {year}, page {page_no}")
        progress_queue.put(f"Found {len(pdfs)} PDFs on page {page_no}: {', '.join(case_info)}")
        logger.info(f"Extracted {len(pdfs)} PDFs from page {page_no} for {category_name}, year {year}")
        return pdfs
    except Exception as e:
        progress_queue.put(f"Error extracting PDFs for page {page_no} for category {category_name}, year {year}: {e}")
        logger.error(f"Error extracting PDFs for page {page_no} for {category_name}, year {year}: {e}")
        save_debug_html(html_content, category_name, year, page_no, progress_queue)
        return []

def sanitize_title(title):
    invalid_chars = r'[<>:"/\\|?*\x00-\x1F]'
    safe_title = re.sub(invalid_chars, '', title)
    safe_title = re.sub(r'\s+', '_', safe_title.strip())
    return safe_title[:100]

def download_pdf(pdf_info, processed_urls, drive_service, folder_id, progress_queue):
    try:
        if pdf_info['pdf_url'] in processed_urls:
            progress_queue.put(f"Skipping duplicate PDF: {pdf_info['pdf_url']} ({pdf_info['case_no']}, {pdf_info['title']})")
            logger.info(f"Skipped duplicate PDF: {pdf_info['pdf_url']}")
            return False

        safe_title = sanitize_title(pdf_info['title'])
        url_hash = hashlib.md5(pdf_info['pdf_url'].encode()).hexdigest()[:8]
        filename = f"{safe_title}_{url_hash}.pdf"

        response = requests.get(pdf_info['pdf_url'], headers=headers, timeout=10)
        response.raise_for_status()
        
        drive_file_id = upload_to_drive(drive_service, filename, response.content, folder_id)
        progress_queue.put(f"Uploaded to Google Drive: {filename} (Drive ID: {drive_file_id})")
        processed_urls.add(pdf_info['pdf_url'])
        logger.info(f"Uploaded PDF: {filename} (Drive ID: {drive_file_id})")
        return True
    except requests.RequestException as e:
        progress_queue.put(f"Error downloading PDF {pdf_info['pdf_url']}: {e}")
        logger.error(f"Error downloading PDF {pdf_info['pdf_url']}: {e}")
        return False
    except Exception as e:
        progress_queue.put(f"Error processing PDF {pdf_info['pdf_url']}: {e}")
        logger.error(f"Error processing PDF {pdf_info['pdf_url']}: {e}")
        return False

def crawl_pdfs(progress_queue):
    try:
        drive_service = get_drive_service()
        folder_id = GOOGLE_DRIVE_FOLDER_ID
        progress_queue.put(f"Connected to Google Drive, using folder ID: {folder_id}")
        logger.info(f"Connected to Google Drive, folder ID: {folder_id}")

        file_path = 'case_category_year_combinations.xlsx'
        combinations = load_combinations(file_path)
        if not combinations:
            progress_queue.put("No combinations loaded. Exiting.")
            logger.error(f"No combinations loaded from {file_path}")
            return

        total_combinations = len(combinations)
        for idx, combo in enumerate(combinations, 1):
            category_value = combo['Category_Value']
            category_name = combo['Category_Name']
            year = combo['Year']
            progress_queue.put(f"\nProcessing category: {category_name}, year: {year} ({idx}/{total_combinations})")
            logger.info(f"Processing category: {category_name}, year: {year}")

            processed_urls = set()

            soup, html_content = fetch_page(category_value, year, 1, progress_queue)
            if not soup or not html_content:
                continue

            total_pages = get_total_pages(soup)
            progress_queue.put(f"Found {total_pages} pages for category {category_name}, year {year}")

            pdfs = extract_pdfs(soup, category_name, year, html_content, 1, progress_queue)
            if not pdfs and "no records" in html_content.lower():
                progress_queue.put(f"Skipping further pages for {category_name}, year {year} due to no records")
                logger.info(f"No records for {category_name}, year {year}, skipping further pages")
                continue

            for pdf in pdfs:
                download_pdf(pdf, processed_urls, drive_service, folder_id, progress_queue)
            save_debug_html(html_content, category_name, year, 1, progress_queue)

            for page in range(2, total_pages + 1):
                progress_queue.put(f"Fetching page {page} of {total_pages}")
                soup, html_content = fetch_page(category_value, year, page, progress_queue)
                if not soup or not html_content:
                    continue

                pdfs = extract_pdfs(soup, category_name, year, html_content, page, progress_queue)
                for pdf in pdfs:
                    download_pdf(pdf, processed_urls, drive_service, folder_id, progress_queue)

                save_debug_html(html_content, category_name, year, page, progress_queue)
                time.sleep(1)
            time.sleep(2)
        progress_queue.put("Crawling completed!")
        logger.info("Crawling completed")
    except Exception as e:
        progress_queue.put(f"Error in crawling: {e}")
        logger.error(f"Error in crawling: {e}")

# Streamlit app
def main():
    st.title("Delhi High Court PDF Crawler")
    st.write(f"Crawling PDFs from Delhi High Court website and saving to Google Drive folder ID: {GOOGLE_DRIVE_FOLDER_ID}")

    if 'crawling' not in st.session_state:
        st.session_state.crawling = False
        st.session_state.progress_queue = queue.Queue()
        st.session_state.progress_log = []

    if not st.session_state.crawling:
        st.session_state.crawling = True
        st.write("Starting PDF crawling...")
        threading.Thread(target=crawl_pdfs, args=(st.session_state.progress_queue,), daemon=True).start()

    progress_container = st.empty()
    log_container = st.empty()

    while st.session_state.crawling:
        try:
            while not st.session_state.progress_queue.empty():
                message = st.session_state.progress_queue.get_nowait()
                st.session_state.progress_log.append(message)
                if "Crawling completed" in message or "Error in crawling" in message:
                    st.session_state.crawling = False
            with progress_container.container():
                st.write("**Crawling Progress:**")
                for msg in st.session_state.progress_log[-10:]:
                    st.write(msg)
            with log_container.container():
                st.write("**Full Log:**")
                st.write("\n".join(st.session_state.progress_log))
            time.sleep(0.5)
        except queue.Empty:
            time.sleep(0.5)

    if st.session_state.progress_log and "Crawling completed" in st.session_state.progress_log[-1]:
        st.success(f"Crawling finished! Check Google Drive folder ID: {GOOGLE_DRIVE_FOLDER_ID} for PDFs.")
    else:
        st.error("Crawling failed. Check log for details.")

if __name__ == "__main__":
    main()