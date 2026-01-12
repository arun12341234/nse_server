import requests
import os
import csv
from datetime import date, datetime, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from functools import lru_cache
import time

# Configuration
BASE_URL = "http://127.0.0.1:5000/api/download"
YEAR_TO_FETCH = 2026
MAX_WORKERS = 10  # Adjust based on your system and API limits
BATCH_SIZE = 50   # Process symbols in batches

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Session for connection pooling
session = requests.Session()
session.headers.update({'Connection': 'keep-alive'})

@lru_cache(maxsize=1)
def fetch_dates(year):
    """Fetch dates with caching to avoid redundant API calls"""
    url = f"{BASE_URL}/{year}"
    logger.info(f"Fetching dates from: {url}...")

    try:
        response = session.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            date_list = data.get("dates", [])
            logger.info(f"Successfully fetched {len(date_list)} dates")
            return date_list
        else:
            logger.error(f"Failed to retrieve data. Status Code: {response.status_code}")
            return None

    except requests.exceptions.ConnectionError:
        logger.error("Could not connect to the server. Is 'app.py' running?")
        return None
    except Exception as e:
        logger.error(f"Error fetching dates: {e}")
        return None

@lru_cache(maxsize=1)
def fetch_file_list(year):
    """Fetch file list once and cache it"""
    url = f"http://127.0.0.1:5000/api/{year}/tracking"
    
    try:
        response = session.get(url, json={}, timeout=30)

        if response.status_code == 200:
            data = response.json().get("data", [])
            file_1_list = [item["file_2"] for item in data if "file_2" in item]
            logger.info(f"Successfully fetched {len(file_1_list)} files")
            return file_1_list
        else:
            logger.error(f"Error {response.status_code}: {response.text}")
            return None

    except requests.exceptions.ConnectionError:
        logger.error("Failed to connect. Is the Flask server running?")
        return None
    except Exception as e:
        logger.error(f"Error fetching file list: {e}")
        return None

def load_symbols(csv_path='symbols.csv'):
    """Load symbols from CSV file"""
    symbols = []
    try:
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            symbols = [row['symbol'] for row in reader]
        logger.info(f"Loaded {len(symbols)} symbols from {csv_path}")
        return symbols
    except Exception as e:
        logger.error(f"Error loading symbols: {e}")
        return []

def process_single_symbol(symbol, file_1_list, year, lot_size=125):
    """Process a single symbol - to be run in parallel"""
    try:
        from step_2_create_fudata_sheet import FuDataSheetCreator
        
        logger.info(f"Processing symbol: {symbol}")
        start_time = time.time()
        
        creator = FuDataSheetCreator(symbol=symbol, lot_size=lot_size)
        creator.list_exp_start(file_1_list)
        contracts_data = creator.create_fudata_sheet()
        
        output_path = f'./Nse_files/{year}/{symbol}_FuData_Generated.xlsx'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        creator.save_workbook(output_path)
        
        elapsed = time.time() - start_time
        logger.info(f"✓ Completed {symbol} in {elapsed:.2f}s")
        
        return {
            'symbol': symbol,
            'status': 'success',
            'time': elapsed,
            'output': output_path
        }
        
    except Exception as e:
        logger.error(f"✗ Error processing {symbol}: {e}")
        return {
            'symbol': symbol,
            'status': 'error',
            'error': str(e)
        }

def process_symbols_parallel(symbols, file_1_list, year, max_workers=MAX_WORKERS):
    """Process symbols in parallel using ThreadPoolExecutor"""
    results = []
    total = len(symbols)
    completed = 0
    
    logger.info(f"Starting parallel processing of {total} symbols with {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_symbol = {
            executor.submit(process_single_symbol, symbol, file_1_list, year): symbol 
            for symbol in symbols
        }
        
        # Process completed tasks
        for future in as_completed(future_to_symbol):
            result = future.result()
            results.append(result)
            completed += 1
            
            if result['status'] == 'success':
                logger.info(f"Progress: {completed}/{total} ({completed/total*100:.1f}%)")
            
    return results

def process_symbols_batched(symbols, file_1_list, year, batch_size=BATCH_SIZE):
    """Process symbols in batches to manage memory"""
    all_results = []
    total_batches = (len(symbols) + batch_size - 1) // batch_size
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing Batch {batch_num}/{total_batches} ({len(batch)} symbols)")
        logger.info(f"{'='*60}\n")
        
        batch_results = process_symbols_parallel(batch, file_1_list, year)
        all_results.extend(batch_results)
        
        # Optional: Add a small delay between batches to avoid overwhelming the server
        if i + batch_size < len(symbols):
            time.sleep(1)
    
    return all_results

def generate_summary_report(results, output_file='processing_summary.csv'):
    """Generate a summary report of processing results"""
    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False)
    
    success_count = len(df[df['status'] == 'success'])
    error_count = len(df[df['status'] == 'error'])
    
    logger.info(f"\n{'='*60}")
    logger.info("PROCESSING SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total symbols: {len(results)}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Errors: {error_count}")
    
    if 'time' in df.columns:
        avg_time = df[df['status'] == 'success']['time'].mean()
        total_time = df[df['status'] == 'success']['time'].sum()
        logger.info(f"Average time per symbol: {avg_time:.2f}s")
        logger.info(f"Total processing time: {total_time:.2f}s")
    
    logger.info(f"\nDetailed report saved to: {output_file}")
    logger.info(f"{'='*60}\n")
    
    if error_count > 0:
        logger.warning("Failed symbols:")
        for result in results:
            if result['status'] == 'error':
                logger.warning(f"  - {result['symbol']}: {result.get('error', 'Unknown error')}")

def main():
    """Main execution function"""
    overall_start = time.time()
    
    # Step 1: Fetch dates (cached)
    dates = fetch_dates(YEAR_TO_FETCH)
    if not dates:
        logger.error("Failed to fetch dates. Exiting.")
        return
    
    # Step 2: Run initial setup
    try:
        from step_1_equity_derivatives_list import start
        logger.info("Running step_1_equity_derivatives_list...")
        start()
    except Exception as e:
        logger.error(f"Error in step_1: {e}")
        return
    
    # Step 3: Fetch file list (cached)
    file_1_list = fetch_file_list(YEAR_TO_FETCH)
    if not file_1_list:
        logger.error("Failed to fetch file list. Exiting.")
        return
    
    # Step 4: Load symbols
    symbols = load_symbols('symbols.csv')
    if not symbols:
        logger.error("No symbols loaded. Exiting.")
        return
    
    # Step 5: Process symbols
    # Choose one of the following processing methods:
    
    # Option A: Batched processing (recommended for large datasets)
    results = process_symbols_batched(symbols, file_1_list, YEAR_TO_FETCH)
    
    # Option B: Simple parallel processing (all at once)
    # results = process_symbols_parallel(symbols, file_1_list, YEAR_TO_FETCH)
    
    # Step 6: Generate summary report
    generate_summary_report(results)
    
    overall_time = time.time() - overall_start
    logger.info(f"\n{'='*60}")
    logger.info(f"Total execution time: {overall_time:.2f}s ({overall_time/60:.2f} minutes)")
    logger.info(f"{'='*60}\n")

if __name__ == "__main__":
    main()