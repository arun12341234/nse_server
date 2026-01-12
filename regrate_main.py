import asyncio
import aiohttp
import csv
import logging
import time
from pathlib import Path
from typing import List, Dict
import pandas as pd

# Import configuration
# from config import *
# config.py
"""
Configuration file for optimized processing
Adjust these values based on your system capabilities and API limits
"""

# Server Configuration
BASE_URL = "http://127.0.0.1:5000/api/download"
YEAR_TO_FETCH = 2026

# Performance Tuning
# -----------------------------
# Number of parallel workers (threads)
# Recommended: CPU cores * 2-4 for I/O-bound tasks
# Start with 10, increase gradually while monitoring
MAX_WORKERS = 30

# Batch size for processing symbols
# Larger batches = more memory usage but fewer pauses
# Smaller batches = less memory but more overhead
BATCH_SIZE = 100

# Delay between batches (seconds)
# Useful to avoid overwhelming the API server
BATCH_DELAY = 1

# Request timeout (seconds)
REQUEST_TIMEOUT = 30

# Retry Configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds between retries

# File Paths
SYMBOLS_CSV = 'symbols.csv'
OUTPUT_DIR = './Nse_files'
LOG_FILE = 'processing.log'
SUMMARY_FILE = 'processing_summary.csv'

# Default lot size
DEFAULT_LOT_SIZE = 125

# Logging Level
# Options: DEBUG, INFO, WARNING, ERROR
LOG_LEVEL = 'INFO'


# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AsyncSymbolProcessor:
    """Async processor for handling multiple symbols concurrently"""
    
    def __init__(self):
        self.session = None
        self.file_list = None
        self.semaphore = asyncio.Semaphore(MAX_WORKERS)
        
    async def __aenter__(self):
        """Setup async context"""
        # Create session with connection pooling
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=MAX_WORKERS)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup async context"""
        if self.session:
            await self.session.close()
    
    async def fetch_dates(self, year: int) -> List[str]:
        """Fetch dates asynchronously"""
        url = f"{BASE_URL}/{year}"
        logger.info(f"Fetching dates from: {url}...")
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    date_list = data.get("dates", [])
                    logger.info(f"Successfully fetched {len(date_list)} dates")
                    return date_list
                else:
                    logger.error(f"Failed to retrieve data. Status: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching dates: {e}")
            return None
    
    async def fetch_file_list(self, year: int) -> List[str]:
        """Fetch file list asynchronously"""
        url = f"http://127.0.0.1:5000/api/{year}/tracking"
        
        try:
            async with self.session.get(url, json={}) as response:
                if response.status == 200:
                    data = await response.json()
                    file_list = [item["file_2"] for item in data.get("data", []) if "file_2" in item]
                    logger.info(f"Successfully fetched {len(file_list)} files")
                    self.file_list = file_list
                    return file_list
                else:
                    logger.error(f"Error {response.status}: {await response.text()}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching file list: {e}")
            return None
    
    async def process_symbol(self, symbol: str, year: int, lot_size: int = DEFAULT_LOT_SIZE) -> Dict:
        """Process a single symbol with semaphore for concurrency control"""
        async with self.semaphore:
            return await self._process_symbol_impl(symbol, year, lot_size)
    
    async def _process_symbol_impl(self, symbol: str, year: int, lot_size: int) -> Dict:
        """Internal implementation of symbol processing"""
        try:
            # Run CPU-bound work in thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, 
                self._process_symbol_sync, 
                symbol, 
                year, 
                lot_size
            )
            return result
        except Exception as e:
            logger.error(f"✗ Error processing {symbol}: {e}")
            return {
                'symbol': symbol,
                'status': 'error',
                'error': str(e)
            }
    
    def _process_symbol_sync(self, symbol: str, year: int, lot_size: int) -> Dict:
        """Synchronous processing (runs in thread pool)"""
        try:
            from step_2_create_fudata_sheet import FuDataSheetCreator
            
            logger.info(f"Processing symbol: {symbol}")
            start_time = time.time()
            
            creator = FuDataSheetCreator(symbol=symbol, lot_size=lot_size)
            creator.list_exp_start(self.file_list)
            contracts_data = creator.create_fudata_sheet()
            
            output_path = f'{OUTPUT_DIR}/{year}/{symbol}_FuData_Generated.xlsx'
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
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
            raise
    
    async def process_symbols_batch(self, symbols: List[str], year: int) -> List[Dict]:
        """Process a batch of symbols concurrently"""
        tasks = [self.process_symbol(symbol, year) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions
        processed_results = []
        for symbol, result in zip(symbols, results):
            if isinstance(result, Exception):
                processed_results.append({
                    'symbol': symbol,
                    'status': 'error',
                    'error': str(result)
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def process_all_symbols(self, symbols: List[str], year: int) -> List[Dict]:
        """Process all symbols in batches"""
        all_results = []
        total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE
        
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing Batch {batch_num}/{total_batches} ({len(batch)} symbols)")
            logger.info(f"{'='*60}\n")
            
            batch_results = await self.process_symbols_batch(batch, year)
            all_results.extend(batch_results)
            
            # Progress update
            completed = len(all_results)
            total = len(symbols)
            logger.info(f"Overall Progress: {completed}/{total} ({completed/total*100:.1f}%)")
            
            # Delay between batches
            if i + BATCH_SIZE < len(symbols):
                await asyncio.sleep(BATCH_DELAY)
        
        return all_results

def load_symbols(csv_path: str = SYMBOLS_CSV) -> List[str]:
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

def generate_summary_report(results: List[Dict], output_file: str = SUMMARY_FILE):
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
    
    if 'time' in df.columns and success_count > 0:
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

async def main():
    """Main async execution function"""
    overall_start = time.time()
    
    async with AsyncSymbolProcessor() as processor:
        # Step 1: Fetch dates
        dates = await processor.fetch_dates(YEAR_TO_FETCH)
        if not dates:
            logger.error("Failed to fetch dates. Exiting.")
            return
        
        # Step 2: Run initial setup (sync operation)
        try:
            from step_1_equity_derivatives_list import start
            logger.info("Running step_1_equity_derivatives_list...")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, start)
        except Exception as e:
            logger.error(f"Error in step_1: {e}")
            return
        
        # Step 3: Fetch file list
        file_list = await processor.fetch_file_list(YEAR_TO_FETCH)
        if not file_list:
            logger.error("Failed to fetch file list. Exiting.")
            return
        
        # Step 4: Load symbols
        symbols = load_symbols()
        if not symbols:
            logger.error("No symbols loaded. Exiting.")
            return
        
        # Step 5: Process all symbols
        results = await processor.process_all_symbols(symbols, YEAR_TO_FETCH)
        
        # Step 6: Generate summary report
        generate_summary_report(results)
    
    overall_time = time.time() - overall_start
    logger.info(f"\n{'='*60}")
    logger.info(f"Total execution time: {overall_time:.2f}s ({overall_time/60:.2f} minutes)")
    logger.info(f"{'='*60}\n")

if __name__ == "__main__":
    asyncio.run(main())