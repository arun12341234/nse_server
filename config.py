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
MAX_WORKERS = 10

# Batch size for processing symbols
# Larger batches = more memory usage but fewer pauses
# Smaller batches = less memory but more overhead
BATCH_SIZE = 50

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
