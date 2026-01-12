"""
NSE Data Downloader Client

This module fetches trading dates and downloads various NSE data files
including CM Bhavcopy, FO Bhavcopy, Combined OI, and FII Statistics.

Usage:
    python data_downloader.py
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

import requests
from requests.exceptions import ConnectionError, RequestException


# Configuration
BASE_URL = "http://127.0.0.1:5000/api"
YEAR_TO_FETCH = 2026
TIMEOUT = 30  # Request timeout in seconds

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NSEDataDownloader:
    """Client for downloading NSE market data files."""
    
    def __init__(self, base_url: str = BASE_URL, year: int = YEAR_TO_FETCH):
        """
        Initialize the NSE Data Downloader.
        
        Args:
            base_url: Base URL of the API server
            year: Year to fetch data for
        """
        self.base_url = base_url
        self.year = year
        self.session = requests.Session()
    
    def fetch_trading_dates(self) -> Optional[List[str]]:
        """
        Fetch list of trading dates for the configured year.
        
        Returns:
            List of date strings in YYYY-MM-DD format, or None if failed
        """
        url = f"{self.base_url}/download/{self.year}"
        logger.info(f"Fetching trading dates from: {url}")
        
        try:
            response = self.session.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            date_list = data.get("dates", [])
            
            logger.info(f"Successfully fetched {len(date_list)} trading dates")
            logger.debug(f"Date range: {data.get('range_start')} to {data.get('range_end')}")
            
            return date_list
            
        except ConnectionError:
            logger.error("Could not connect to server. Is app.py running?")
            return None
        except RequestException as e:
            logger.error(f"Failed to retrieve dates. Error: {e}")
            return None
    
    def get_tracking_status(self, date: str) -> Optional[Dict[str, Any]]:
        """
        Get download tracking status for a specific date.
        
        Args:
            date: Date string in YYYY-MM-DD format
            
        Returns:
            Dictionary with tracking data or None if failed
        """
        url = f"{self.base_url}/{self.year}/tracking/{date}"
        
        try:
            response = self.session.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
            
        except RequestException as e:
            logger.warning(f"Failed to get tracking status for {date}: {e}")
            return None
    
    def update_tracking_status(self, date: str, file_key: str, file_data: str) -> bool:
        """
        Update tracking status for a downloaded file.
        
        Args:
            date: Date string in YYYY-MM-DD format
            file_key: Key for the file type (file_1, file_2, etc.)
            file_data: Downloaded file data/path
            
        Returns:
            True if update successful, False otherwise
        """
        url = f"{self.base_url}/{self.year}/tracking/{date}"
        payload = {file_key: file_data}
        
        try:
            response = self.session.post(url, json=payload, timeout=TIMEOUT)
            response.raise_for_status()
            logger.debug(f"Updated {file_key} for {date}")
            return True
            
        except RequestException as e:
            logger.error(f"Failed to update tracking for {date}/{file_key}: {e}")
            return False
    
    def download_cm_bhavcopy(self, date_str: str) -> Optional[str]:
        """
        Download CM (Capital Market) Bhavcopy for given date.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Downloaded file data/path or None if failed
        """
        try:
            from cm_bc import CMBhavcopyDownloader
            
            downloader = CMBhavcopyDownloader()
            date = datetime.strptime(date_str, "%Y-%m-%d")
            result = downloader.download_date(date)
            
            logger.info(f"Downloaded CM Bhavcopy for {date_str}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to download CM Bhavcopy for {date_str}: {e}")
            return None
    
    def download_fo_bhavcopy(self, date_str: str) -> Optional[str]:
        """
        Download FO (Futures & Options) Bhavcopy for given date.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Downloaded file data/path or None if failed
        """
        try:
            from bc import NSEBhavcopyDownloader
            
            downloader = NSEBhavcopyDownloader()
            date = datetime.strptime(date_str, "%Y-%m-%d")
            result = downloader.download_date(date)
            
            logger.info(f"Downloaded FO Bhavcopy for {date_str}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to download FO Bhavcopy for {date_str}: {e}")
            return None
    
    def download_combined_oi(self, date_str: str) -> Optional[str]:
        """
        Download Combined Open Interest data for given date.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Downloaded file data/path or None if failed
        """
        try:
            from nse_combined_oi_downloader import NSECombinedOIDownloader
            
            downloader = NSECombinedOIDownloader()
            
            if not downloader.init_session():
                logger.error("Failed to initialize Combined OI downloader session")
                return None
            
            # Convert date format to DD-MMM-YYYY
            date_formatted = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%b-%Y")
            result = downloader.download_combined_oi(date=date_formatted)
            
            logger.info(f"Downloaded Combined OI for {date_str}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to download Combined OI for {date_str}: {e}")
            return None
    
    def download_fii_statistics(self, date_str: str) -> Optional[str]:
        """
        Download FII Statistics for given date.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Downloaded file data/path or None if failed
        """
        try:
            from nse_fii_statistics_downloader import NSEAPIDownloader
            
            downloader = NSEAPIDownloader()
            
            # Convert date format to DD-MMM-YYYY
            date_formatted = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%b-%Y")
            result = downloader.download_fii_statistics(date=date_formatted)
            
            logger.info(f"Downloaded FII Statistics for {date_str}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to download FII Statistics for {date_str}: {e}")
            return None
        
    def download_participant_oi(self, date_str: str) -> Optional[str]:
        """
        Download FII Statistics for given date.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Downloaded file data/path or None if failed
        """
        try:
            from nse_participant_oi_downloader import NSEAPIDownloader
            
            downloader = NSEAPIDownloader()
            
            # Convert date format to DD-MMM-YYYY
            date_formatted = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%b-%Y")
            result = downloader.download_participant_oi(date=date_formatted)
            
            logger.info(f"Downloaded Participant OI for {date_str}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to download Participant OI for {date_str}: {e}")
            return None
    
    def process_date(self, date: str) -> None:
        """
        Process all downloads for a specific date.
        
        Args:
            date: Date string in YYYY-MM-DD format
        """
        logger.info(f"Processing date: {date}")
        
        # Get current tracking status
        tracking_data = self.get_tracking_status(date)
        
        if not tracking_data:
            logger.warning(f"No tracking data found for {date}, will create new entry")
            tracking_data = {"data": {}}
        
        files_status = tracking_data.get("data", {})
        
        # Define download tasks
        download_tasks = [
            ("file_1", "CM Bhavcopy", self.download_cm_bhavcopy),
            ("file_2", "FO Bhavcopy", self.download_fo_bhavcopy),
            ("file_3", "Combined OI", self.download_combined_oi),
            ("file_4", "FII Statistics", self.download_fii_statistics),
            ("file_5", "Participant OI", self.download_participant_oi),
        ]
        
        # Process each file type
        for file_key, file_name, download_func in download_tasks:
            if files_status.get(file_key):
                logger.info(f"{file_name} already downloaded for {date} - skipping")
            else:
                logger.info(f"Downloading {file_name} for {date}...")
                file_data = download_func(date)
                
                if file_data:
                    self.update_tracking_status(date, file_key, str(file_data))
                    logger.info(f"Successfully downloaded and tracked {file_name}")
                else:
                    logger.error(f"Failed to download {file_name} for {date}")
    
    def verify_tracking_endpoint(self) -> bool:
        """
        Verify the tracking endpoint is accessible.
        
        Returns:
            True if endpoint is accessible, False otherwise
        """
        url = f"{self.base_url}/{self.year}/track_date"
        
        try:
            response = self.session.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Tracking endpoint verified: {data}")
            return True
            
        except RequestException as e:
            logger.error(f"Failed to verify tracking endpoint: {e}")
            return False
    
    def run(self) -> None:
        """Execute the complete download workflow."""
        logger.info(f"Starting NSE Data Downloader for year {self.year}")
        
        # Fetch trading dates
        dates = self.fetch_trading_dates()
        if not dates:
            logger.error("Failed to fetch trading dates. Exiting.")
            return
        
        # Verify tracking endpoint
        self.verify_tracking_endpoint()
        
        # Process each date
        for date in dates:
            try:
                self.process_date(date)
            except Exception as e:
                logger.error(f"Error processing date {date}: {e}", exc_info=True)
                continue
        
        logger.info("Download workflow completed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        self.session.close()


def main():
    """Main entry point for the script."""
    try:
        with NSEDataDownloader() as downloader:
            downloader.run()
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
