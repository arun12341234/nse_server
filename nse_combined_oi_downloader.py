"""
NSE Combined Open Interest Downloader
Downloads Combine Open Interest across exchanges data from NSE India
Handles the new NSE authentication and session requirements
"""

import requests
from datetime import datetime, timedelta
import os

class NSECombinedOIDownloader:
    def __init__(self):
        self.base_url = "https://www.nseindia.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def init_session(self):
        """Initialize session by visiting the main page first"""
        try:
            # Visit main page to get cookies
            url = f"{self.base_url}/all-reports-derivatives"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            print("✓ Session initialized successfully")
            return True
        except Exception as e:
            print(f"✗ Failed to initialize session: {str(e)}")
            return False
    
    def download_combined_oi(self, date=None, output_dir="./NSE_Downloads/FO_Combined_OI"):
        """
        Download Combined Open Interest across exchanges
        
        Args:
            date: datetime object or string in 'DD-MMM-YYYY' format (e.g., '11-JAN-2024')
                  If None, uses previous trading day
            output_dir: Directory to save the downloaded file
        """
        if date is None:
            # Use previous trading day (assuming today is a trading day)
            date = datetime.now() - timedelta(days=1)
        
        # Format date for NSE (DD-MMM-YYYY uppercase)
        if isinstance(date, str):
            date_str = date
        else:
            date_str = date.strftime("%d-%b-%Y").upper()
        
        # NSE file naming format: ddMMMyyyy (e.g., 11JAN2024)
        file_date = date_str.replace('-', '').upper()
        
        # The download URL pattern for Combined OI
        # Note: NSE may use different patterns, this is the most common one
        download_url = f"{self.base_url}/api/reports?archives=%5B%7B%22name%22%3A%22F%26O%20-%20Combine%20Open%20Interest%20across%20exchanges%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D&date={date_str}&type=equity&mode=single"
        
        # Alternative direct file URL (if above doesn't work)
        # This is based on the old pattern that might still work for some files
        alt_url = f"https://archives.nseindia.com/content/nsccl/fao_combine_oi_{file_date}.csv"
        
        try:
            # Try the API endpoint first
            # print(f"Attempting to download Combined OI for {date_str}...")
            response = self.session.get(download_url, timeout=15)
            
            # If API doesn't work, try direct file URL
            if response.status_code != 200:
                print(f"API failed, trying direct URL...")
                response = self.session.get(alt_url, timeout=15)
            
            response.raise_for_status()
            
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Save the file
            filename = f"combined_oi_{file_date}.csv"
            # print(filename, file_date)
            filepath = os.path.join(output_dir, filename)
            import io
            import zipfile
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                    csv_filename = zip_file.namelist()[0]
                    # print(csv_filename)
                    with zip_file.open(csv_filename) as csv_file:
                        import pandas as pd
                        df = pd.read_csv(csv_file)
                        # print(df)
                    filename = f"combined_oi_{pd.to_datetime(file_date).strftime('%Y%m%d')}.csv"
                    filepath = os.path.join("./NSE_Downloads/FO_Combined_OI", filename)
                    df.to_csv(filepath, index=False)
                
                    # file_size = os.path.getsize(filepath) / (1024 * 1024)
                    # self.stats['total_size_mb'] += file_size
                    # self.stats['successful_downloads'] += 1

            
            # with open(filepath, 'wb') as f:
            #     import io
            #     print(response.content)
            #         # dir(io.BytesIO(response.content))
            #         # )
            #     byte_stream = io.BytesIO(response.content)
            #     f.write(byte_stream.getvalue())
            
            print(f"✓ Downloaded successfully: {filepath}")
            print(f"  File size: {len(response.content)} bytes")
            return filepath
            
        except requests.exceptions.HTTPError as e:
            print(f"✗ HTTP Error: {e}")
            print(f"  Status Code: {response.status_code}")
            if response.status_code == 404:
                print(f"  File not found. Possible reasons:")
                print(f"  - {date_str} was not a trading day")
                print(f"  - Data not yet available")
                print(f"  - URL structure has changed")
        except Exception as e:
            print(f"✗ Failed to download: {str(e)}")
        
        return None
    
    def download_date_range(self, start_date, end_date, output_dir="./nse_data"):
        """Download Combined OI for a date range"""
        current = start_date
        successful = []
        failed = []
        
        while current <= end_date:
            # Skip weekends (Saturday=5, Sunday=6)
            if current.weekday() < 5:  
                result = self.download_combined_oi(current, output_dir)
                if result:
                    successful.append(current.strftime("%d-%b-%Y"))
                else:
                    failed.append(current.strftime("%d-%b-%Y"))
            
            current += timedelta(days=1)
        
        print(f"\n{'='*50}")
        print(f"Download Summary:")
        print(f"  Successful: {len(successful)}")
        print(f"  Failed: {len(failed)}")
        if failed:
            print(f"  Failed dates: {', '.join(failed)}")
        print(f"{'='*50}\n")
        
        return successful, failed


# Example usage
if __name__ == "__main__":
    downloader = NSECombinedOIDownloader()
    
    # Initialize session (required)
    if not downloader.init_session():
        print("Failed to initialize. Exiting.")
        exit(1)
    
    # # Example 1: Download specific date
    # print("\nExample 1: Download for specific date")
    # downloader.download_combined_oi(date="11-JAN-2024")
    
    # # Example 2: Download previous trading day
    # print("\nExample 2: Download for previous trading day")
    # downloader.download_combined_oi()
    
    # Example 3: Download date range
    print("\nExample 3: Download date range")
    start = datetime(2024, 7, 8)
    end = datetime(2025, 12, 31)
    # y,m,d
    print(end)
    downloader.download_date_range(start, end)
    
    print("\nAll downloads complete!")
