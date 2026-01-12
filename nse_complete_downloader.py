"""
NSE Comprehensive Data Downloader
Downloads all available NSE data files for a given date or date range
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import zipfile
import io
from pathlib import Path

class NSEDataDownloader:
    def __init__(self, output_dir="NSE_Downloads"):
        """Initialize the NSE Data Downloader"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for different data types
        self.subdirs = {
            'fo_bhavcopy': self.output_dir / 'FO_Bhavcopy',
            'fo_participant_oi': self.output_dir / 'FO_Participant_OI',
            'fo_participant_volume': self.output_dir / 'FO_Participant_Volume',
            'fo_combined_oi': self.output_dir / 'FO_Combined_OI',
            'fii_statistics': self.output_dir / 'FII_Statistics',
            'equity_bhavcopy': self.output_dir / 'Equity_Bhavcopy',
            'equity_deliverable': self.output_dir / 'Equity_Deliverable',
            'indices': self.output_dir / 'Indices',
            'vix': self.output_dir / 'VIX',
            'cm_udiff': self.output_dir / 'CM_UDiFF_Bhavcopy'
        }
        
        for subdir in self.subdirs.values():
            subdir.mkdir(exist_ok=True)
        
        # Session with headers to mimic browser
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })
        
        self.stats = {
            'total_attempted': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
    
    def is_trading_day(self, date):
        """Check if the date is a weekday (potential trading day)"""
        return date.weekday() < 5
    
    def format_date(self, date, format_type='DDMMMYYYYupper'):
        """Format date in various NSE-required formats"""
        formats = {
            'DDMMMYYYYupper': date.strftime('%d%b%Y').upper(),  # 13DEC2024
            'DDMMMYYYYlower': date.strftime('%d%b%Y'),           # 13Dec2024
            'DDMMYYYY': date.strftime('%d%m%Y'),                 # 13122024
            'DDMMMYY': date.strftime('%d%b%y').upper(),          # 13DEC24
            'YYYYMMDD': date.strftime('%Y%m%d'),                 # 20241213
            'DD-MMM-YYYY': date.strftime('%d-%b-%Y').upper(),    # 13-DEC-2024
        }
        return formats.get(format_type, date.strftime('%d%m%Y'))
    
    def download_file(self, url, output_path, description=""):
        """Download a file from URL and save to output_path"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            print(f"  [+] {description or os.path.basename(output_path)}")
            self.stats['successful'] += 1
            # return True
            return output_path
            
        except requests.exceptions.RequestException as e:
            print(f"  [-] Failed: {description or os.path.basename(output_path)} - {str(e)[:50]}")
            self.stats['failed'] += 1
            return None
    
    def download_fo_bhavcopy(self, date):
        """Download F&O Bhavcopy (CSV format)"""
        date_str = self.format_date(date, 'DDMMMYYYYupper')
        filename = f"fo{date_str}bhav.csv.zip"

        # url = f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{date.year}/{date.strftime('%b').upper()}/{filename}"
        output_path = self.subdirs['fo_bhavcopy'] / filename
        
        if output_path.exists():
            self.stats['skipped'] += 1
            return True
        
        self.stats['total_attempted'] += 1
        from bc import NSEBhavcopyDownloader
        downloader = NSEBhavcopyDownloader()
        downloader.download_date(date)
        # return self.download_file(url, output_path, f"F&O Bhavcopy: {filename}")
    
    def download_fo_participant_oi(self, date):
        """Download F&O Participant-wise Open Interest"""
        date_str = self.format_date(date, 'DD-MMM-YYYY')
        filename = f"FOPARTWISEOIdata_{date_str}.csv"
        # url = f"https://archives.nseindia.com/content/nsccl/{filename}"
        output_path = self.subdirs['fo_participant_oi'] / filename
        
        if output_path.exists():
            self.stats['skipped'] += 1
            return True
        
        self.stats['total_attempted'] += 1
        # return self.download_file(url, output_path, f"Participant OI: {filename}")
        from nse_participant_oi_downloader import NSEAPIDownloader
        downloader = NSEAPIDownloader()
        downloader.download_participant_oi(f"{date_str}")


    
    def download_fo_participant_volume(self, date):
        """Download F&O Participant-wise Trading Volume"""
        date_str = self.format_date(date, 'DD-MMM-YYYY')
        filename = f"FOPARTWISETDdata_{date_str}.csv"
        # url = f"https://archives.nseindia.com/content/nsccl/{filename}"
        output_path = self.subdirs['fo_participant_volume'] / filename
        
        if output_path.exists():
            self.stats['skipped'] += 1
            return True
        
        self.stats['total_attempted'] += 1
        # return self.download_file(url, output_path, f"Participant Volume: {filename}")
        from nse_participant_tv_downloader import NSEAPIDownloader
        downloader = NSEAPIDownloader()
        downloader.download_participant_tv(f"{date_str}")
    
    def download_fo_combined_oi(self, date):
        """Download F&O Combined Open Interest across exchanges"""
        date_str = self.format_date(date, 'DD-MMM-YYYY')
        filename = f"combineoi_oi_{date_str}.csv"
        # url = f"https://archives.nseindia.com/content/nsccl/{filename}"
        output_path = self.subdirs['fo_combined_oi'] / filename
        
        if output_path.exists():
            self.stats['skipped'] += 1
            return True
        
        self.stats['total_attempted'] += 1
        # return self.download_file(url, output_path, f"Combined OI: {filename}")
        from nse_combined_oi_downloader import NSECombinedOIDownloader
        downloader = NSECombinedOIDownloader()
        downloader.download_combined_oi(date=f"{date_str}")
    
    def download_fii_statistics(self, date):
        """Download FII Derivatives Statistics"""
        date_str = self.format_date(date, 'DD-MMM-YYYY')
        filename = f"fii_stats_{date_str}.xls"
        # url = f"https://archives.nseindia.com/content/fo/{filename}"
        output_path = self.subdirs['fii_statistics'] / filename
        
        if output_path.exists():
            self.stats['skipped'] += 1
            return True
        
        self.stats['total_attempted'] += 1
        # return self.download_file(url, output_path, f"FII Statistics: {filename}")
        from nse_fii_statistics_downloader import NSEAPIDownloader
        downloader = NSEAPIDownloader()
        downloader.download_fii_statistics(date=f"{date_str}")
    
    def download_equity_bhavcopy(self, date):
        """Download Equity Bhavcopy (old format, pre-July 2024)"""
        date_str = self.format_date(date, 'DDMMYYYY')
        filename = f"cm{date_str}bhav.csv.zip"
        url = f"https://archives.nseindia.com/products/content/{filename}"
        output_path = self.subdirs['equity_bhavcopy'] / filename
        
        if output_path.exists():
            self.stats['skipped'] += 1
            return True
        
        self.stats['total_attempted'] += 1
        return self.download_file(url, output_path, f"Equity Bhavcopy: {filename}")
    
    def download_cm_udiff_bhavcopy(self, date):
        """Download CM-UDiFF Common Bhavcopy (new format, post-July 2024)"""
        # UDiFF format started from July 8, 2024
        if date < datetime(2024, 7, 8):
            return True  # Skip for dates before UDiFF format
        
        date_str = self.format_date(date, 'DD-MMM-YYYY')
        filename = f"BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"
        url = f"https://nsearchives.nseindia.com/products/dynaContent/common/productsSymbolMapping/{filename}"
        output_path = self.subdirs['cm_udiff'] / filename
        
        if output_path.exists():
            self.stats['skipped'] += 1
            return True
        
        self.stats['total_attempted'] += 1
        return self.download_file(url, output_path, f"CM-UDiFF Bhavcopy: {filename}")
    
    def download_equity_deliverable(self, date):
        """Download Equity Deliverable Data"""
        # date_str1 = self.format_date(date, 'DDMMMYY')
        # date_str2 = self.format_date(date, 'DDMMMYYYYupper')
        
        # # Try multiple URL patterns
        # urls = [
        #     f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str1}.csv",
        #     f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str2}.csv"
        # ]
        
        # filename = f"sec_bhavdata_full_{date_str2}.csv"
        date_str = date.strftime('%d%m%Y')
        filename = f"sec_bhavdata_full_{date_str}.csv"
        urls = [f"https://archives.nseindia.com/products/content/{filename}"]
        date_str1 = self.format_date(date, 'YYYYMMDD')
        filename1 = f"sec_bhavdata_full_{date_str1}.csv"
        output_path = self.subdirs['equity_deliverable'] / filename1
        
        if output_path.exists():
            self.stats['skipped'] += 1
            return output_path
        
        self.stats['total_attempted'] += 1
        
        for url in urls:
            return self.download_file(url, output_path, f"Deliverable Data: {filename}")
            if self.download_file(url, output_path, f"Deliverable Data: {filename}"):
                return True
        
        return None
    
    def download_indices(self, date):
        """Download Indices Data"""
        date_str = self.format_date(date, 'DDMMYYYY')
        filename = f"ind_close_all_{date_str}.csv"
        url = f"https://archives.nseindia.com/content/indices/{filename}"
        date_str1 = self.format_date(date, 'YYYYMMDD')
        filename1 = f"ind_close_all_{date_str1}.csv"
        output_path = self.subdirs['indices'] / filename1
        
        if output_path.exists():
            self.stats['skipped'] += 1
            return output_path
        
        self.stats['total_attempted'] += 1
        return self.download_file(url, output_path, f"Indices: {filename}")
    
    def download_vix(self, date):
        """Download India VIX Data"""
        # date_str = self.format_date(date, 'DD-MMM-YYYY')
        # filename = f"INDIA_VIX_{date_str}.csv"
        # date_str = date.strftime('%d-%b-%Y')  # e.g., 31-Dec-2024
        # filename = f"IndiaVIX_{date_str}.csv"
        # url = f"https://archives.nseindia.com/content/indices/{filename}"
        date_str = self.format_date(date, 'DDMMMYYYY')  # e.g., 31Dec2024
        filename = f"india-vix-daily-{date_str}.csv"
        url = f"https://nsearchives.nseindia.com/content/indices/{filename}"
        output_path = self.subdirs['vix'] / filename
        
        if output_path.exists():
            self.stats['skipped'] += 1
            return True
        
        self.stats['total_attempted'] += 1
        return self.download_file(url, output_path, f"VIX: {filename}")
    
    def download_all_for_date(self, date):
        """Download all available data for a given date"""
        if not self.is_trading_day(date):
            print(f"\nSkipping {date.strftime('%Y-%m-%d')} (Weekend)")
            return
        
        print(f"\n{'='*70}")
        print(f"Downloading data for: {date.strftime('%Y-%m-%d (%A)')}")
        print(f"{'='*70}")
        
        # Download all data types
        # self.download_fo_bhavcopy(date)
        # self.download_fo_participant_oi(date)
        # self.download_fo_participant_volume(date)
        # self.download_fo_combined_oi(date)
        # self.download_fii_statistics(date)

        # self.download_equity_bhavcopy(date)
        # self.download_cm_udiff_bhavcopy(date)
        self.download_equity_deliverable(date)
        # self.download_indices(date)
        # self.download_vix(date)
        
        # Rate limiting
        time.sleep(0.5)
    
    def download_date_range(self, start_date, end_date):
        """Download data for a date range"""
        current_date = start_date
        
        print(f"\n{'#'*70}")
        print(f"NSE DATA DOWNLOADER")
        print(f"{'#'*70}")
        print(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"Output Directory: {self.output_dir.absolute()}")
        print(f"{'#'*70}")
        
        while current_date <= end_date:
            self.download_all_for_date(current_date)
            current_date += timedelta(days=1)
        
        self.print_summary()
    
    def print_summary(self):
        """Print download summary statistics"""
        print(f"\n{'='*70}")
        print(f"DOWNLOAD SUMMARY")
        print(f"{'='*70}")
        print(f"Total Attempted:  {self.stats['total_attempted']}")
        print(f"Successful:       {self.stats['successful']} ({self.stats['successful']/max(1, self.stats['total_attempted'])*100:.1f}%)")
        print(f"Failed:           {self.stats['failed']}")
        print(f"Skipped (Exists): {self.stats['skipped']}")
        print(f"{'='*70}")


def main():
    """Main function to run the downloader"""
    downloader = NSEDataDownloader()
    
    # Example 1: Download today's data
    today = datetime.now()
    # downloader.download_all_for_date(today)
    
    # Example 2: Download last 5 trading days
    # end_date = datetime.now()
    # start_date = end_date - timedelta(days=10)
    # downloader.download_date_range(start_date, end_date)
    
    # Example 3: Download specific date
    specific_date = datetime(2025, 10, 2)
    abc = downloader.download_equity_deliverable(specific_date)
    print(abc,"abc")
    
    # Example 4: Download last month
    # end_date = datetime.now()
    # start_date = end_date - timedelta(days=30)
    # downloader.download_date_range(start_date, end_date)
    
    # Example 5: Download entire year (2024)
    # start_date = datetime(2025, 12, 1)
    # end_date = today
    # # datetime.now()
    # print(downloader.download_date_range(start_date, end_date))


if __name__ == "__main__":
    main()
