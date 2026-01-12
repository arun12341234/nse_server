"""
NSE Cash Market (CM) Bhavcopy Downloader
Real-time ready - Downloads current day and historical CM bhavcopy
Supports UDiFF format (post July 8, 2024) and old format (pre July 8, 2024)
"""

import requests
import pandas as pd
import zipfile
import io
from datetime import datetime, timedelta
import os
import time
from typing import Optional, List
import calendar


class CMBhavcopyDownloader:
    """Download Cash Market (CM) bhavcopy data from NSE"""
    
    def __init__(self, output_dir: str = r".\NSE_Downloads\CM_Bhavcopy"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # UDiFF format started from July 8, 2024
        self.udiff_start_date = datetime(2024, 7, 8)
        
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
    
    def is_trading_day(self, date: datetime) -> bool:
        """Check if date is a trading day (weekday)"""
        return date.weekday() < 5  # Monday=0, Friday=4
    
    def download_udiff_cm_bhavcopy(self, date: datetime) -> Optional[pd.DataFrame]:
        """
        Download UDiFF format CM bhavcopy (post July 8, 2024)
        URL: https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv.zip
        """
        date_str = date.strftime("%Y%m%d")
        
        url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"
        
        print(f"Downloading CM UDiFF bhavcopy for {date.strftime('%Y-%m-%d')}...")
        print(f"URL: {url}")
        
        try:
            response = self.session.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                # Extract zip and read CSV
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                    csv_filename = zip_file.namelist()[0]
                    with zip_file.open(csv_filename) as csv_file:
                        df = pd.read_csv(csv_file)
                
                print(f"✓ Downloaded {len(df)} records")
                
                # Save to disk
                output_file = os.path.join(
                    self.output_dir, 
                    f"CM_UDiFF_{date.strftime('%Y%m%d')}.csv"
                )
                df.to_csv(output_file, index=False)
                print(f"✓ Saved to {output_file}")
                
                return output_file
            else:
                print(f"✗ Failed: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            print(f"✗ Error: {e}")
            return None
    
    def download_old_cm_bhavcopy(self, date: datetime) -> Optional[pd.DataFrame]:
        """
        Download old format CM bhavcopy (pre July 8, 2024)
        URL: https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_DDMMYYYY.csv
        """
        date_str = date.strftime("%d%m%Y")
        
        url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
        
        print(f"Downloading CM OLD bhavcopy for {date.strftime('%Y-%m-%d')}...")
        print(f"URL: {url}")
        
        try:
            response = self.session.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                df = pd.read_csv(io.StringIO(response.text))
                
                print(f"✓ Downloaded {len(df)} records")
                
                # Save to disk
                output_file = os.path.join(
                    self.output_dir, 
                    f"CM_OLD_{date.strftime('%Y%m%d')}.csv"
                )
                df.to_csv(output_file, index=False)
                print(f"✓ Saved to {output_file}")
                
                return output_file
            else:
                print(f"✗ Failed: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            print(f"✗ Error: {e}")
            return None
    
    def download_today(self) -> Optional[pd.DataFrame]:
        """Download today's CM bhavcopy (for real-time use case)"""
        today = datetime.now()
        
        if not self.is_trading_day(today):
            print(f"⚠️  Today ({today.strftime('%A')}) is not a trading day")
            print("Trying to download previous trading day...")
            
            # Find previous trading day
            check_date = today - timedelta(days=1)
            while not self.is_trading_day(check_date):
                check_date -= timedelta(days=1)
            
            return self.download_date(check_date)
        
        return self.download_date(today)
    
    def download_date(self, date: datetime) -> Optional[pd.DataFrame]:
        """Download CM bhavcopy for a specific date (auto-detects format)"""
        
        # Skip weekends
        if not self.is_trading_day(date):
            print(f"⊗ Skipping {date.strftime('%Y-%m-%d')} (weekend)")
            return None
        
        # Check if date is in future
        if date > datetime.now():
            print(f"⊗ Skipping {date.strftime('%Y-%m-%d')} (future date)")
            return None
        
        # Use appropriate format based on date
        if date >= self.udiff_start_date:
            return self.download_udiff_cm_bhavcopy(date)
        else:
            return self.download_old_cm_bhavcopy(date)
    
    def download_date_range(self, start_date: datetime, end_date: datetime, 
                           delay: float = 1.0) -> List[pd.DataFrame]:
        """Download CM bhavcopy for a date range"""
        all_data = []
        current_date = start_date
        success_count = 0
        fail_count = 0
        skip_count = 0
        
        print("=" * 80)
        print(f"DOWNLOADING CM BHAVCOPY FROM {start_date.strftime('%Y-%m-%d')} TO {end_date.strftime('%Y-%m-%d')}")
        print("=" * 80)
        
        while current_date <= end_date:
            df = self.download_date(current_date)
            
            if df is not None:
                all_data.append(df)
                success_count += 1
            elif self.is_trading_day(current_date) and current_date <= datetime.now():
                fail_count += 1
            else:
                skip_count += 1
            
            current_date += timedelta(days=1)
            time.sleep(delay)  # Respect rate limits
        
        print("\n" + "=" * 80)
        print(f"DOWNLOAD COMPLETE")
        print(f"Success: {success_count} | Failed: {fail_count} | Skipped: {skip_count}")
        print("=" * 80)
        
        return all_data
    
    def download_last_n_days(self, n: int = 5) -> List[pd.DataFrame]:
        """Download last N trading days"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=n*2)  # Account for weekends
        return self.download_date_range(start_date, end_date)
    
    def get_stock_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Extract data for a specific stock symbol"""
        
        # Try different column names for symbol
        symbol_columns = ['SYMBOL', 'TckrSymb', 'FinInstrmId']
        
        for col in symbol_columns:
            if col in df.columns:
                return df[df[col] == symbol]
        
        return pd.DataFrame()
    
    def get_top_gainers(self, df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
        """Get top N gainers from bhavcopy"""
        
        # Try to find relevant columns
        if 'PRCNTCHNG' in df.columns:
            return df.nlargest(n, 'PRCNTCHNG')[['SYMBOL', 'CLOSE', 'PRCNTCHNG']]
        elif 'PctChng' in df.columns:
            return df.nlargest(n, 'PctChng')[['TckrSymb', 'ClsPric', 'PctChng']]
        
        return pd.DataFrame()
    
    def get_top_losers(self, df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
        """Get top N losers from bhavcopy"""
        
        if 'PRCNTCHNG' in df.columns:
            return df.nsmallest(n, 'PRCNTCHNG')[['SYMBOL', 'CLOSE', 'PRCNTCHNG']]
        elif 'PctChng' in df.columns:
            return df.nsmallest(n, 'PctChng')[['TckrSymb', 'ClsPric', 'PctChng']]
        
        return pd.DataFrame()
    
    def get_most_active(self, df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
        """Get most active stocks by volume"""
        
        if 'TOTTRDQTY' in df.columns:
            return df.nlargest(n, 'TOTTRDQTY')[['SYMBOL', 'CLOSE', 'TOTTRDQTY']]
        elif 'TtlTradgVol' in df.columns:
            return df.nlargest(n, 'TtlTradgVol')[['TckrSymb', 'ClsPric', 'TtlTradgVol']]
        
        return pd.DataFrame()
    
    def consolidate_data(self, data_list: List[pd.DataFrame], 
                        output_file: str = "consolidated_cm_bhavcopy.csv") -> pd.DataFrame:
        """Consolidate multiple bhavcopy dataframes"""
        
        if not data_list:
            print("No data to consolidate")
            return None
        
        print(f"\nConsolidating {len(data_list)} files...")
        consolidated = pd.concat(data_list, ignore_index=True)
        
        output_path = os.path.join(self.output_dir, output_file)
        consolidated.to_csv(output_path, index=False)
        
        print(f"✓ Consolidated {len(consolidated)} records")
        print(f"✓ Saved to {output_path}")
        
        return consolidated


def main():
    """Example usage for CM Bhavcopy"""
    
    downloader = CMBhavcopyDownloader()
    
    print("=" * 80)
    print("NSE CASH MARKET (CM) BHAVCOPY DOWNLOADER")
    print("Real-time ready - UDiFF & Old Format Support")
    print("=" * 80)
    print("\nOptions:")
    print("1. Download TODAY's CM bhavcopy (Real-time)")
    print("2. Download YESTERDAY's CM bhavcopy")
    print("3. Download LAST WEEK")
    print("4. Download LAST MONTH")
    print("5. Download LAST 5 TRADING DAYS")
    print("6. Download SPECIFIC DATE")
    print("7. Download DATE RANGE")
    print("8. Download ENTIRE YEAR")
    print()
    
    choice = input("Enter your choice (1-8): ").strip()
    
    if choice == "1":
        # Today - Real-time use case
        print("\n🔴 DOWNLOADING TODAY'S DATA (Real-time)")
        df = downloader.download_today()
        
        if df is not None:
            print("\n📊 Quick Stats:")
            print(f"Total records: {len(df)}")
            
            print("\n🔥 Top 5 Gainers:")
            print(downloader.get_top_gainers(df, 5))
            
            print("\n📉 Top 5 Losers:")
            print(downloader.get_top_losers(df, 5))
            
            print("\n📈 Most Active:")
            print(downloader.get_most_active(df, 5))
    
    elif choice == "2":
        # Yesterday
        yesterday = datetime.now() - timedelta(days=1)
        downloader.download_date(yesterday)
    
    elif choice == "3":
        # Last week
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        downloader.download_date_range(start_date, end_date)
    
    elif choice == "4":
        # Last month
        today = datetime.now()
        last_month = today.month - 1 if today.month > 1 else 12
        year = today.year if today.month > 1 else today.year - 1
        
        last_day = calendar.monthrange(year, last_month)[1]
        start_date = datetime(year, last_month, 1)
        end_date = datetime(year, last_month, last_day)
        
        downloader.download_date_range(start_date, end_date)
    
    elif choice == "5":
        # Last 5 trading days
        print("\n📅 Downloading last 5 trading days...")
        data_list = downloader.download_last_n_days(5)
        
        if data_list:
            consolidate = input("\nConsolidate into single file? (y/n): ").strip().lower()
            if consolidate == 'y':
                downloader.consolidate_data(data_list, "last_5_days_cm.csv")
    
    elif choice == "6":
        # Specific date
        date_str = input("Enter date (YYYY-MM-DD): ").strip()
        date = datetime.strptime(date_str, "%Y-%m-%d")
        downloader.download_date(date)
    
    elif choice == "7":
        # Date range
        start_str = input("Enter start date (YYYY-MM-DD): ").strip()
        end_str = input("Enter end date (YYYY-MM-DD): ").strip()
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_str, "%Y-%m-%d")
        
        data_list = downloader.download_date_range(start_date, end_date)
        
        if data_list:
            consolidate = input("\nConsolidate into single file? (y/n): ").strip().lower()
            if consolidate == 'y':
                downloader.consolidate_data(data_list)
    
    elif choice == "8":
        # Entire year
        year = int(input("Enter year (e.g., 2024): ").strip())
        
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
        
        if end_date > datetime.now():
            end_date = datetime.now()
        
        print(f"\n⚠️  This will download CM data for entire year {year}")
        confirm = input("Continue? (y/n): ").strip().lower()
        
        if confirm == 'y':
            data_list = downloader.download_date_range(start_date, end_date)
            
            if data_list:
                consolidate = input("\nConsolidate into single file? (y/n): ").strip().lower()
                if consolidate == 'y':
                    downloader.consolidate_data(data_list, f"CM_{year}_consolidated.csv")
    
    else:
        print("Invalid choice")


if __name__ == "__main__":
    main()













































# """
# NSE F&O Historical Bhavcopy Downloader
# Downloads UDiFF format bhavcopy files (post July 8, 2024) and old format (pre July 8, 2024)
# Supports data from 2000 to present
# """

# import requests
# import pandas as pd
# import zipfile
# import io
# from datetime import datetime, timedelta
# import os
# import time
# from typing import Optional, List
# import calendar


# class NSEBhavcopyDownloader:
#     """Download historical F&O bhavcopy data from NSE"""
    
#     def __init__(self, output_dir: str = r".\NSE_Downloads\CM_UDiFF_Bhavcopy"):
#         self.output_dir = output_dir
#         os.makedirs(output_dir, exist_ok=True)
        
#         # UDiFF format started from July 8, 2024
#         self.udiff_start_date = datetime(2024, 7, 8)
        
#         self.session = requests.Session()
#         self.headers = {
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
#             'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#             'Accept-Language': 'en-US,en;q=0.5',
#             'Accept-Encoding': 'gzip, deflate, br',
#             'Connection': 'keep-alive',
#         }
    
#     def is_trading_day(self, date: datetime) -> bool:
#         """Check if date is a trading day (weekday)"""
#         return date.weekday() < 5  # Monday=0, Friday=4
    
#     def download_udiff_bhavcopy(self, date: datetime, segment: str = "CM") -> Optional[pd.DataFrame]:
#         """
#         Download UDiFF format bhavcopy (post July 8, 2024)
        
#         Segments: FO (Futures & Options), CM (Cash Market), CD (Currency), COM (Commodity)
#         """
#         date_str = date.strftime("%Y%m%d")
        
#         # UDiFF URL format
#         url = f"https://nsearchives.nseindia.com/content/{segment.lower()}/BhavCopy_NSE_{segment}_0_0_0_{date_str}_F_0000.csv.zip"
        
#         print(f"Downloading UDiFF {segment} bhavcopy for {date.strftime('%Y-%m-%d')}...")
        
#         try:
#             response = self.session.get(url, headers=self.headers, timeout=30)
            
#             if response.status_code == 200:
#                 # Extract zip and read CSV
#                 with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
#                     csv_filename = zip_file.namelist()[0]
#                     with zip_file.open(csv_filename) as csv_file:
#                         df = pd.read_csv(csv_file)
                
#                 print(f"✓ Downloaded {len(df)} records")
                
#                 # Save to disk
#                 output_file = os.path.join(
#                     self.output_dir, 
#                     f"{segment}_UDiFF_{date.strftime('%Y%m%d')}.csv"
#                 )
#                 df.to_csv(output_file, index=False)
#                 print(f"✓ Saved to {output_file}")
                
#                 return df
#             else:
#                 print(f"✗ Failed: HTTP {response.status_code}")
#                 return None
                
#         except Exception as e:
#             print(f"✗ Error: {e}")
#             return None
    
#     def download_old_bhavcopy(self, date: datetime) -> Optional[pd.DataFrame]:
#         """
#         Download old format bhavcopy (pre July 8, 2024)
        
#         Old URL format:
#         https://archives.nseindia.com/content/historical/DERIVATIVES/2024/JAN/fo01JAN2024bhav.csv.zip
#         https://archives.nseindia.com/content/historical/DERIVATIVES/{YEAR}/{MONTH}/fo{DD}{MONTH}{YEAR}bhav.csv.zip
#         """
#         year = date.strftime("%Y")
#         month = date.strftime("%b").upper()
#         day = date.strftime("%d")
#         date_suffix = date.strftime("%d%b%Y").upper()
        
#         # Old format URL

#         url = f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{year}/{month}/fo{date_suffix}bhav.csv.zip"
        
#         print(f"{url} Downloading OLD F&O bhavcopy for {date.strftime('%Y-%m-%d')}...")
        
#         try:
#             response = self.session.get(url, headers=self.headers, timeout=30)
            
#             if response.status_code == 200:
#                 # Extract zip and read CSV
#                 with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
#                     csv_filename = zip_file.namelist()[0]
#                     with zip_file.open(csv_filename) as csv_file:
#                         df = pd.read_csv(csv_file)
                
#                 print(f"✓ Downloaded {len(df)} records")
                
#                 # Save to disk
#                 output_file = os.path.join(
#                     self.output_dir, 
#                     f"FO_OLD_{date.strftime('%Y%m%d')}.csv"
#                 )
#                 df.to_csv(output_file, index=False)
#                 print(f"✓ Saved to {output_file}")
                
#                 return df
#             else:
#                 print(f"✗ Failed: HTTP {response.status_code}")
#                 return None
                
#         except Exception as e:
#             print(f"✗ Error: {e}")
#             return None
    
#     def download_date(self, date: datetime, segment: str = "FO") -> Optional[pd.DataFrame]:
#         """Download bhavcopy for a specific date (auto-detects format)"""
        
#         # Skip weekends
#         if not self.is_trading_day(date):
#             print(f"⊗ Skipping {date.strftime('%Y-%m-%d')} (weekend)")
#             return None
        
#         # Check if date is in future
#         if date > datetime.now():
#             print(f"⊗ Skipping {date.strftime('%Y-%m-%d')} (future date)")
#             return None
        
#         # Use appropriate format based on date
#         if date >= self.udiff_start_date:
#             return self.download_udiff_bhavcopy(date, segment)
#         else:
#             return self.download_old_bhavcopy(date)
    
#     def download_date_range(self, start_date: datetime, end_date: datetime, 
#                            segment: str = "FO", delay: float = 1.0) -> List[pd.DataFrame]:
#         """
#         Download bhavcopy for a date range
        
#         Args:
#             start_date: Start date
#             end_date: End date
#             segment: FO/CM/CD/COM
#             delay: Delay between downloads (seconds)
#         """
#         all_data = []
#         current_date = start_date
#         success_count = 0
#         fail_count = 0
#         skip_count = 0
        
#         print("=" * 80)
#         print(f"DOWNLOADING BHAVCOPY FROM {start_date.strftime('%Y-%m-%d')} TO {end_date.strftime('%Y-%m-%d')}")
#         print("=" * 80)
        
#         while current_date <= end_date:
#             df = self.download_date(current_date, segment)
            
#             if df is not None:
#                 all_data.append(df)
#                 success_count += 1
#             elif self.is_trading_day(current_date) and current_date <= datetime.now():
#                 fail_count += 1
#             else:
#                 skip_count += 1
            
#             current_date += timedelta(days=1)
#             time.sleep(delay)  # Respect rate limits
        
#         print("\n" + "=" * 80)
#         print(f"DOWNLOAD COMPLETE")
#         print(f"Success: {success_count} | Failed: {fail_count} | Skipped: {skip_count}")
#         print("=" * 80)
        
#         return all_data
    
#     def download_month(self, year: int, month: int, segment: str = "FO") -> List[pd.DataFrame]:
#         """Download entire month of bhavcopy data"""
        
#         # Get last day of month
#         last_day = calendar.monthrange(year, month)[1]
        
#         start_date = datetime(year, month, 1)
#         end_date = datetime(year, month, last_day)
        
#         return self.download_date_range(start_date, end_date, segment)
    
#     def download_year(self, year: int, segment: str = "CM") -> List[pd.DataFrame]:
#         """Download entire year of bhavcopy data"""
        
#         start_date = datetime(year, 1, 1)
#         end_date = datetime(year, 12, 31)
        
#         # Don't go beyond today
#         if end_date > datetime.now():
#             end_date = datetime.now()
        
#         return self.download_date_range(start_date, end_date, segment)
    
#     def consolidate_data(self, data_list: List[pd.DataFrame], 
#                         output_file: str = "consolidated_bhavcopy.csv") -> pd.DataFrame:
#         """Consolidate multiple bhavcopy dataframes into one"""
        
#         if not data_list:
#             print("No data to consolidate")
#             return None
        
#         print(f"\nConsolidating {len(data_list)} files...")
#         consolidated = pd.concat(data_list, ignore_index=True)
        
#         output_path = os.path.join(self.output_dir, output_file)
#         consolidated.to_csv(output_path, index=False)
        
#         print(f"✓ Consolidated {len(consolidated)} records")
#         print(f"✓ Saved to {output_path}")
        
#         return consolidated
    
#     def parse_udiff_columns(self, df: pd.DataFrame) -> pd.DataFrame:
#         """Parse and standardize UDiFF format columns"""
        
#         # Common UDiFF columns for F&O
#         udiff_mapping = {
#             'TradDt': 'TIMESTAMP',
#             'BizDt': 'BUSINESS_DATE',
#             'Sgmt': 'SEGMENT',
#             'Src': 'SOURCE',
#             'FinInstrmTp': 'INSTRUMENT_TYPE',
#             'FinInstrmId': 'SYMBOL',
#             'ISIN': 'ISIN',
#             'TckrSymb': 'TICKER_SYMBOL',
#             'SctySrs': 'SERIES',
#             'XpryDt': 'EXPIRY_DATE',
#             'StrkPric': 'STRIKE_PRICE',
#             'OptnTp': 'OPTION_TYPE',
#             'OpnPric': 'OPEN_PRICE',
#             'HghPric': 'HIGH_PRICE',
#             'LwPric': 'LOW_PRICE',
#             'ClsPric': 'CLOSE_PRICE',
#             'LastPric': 'LAST_PRICE',
#             'PrvsClsgPric': 'PREV_CLOSE',
#             'UndrlygPric': 'UNDERLYING_PRICE',
#             'SttlmPric': 'SETTLEMENT_PRICE',
#             'OpnIntrst': 'OPEN_INTEREST',
#             'ChngInOpnIntrst': 'CHANGE_IN_OI',
#             'TtlTradgVol': 'VOLUME',
#             'TtlTrfVal': 'TURNOVER',
#             'TtlNbOfTrds': 'NO_OF_TRADES',
#             'SsnId': 'SESSION_ID'
#         }
        
#         # Rename columns if they exist
#         df_renamed = df.copy()
#         for old_col, new_col in udiff_mapping.items():
#             if old_col in df_renamed.columns:
#                 df_renamed.rename(columns={old_col: new_col}, inplace=True)
        
#         return df_renamed
    
#     def filter_nifty_options(self, df: pd.DataFrame) -> pd.DataFrame:
#         """Filter for NIFTY options only"""
        
#         # Try to identify NIFTY in different column names
#         symbol_columns = ['SYMBOL', 'TICKER_SYMBOL', 'FinInstrmId', 'TckrSymb']
        
#         for col in symbol_columns:
#             if col in df.columns:
#                 return df[df[col].str.contains('NIFTY', na=False, case=False)]
        
#         return df
    
#     def get_latest_files(self, n: int = 5) -> List[str]:
#         """Get list of latest downloaded files"""
        
#         files = [f for f in os.listdir(self.output_dir) if f.endswith('.csv')]
#         files.sort(reverse=True)
        
#         return files[:n]


# def main():
#     """Example usage"""
    
#     downloader = NSEBhavcopyDownloader()
    
#     print("=" * 80)
#     print("NSE F&O BHAVCOPY DOWNLOADER (UDiFF FORMAT)")
#     print("Supports data from 2000 to present")
#     print("=" * 80)
#     print("\nOptions:")
#     print("1. Download today's bhavcopy")
#     print("2. Download yesterday's bhavcopy")
#     print("3. Download last week")
#     print("4. Download last month")
#     print("5. Download specific date")
#     print("6. Download date range")
#     print("7. Download entire year")
#     print("8. Download from 2000 to present (WARNING: Large download)")
#     print()
    
#     choice = input("Enter your choice (1-8): ").strip()
    
#     if choice == "1":
#         # Today
#         today = datetime.now()
#         downloader.download_date(today)
    
#     elif choice == "2":
#         # Yesterday
#         yesterday = datetime.now() - timedelta(days=1)
#         downloader.download_date(yesterday)
    
#     elif choice == "3":
#         # Last week
#         end_date = datetime.now()
#         start_date = end_date - timedelta(days=7)
#         downloader.download_date_range(start_date, end_date)
    
#     elif choice == "4":
#         # Last month
#         today = datetime.now()
#         last_month = today.month - 1 if today.month > 1 else 12
#         year = today.year if today.month > 1 else today.year - 1
#         downloader.download_month(year, last_month)
    
#     elif choice == "5":
#         # Specific date
#         date_str = input("Enter date (YYYY-MM-DD): ").strip()
#         date = datetime.strptime(date_str, "%Y-%m-%d")
#         downloader.download_date(date)
    
#     elif choice == "6":
#         # Date range
#         start_str = input("Enter start date (YYYY-MM-DD): ").strip()
#         end_str = input("Enter end date (YYYY-MM-DD): ").strip()
#         start_date = datetime.strptime(start_str, "%Y-%m-%d")
#         end_date = datetime.strptime(end_str, "%Y-%m-%d")
        
#         data_list = downloader.download_date_range(start_date, end_date)
        
#         if data_list:
#             consolidate = input("\nConsolidate into single file? (y/n): ").strip().lower()
#             if consolidate == 'y':
#                 downloader.consolidate_data(data_list)
    
#     elif choice == "7":
#         # Entire year
#         year = int(input("Enter year (e.g., 2023): ").strip())
        
#         print(f"\n⚠️  This will download entire year {year}")
#         print("   Estimated time: 5-10 minutes")
#         confirm = input("Continue? (y/n): ").strip().lower()
        
#         if confirm == 'y':
#             data_list = downloader.download_year(year)
            
#             if data_list:
#                 consolidate = input("\nConsolidate into single file? (y/n): ").strip().lower()
#                 if consolidate == 'y':
#                     downloader.consolidate_data(data_list, f"FO_{year}_consolidated.csv")
    
#     elif choice == "8":
#         # From 2000 to present
#         print("\n⚠️  WARNING: This will download 24+ years of data")
#         print("   Estimated time: 2-4 hours")
#         print("   Estimated size: 1-2 GB")
#         confirm = input("\nAre you sure? (yes/no): ").strip().lower()
        
#         if confirm == 'yes':
#             start_date = datetime(2000, 1, 1)
#             end_date = datetime.now()
            
#             # Download by year to manage better
#             data_by_year = {}
#             current_year = start_date.year
            
#             while current_year <= end_date.year:
#                 print(f"\n{'='*80}")
#                 print(f"DOWNLOADING YEAR: {current_year}")
#                 print(f"{'='*80}")
                
#                 year_data = downloader.download_year(current_year)
#                 data_by_year[current_year] = year_data
                
#                 # Consolidate each year
#                 if year_data:
#                     downloader.consolidate_data(year_data, f"FO_{current_year}_consolidated.csv")
                
#                 current_year += 1
            
#             print("\n" + "="*80)
#             print("COMPLETE HISTORICAL DOWNLOAD FINISHED")
#             print(f"Data saved in: {downloader.output_dir}")
#             print("="*80)
    
#     else:
#         print("Invalid choice")
    
#     # Show downloaded files
#     print("\n" + "="*80)
#     print("LATEST DOWNLOADED FILES:")
#     print("="*80)
#     for file in downloader.get_latest_files(10):
#         filepath = os.path.join(downloader.output_dir, file)
#         size = os.path.getsize(filepath) / (1024 * 1024)  # MB
#         print(f"  {file} ({size:.2f} MB)")


# if __name__ == "__main__":
#     main()