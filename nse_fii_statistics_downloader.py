"""
NSE Official API Downloader
Uses NSE's official API endpoints instead of direct file URLs
This is more reliable as it's the same API the website uses
"""

import requests
import json
import urllib.parse
from datetime import datetime, timedelta
import pandas as pd
import io

class NSEAPIDownloader:
    """
    Download NSE F&O reports using official API endpoints
    This is the same API that NSE's website uses
    """
    
    def __init__(self):
        self.base_url = "https://www.nseindia.com"
        self.session = requests.Session()
        self._setup_session()
    
    def _setup_session(self):
        """Setup session with proper headers for NSE API"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.nseindia.com/all-reports-derivatives',
            'X-Requested-With': 'XMLHttpRequest'
        })
        
        # Get cookies by visiting the page first
        try:
            response = self.session.get(
                f"{self.base_url}/all-reports-derivatives",
                timeout=10
            )
            print("✓ Session initialized with cookies")
        except Exception as e:
            print(f"! Warning during session init: {e}")
    
    def _build_api_url(self, report_config, date_str):
        """
        Build NSE API URL with proper encoding
        
        Args:
            report_config: Dict with report configuration
            date_str: Date in DD-MMM-YYYY format (e.g., '24-Nov-2025')
        
        Returns:
            Complete API URL
        """
        # Convert report config to JSON and encode
        archives_json = json.dumps([report_config])
        archives_encoded = urllib.parse.quote(archives_json)
        
        # Build full URL
        url = (
            f"{self.base_url}/api/reports"
            f"?archives={archives_encoded}"
            f"&date={date_str}"
            f"&type={report_config.get('section', 'equity')}"
            f"&mode=single"
        )
        
        return url
    
    def download_combined_oi(self, date, output_file=None):
        """
        Download Combined Open Interest using NSE API
        
        Args:
            date: datetime object or date string
            output_file: Optional output filename
        
        Returns:
            DataFrame or raw data
        """
        # Format date
        if isinstance(date, str):
            try:
                if '-' in date:
                    dt = datetime.strptime(date, "%d-%b-%Y")
                else:
                    dt = datetime.strptime(date, "%d%b%Y")
            except:
                dt = datetime.strptime(date, "%Y-%m-%d")
        else:
            dt = date
        
        # NSE API expects DD-MMM-YYYY format
        date_str = dt.strftime("%d-%b-%Y")
        
        print(f"\n{'='*70}")
        print(f"Downloading Combined OI via NSE API")
        print(f"Date: {date_str}")
        print(f"{'='*70}\n")
        
        # Report configuration
        report_config = {
            "name": "F&O - Combine Open Interest across exchanges",
            "type": "archives",
            "category": "derivatives",
            "section": "equity"
        }
        
        # Build API URL
        api_url = self._build_api_url(report_config, date_str)
        
        print(f"API URL (decoded):")
        print(f"  {urllib.parse.unquote(api_url)}\n")
        
        try:
            print(f"Making API request...")
            response = self.session.get(api_url, timeout=20)
            
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                # Check content type
                content_type = response.headers.get('Content-Type', '')
                print(f"Content-Type: {content_type}")
                
                # NSE API might return JSON with download link or direct CSV
                if 'application/json' in content_type:
                    # JSON response - parse it
                    data = response.json()
                    print(f"\nAPI Response (JSON):")
                    print(json.dumps(data, indent=2)[:500] + "...")
                    
                    # Look for download URL in response
                    if isinstance(data, dict) and 'path' in data:
                        download_url = data['path']
                        print(f"\nFound download URL: {download_url}")
                        
                        # Download the actual file
                        file_response = self.session.get(
                            f"{self.base_url}{download_url}",
                            timeout=20
                        )
                        
                        if file_response.status_code == 200:
                            content = file_response.content
                        else:
                            print(f"✗ Failed to download file: {file_response.status_code}")
                            return None
                    else:
                        print("✗ Unexpected JSON response format")
                        return None
                
                elif 'text/csv' in content_type or 'application/csv' in content_type:
                    # Direct CSV response
                    content = response.content
                    print(f"✓ Received CSV directly")
                
                else:
                    # Unknown content type, try to parse anyway
                    content = response.content
                    print(f"! Unknown content type, attempting to parse...")
                
                # Save to file if specified
                if output_file is None:
                    output_file = f"combined_oi_{dt.strftime('%d%b%Y').upper()}.csv"
                
                with open(output_file, 'wb') as f:
                    f.write(content)
                
                print(f"\n✓ Saved to: {output_file}")
                print(f"  File size: {len(content):,} bytes")
                
                # Try to parse as DataFrame
                try:
                    df = pd.read_csv(io.BytesIO(content))
                    print(f"  Data shape: {df.shape}")
                    print(f"\nFirst few rows:")
                    print(df.head())
                    return df
                except Exception as e:
                    print(f"! Could not parse as CSV: {e}")
                    print(f"  Saved raw content to {output_file}")
                    return content
                
            else:
                print(f"\n✗ API request failed: {response.status_code}")
                print(f"Response: {response.text[:200]}")
                return None
                
        except Exception as e:
            print(f"\n✗ Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def download_combined_oi(self, date, output_file=None):
        """
        Download Combined Open Interest using NSE API
        
        Args:
            date: datetime object or date string
            output_file: Optional output filename
        
        Returns:
            pandas DataFrame with combined OI data
        """
        # Format date
        if isinstance(date, str):
            try:
                if '-' in date:
                    dt = datetime.strptime(date, "%d-%b-%Y")
                else:
                    dt = datetime.strptime(date, "%d%b%Y")
            except:
                dt = datetime.strptime(date, "%Y-%m-%d")
        else:
            dt = date
        
        # NSE API expects DD-MMM-YYYY format
        date_str = dt.strftime("%d-%b-%Y")
        
        print(f"\n{'='*70}")
        print(f"Downloading Combined OI via NSE API")
        print(f"Date: {date_str}")
        print(f"{'='*70}\n")
        
        # Report configuration
        report_config = {
            "name": "F&O - Combine Open Interest across exchanges",
            "type": "archives",
            "category": "derivatives",
            "section": "equity"
        }
        
        # Build API URL
        api_url = self._build_api_url(report_config, date_str)
        
        print(f"API URL: {api_url}\n")
        print(f"Making API request...")
        
        try:
            response = self.session.get(api_url, timeout=20)
            
            print(f"Response Status: {response.status_code}")
            print(f"Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
            print(f"Content-Length: {len(response.content):,} bytes")
            
            if response.status_code == 200:
                content = response.content
                
                # Determine output filename
                if output_file is None:
                    output_file = f"combined_oi_{dt.strftime('%d%b%Y').upper()}.csv"
                
                # Save to file
                with open(output_file, 'wb') as f:
                    f.write(content)
                
                print(f"\n✓ Saved to: {output_file}")
                
                # Try to parse as DataFrame
                try:
                    df = pd.read_csv(io.BytesIO(content))
                    print(f"✓ Data shape: {df.shape}")
                    print(f"✓ Columns: {list(df.columns[:5])}...")
                    
                    # Show first few rows
                    if len(df) > 0:
                        print(f"\n📊 First 3 rows:")
                        print(df.head(3).to_string(index=False, max_cols=8))
                    
                    return df
                    
                except Exception as e:
                    print(f"! Could not parse as DataFrame: {e}")
                    print(f"! Raw content saved to {output_file}")
                    return content
                
            else:
                print(f"\n✗ Failed with status code: {response.status_code}")
                if response.text:
                    print(f"Response: {response.text[:200]}")
                return None
                
        except Exception as e:
            print(f"\n✗ Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def download_fii_statistics(self, date, output_file=None):
        """
        Download Participant-wise OI using NSE API
        
        Args:
            date: datetime object or date string
            output_file: Optional output filename
        
        Returns:
            pandas DataFrame with participant OI data
        """
        if isinstance(date, str):
            try:
                if '-' in date:
                    dt = datetime.strptime(date, "%d-%b-%Y")
                else:
                    dt = datetime.strptime(date, "%d%b%Y")
            except:
                dt = datetime.strptime(date, "%Y-%m-%d")
        else:
            dt = date
        
        date_str = dt.strftime("%d-%b-%Y")
        
        print(f"\n{'='*70}")
        print(f"Downloading Participant OI via NSE API")
        print(f"Date: {date_str}")
        print(f"{'='*70}\n")
        
        report_config = {
            "name": "F&O - FII Derivatives Statistics",
            "type": "archives",
            "category": "derivatives",
            "section": "equity"
        }
        
        api_url = self._build_api_url(report_config, date_str)
        
        print(f"API URL: {api_url}\n")
        print(f"Making API request...")
        
        try:
            response = self.session.get(api_url, timeout=20)
            
            print(f"Response Status: {response.status_code}")
            print(f"Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
            print(f"Content-Length: {len(response.content):,} bytes")
            
            if response.status_code == 200:
                content = response.content
                
                # Determine output filename
                if output_file is None:
                    output_file = f"fii_statistics_{dt.strftime('%Y%m%d').upper()}.xls"
                output_dir = "./NSE_Downloads/FII_Statistics"
                import os
                os.makedirs(output_dir, exist_ok=True)
                filepath = os.path.join(output_dir, output_file)
                # Save to file
                with open(filepath, 'wb') as f:
                    # print(
                    #     content
                    # )
                    f.write(content)
                
                print(f"\n✓ Saved to: {output_file}, {filepath}")
                
                # Parse as DataFrame
                try:
                    df = pd.read_csv(io.BytesIO(content))
                    # print(f"✓ Data shape: {df.shape}")
                    # print(f"✓ Columns: {list(df.columns[:5])}...")
                    
                    # Show participant summary
                    if len(df) > 0:
                        # print(f"\n📊 Participant Summary:")
                        for idx in range(min(4, len(df))):
                            row = df.iloc[idx]
                            participant = row.iloc[0] if len(row) > 0 else 'Unknown'
                            total_long = row.iloc[-2] if len(row) > 1 else 'N/A'
                            total_short = row.iloc[-1] if len(row) > 1 else 'N/A'
                            # print(f"  {participant}: Long={total_long}, Short={total_short}")
                    
                    # return df
                    return filepath
                    
                except Exception as e:
                    print(f"! Could not parse as DataFrame: {e}")
                    print(f"! Raw content saved to {output_file}")
                    # return content
                    return filepath
                    
            else:
                print(f"\n✗ Failed with status code: {response.status_code}")
                if response.text:
                    print(f"Response: {response.text[:200]}")
                return None
                
        except Exception as e:
            print(f"\n✗ Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def list_available_reports(self, category="derivatives", section="equity"):
        """
        List all available reports for a category
        
        This is useful to discover what reports are available
        """
        print(f"\n{'='*70}")
        print(f"Available NSE Reports")
        print(f"Category: {category}, Section: {section}")
        print(f"{'='*70}\n")
        
        # Common F&O reports
        fo_reports = [
            {
                "name": "F&O - Combine Open Interest across exchanges",
                "type": "archives",
                "category": "derivatives",
                "section": "equity"
            },
            {
                "name": "F&O - Participant wise Open Interest(csv)",
                "type": "archives",
                "category": "derivatives",
                "section": "equity"
            },
            {
                "name": "F&O - Participant wise Trading Volumes(csv)",
                "type": "archives",
                "category": "derivatives",
                "section": "equity"
            },
            {
                "name": "F&O - Bhavcopy (fo.zip)",
                "type": "archives",
                "category": "derivatives",
                "section": "equity"
            },
            {
                "name": "F&O - Daily Volatility",
                "type": "archives",
                "category": "derivatives",
                "section": "equity"
            }
        ]
        
        for i, report in enumerate(fo_reports, 1):
            print(f"{i}. {report['name']}")
        
        return fo_reports
    
    def download_date_range(self, start_date, end_date, output_dir="./nse_data"):
        """Download Combined OI for a date range"""
        current = start_date
        successful = []
        failed = []
        
        while current <= end_date:
            # Skip weekends (Saturday=5, Sunday=6)
            if current.weekday() < 5:  
                result = current
                print(current)
                self.download_participant_oi(current.strftime("%d-%b-%Y"))
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
        # log_filepath = "log_participant_oi.log"
        # with open(log_filepath, 'w') as f:
        #     f.write(f"Successful: {successful},\nFailed: {failed}")

        return successful, failed


def main():
    """Example usage"""
    downloader = NSEAPIDownloader()
    
    print("\n" + "="*70)
    print("NSE Official API Downloader")
    print("="*70)
    
    # Example 1: Download Combined OI
    print("\nExample 1: Download Combined OI for 24-Nov-2025")
    # df1 = downloader.download_combined_oi("24-Nov-2025")
    
    
    # Example 2: Download Participant OI
    print("\nExample 2: Download Participant OI for 24-Nov-2025")
    df2 = downloader.download_fii_statistics("24-Nov-2025")
    print(df2)
    print("\nExample 3: Download date range")
    # start = datetime(2000, 1, 1)
    # end = datetime(2025, 12, 9)
    # # y,m,d
    # # print(end)
    # # print(current.strftime("%d-%b-%Y"))
    # downloader.download_date_range(start, end)
    
    # Example 3: List available reports
    print("\nExample 3: Available Reports")
    # downloader.list_available_reports()
    
    print("\n" + "="*70)
    print("Complete!")
    print("="*70)


if __name__ == "__main__":
    main()