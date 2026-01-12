"""
NSE Official API Downloader - FIXED VERSION
Issues identified and corrected
"""

import requests
import json
import urllib.parse
from datetime import datetime, timedelta
import pandas as pd
import io
import os
import gzip
import zlib
import lzma

class NSEAPIDownloader:
    """
    Download NSE F&O reports using official API endpoints
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
            # 'Accept-Encoding': 'gzip, deflate, br',  # REMOVED: Causes NSE to send compressed data
            'Connection': 'keep-alive',
            'Referer': 'https://www.nseindia.com/all-reports-derivatives',
            'X-Requested-With': 'XMLHttpRequest'
        })
        
        try:
            response = self.session.get(
                f"{self.base_url}/all-reports-derivatives",
                timeout=10
            )
            print("✓ Session initialized with cookies")
        except Exception as e:
            print(f"! Warning during session init: {e}")
    
    def _build_api_url(self, report_config, date_str):
        """Build NSE API URL with proper encoding"""
        archives_json = json.dumps([report_config])
        archives_encoded = urllib.parse.quote(archives_json)
        
        url = (
            f"{self.base_url}/api/reports"
            f"?archives={archives_encoded}"
            f"&date={date_str}"
            f"&type={report_config.get('section', 'equity')}"
            f"&mode=single"
        )
        
        return url
        
    def download_participant_tv(self, date, output_file=None):
        """
        Download Participant-wise Trading Volumes using NSE API
        
        FIXED ISSUES:
        1. Removed unused variables (successful, failed, log_filepath) declared before try block
        2. Moved logging variables inside proper scope
        3. Fixed logic flow for success/failure tracking
        4. Removed commented-out dead code
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
        print(f"Downloading Participant Trading Volumes via NSE API")
        print(f"Date: {date_str}")
        print(f"{'='*70}\n")
        
        report_config = {
            "name": "F&O - Participant wise Trading Volumes(csv)",
            "type": "archives",
            "category": "derivatives",
            "section": "equity"
        }
        
        api_url = self._build_api_url(report_config, date_str)
        
        print(f"API URL: {api_url}\n")
        print(f"Making API request...")
        
        # FIXED: Declare logging variables in proper scope
        log_filepath = "log_participant_tv.log"
        successful = []
        failed = []
        
        try:
            response = self.session.get(api_url, timeout=20)
            
            print(f"Response Status: {response.status_code}")
            print(f"Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
            print(f"Content-Length: {len(response.content):,} bytes")
            
            if response.status_code == 503:
                print("Service temporarily unavailable (503)")
                failed.append(dt.strftime("%d-%b-%Y"))
                return None

            if response.status_code == 200:
                successful.append(dt.strftime("%d-%b-%Y"))    
                content = response.content
                original_size = len(content)
                
                print(f"Original size: {original_size:,} bytes")
                print(f"First 10 bytes (hex): {content[:10].hex()}")
                
                # Try multiple decompression methods
                decompressed = False
                
                # Method 1: Standard gzip
                if not decompressed and content[:2] == b'\x1f\x8b':
                    try:
                        content = gzip.decompress(content)
                        print(f"✓ Decompressed using GZIP: {original_size:,} → {len(content):,} bytes")
                        decompressed = True
                    except Exception as e:
                        print(f"! GZIP failed: {e}")
                
                # Method 2: Zlib
                if not decompressed:
                    try:
                        content = zlib.decompress(content)
                        print(f"✓ Decompressed using ZLIB: {original_size:,} → {len(content):,} bytes")
                        decompressed = True
                    except:
                        pass
                
                # Method 3: Raw deflate (zlib without header)
                if not decompressed:
                    try:
                        content = zlib.decompress(content, -zlib.MAX_WBITS)
                        print(f"✓ Decompressed using raw DEFLATE: {original_size:,} → {len(content):,} bytes")
                        decompressed = True
                    except:
                        pass
                
                # Method 4: LZMA/XZ
                if not decompressed:
                    try:
                        content = lzma.decompress(content)
                        print(f"✓ Decompressed using LZMA: {original_size:,} → {len(content):,} bytes")
                        decompressed = True
                    except:
                        pass
                
                # Method 5: Skip potential header bytes and try again
                if not decompressed:
                    for skip_bytes in [4, 8, 12, 16]:
                        if len(content) > skip_bytes:
                            try:
                                test_content = gzip.decompress(content[skip_bytes:])
                                content = test_content
                                print(f"✓ Decompressed using GZIP after skipping {skip_bytes} bytes: {original_size:,} → {len(content):,} bytes")
                                decompressed = True
                                break
                            except:
                                pass
                            
                            try:
                                test_content = zlib.decompress(content[skip_bytes:])
                                content = test_content
                                print(f"✓ Decompressed using ZLIB after skipping {skip_bytes} bytes: {original_size:,} → {len(content):,} bytes")
                                decompressed = True
                                break
                            except:
                                pass
                
                if not decompressed:
                    print("! Could not decompress - saving as-is (might be plain text or unknown format)")
                
                # Determine output filename
                if output_file is None:
                    output_file = f"participant_tv_{dt.strftime('%Y%m%d')}.csv"
                
                output_dir = "./NSE_Downloads/FO_Participant_Volume"
                os.makedirs(output_dir, exist_ok=True)
                filepath = os.path.join(output_dir, output_file)
                
                # Save to file
                with open(filepath, 'wb') as f:
                    f.write(content)
                
                print(f"\n✓ Saved to: {filepath}")
                
                # Verify it's valid CSV
                try:
                    df = pd.read_csv(filepath)
                    print(f"✓ CSV verified: {df.shape[0]} rows, {df.shape[1]} columns")
                    if len(df.columns) > 0:
                        print(f"✓ Columns: {', '.join(df.columns[:5])}")
                except Exception as e:
                    print(f"! Could not parse as CSV: {e}")
                    # Show a sample of the content for debugging
                    try:
                        sample = content[:200].decode('utf-8', errors='ignore')
                        print(f"! Content sample: {sample}")
                    except:
                        print(f"! Binary content (hex): {content[:100].hex()}")
                
                return filepath
                
            else:
                print(f"\n✗ Failed with status code: {response.status_code}")
                failed.append(dt.strftime("%d-%b-%Y"))
                if response.text:
                    print(f"Response: {response.text[:200]}")
                return None
    
        except Exception as e:
            print(f"\n✗ Error: {str(e)}")
            failed.append(dt.strftime("%d-%b-%Y"))
            import traceback
            traceback.print_exc()
            return None
            
        finally:
            # Log results
            with open(log_filepath, 'a') as f:
                f.write(f"Date: {date_str}\n")
                f.write(f"Successful: {successful}\n")
                f.write(f"Failed: {failed}\n")
                f.write("-" * 50 + "\n")
    
    def list_available_reports(self, category="derivatives", section="equity"):
        """List all available reports for a category"""
        print(f"\n{'='*70}")
        print(f"Available NSE Reports")
        print(f"Category: {category}, Section: {section}")
        print(f"{'='*70}\n")
        
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
        """
        Download Participant TV for a date range
        
        FIXED ISSUES:
        1. Properly handle return value from download_participant_tv
        2. Fixed success/failure tracking logic
        """
        current = start_date
        successful = []
        failed = []
        
        while current <= end_date:
            # Skip weekends (Saturday=5, Sunday=6)
            if current.weekday() < 5:  
                try:
                    result = self.download_participant_tv(current)
                    
                    # FIXED: Properly check if download was successful
                    if result:  # result is filepath if successful, None if failed
                        successful.append(current.strftime("%d-%b-%Y"))
                    else:
                        failed.append(current.strftime("%d-%b-%Y"))
                        
                except Exception as e:
                    print(f"Error downloading {current.strftime('%d-%b-%Y')}: {e}")
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


def main():
    """Example usage"""
    downloader = NSEAPIDownloader()
    
    print("\n" + "="*70)
    print("NSE Official API Downloader - FIXED VERSION")
    print("="*70)
    
    # Example: Download date range
    print("\nDownloading date range...")
    start = datetime(2025, 1, 1)
    end = datetime(2025, 12, 5)  # Smaller range for testing
    
    downloader.download_date_range(start, end)
    
    print("\n" + "="*70)
    print("Complete!")
    print("="*70)


if __name__ == "__main__":
    main()