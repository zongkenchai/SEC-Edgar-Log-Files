# Selenium and Undetected Chromedriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import WebDriverException
from selenium import webdriver
import undetected_chromedriver as uc
from urllib.parse import urlparse, urlunparse
import os
import time
import json
import subprocess
from typing import List, Dict, Optional, Union
from pathlib import Path

# Custom modules
from lib.logging_config import get_logger

logger = get_logger(__name__)

class WebDriver:
    """A base class for handling web browser automation using Selenium and undetected-chromedriver.
    
    This class provides core functionality for browser automation including:
    - Browser initialization and configuration
    - Download handling
    - Network request management
    - Browser window management
    - Performance monitoring
    """

    # Default Chrome options for browser initialization
    default_option_list = [
        '--window-size=1920,1080',
        '--no-sandbox',
        '--enable-extensions',
        '--allow-extensions-in-incognito',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--disable-notifications',
        '--disable-popup-blocking',
        '--disable-blink-features=AutomationControlled'
    ]
    
    # Default blocked URLs for performance optimization
    default_blocked_urls = [
        # Image formats
        "*.png", "*.png?*", "*.jpg", "*.jpg?*", "*.jpeg", "*.jpeg?*",
        "*.gif", "*.gif?*", "*.webp", "*.webp?*",
        
        # Video formats
        "*.mp4", "*.mp4?*", "*.webm", "*.webm?*",
        
        # Web resources
        "*.css", "*.css?*", "*.svg", "*.svg?*", "*.ico", "*.ico?*",
        "*.woff", "*.woff?*", "*.woff2", "*.woff2?*",
        
        # Analytics and tracking
        "https://www.google.com/xjs/_/js/*",
        "https://www.gstatic.com/og/_/js/*",
        "https://www.google.com/gen_204*",
        "https://www.googleadservices.com/",
        "https://www.googletagmanager.com",
        "https://www.google-analytics.com",
        "*/gtm.js?*",
        
        # Social media and third-party services
        "https://www.youtube.com",
        "https://maps.googleapis.com",
        "https://cdn.shopify.com",
        "https://*.cloudfront.net/*.js",
        "https://static.cloudflareinsights.com",
        "https://connect.facebook.net/*",
        "https://www.facebook.com",
        "https://static.zdassets.com",
        "https://d.oracleinfinity.io",
        "https://bat.bing.com/",
        "https://i8.amplience.net/i/jpl/*",
        "https://*.monetate.net",
        "https://*.criteo.com/*",
        "https://analytics.tiktok.com",
        "https://assets.gorgias.chat",
        "https://sdk.postscript.io",
        "https://script.hotjar.com",
        "https://cdn.judge.me",
        "https://cdn.jsdelivr.net",
        "https://consent.cookiebot.co",
        "https://static.klaviyo.com",
        "https://www.clarity.ms",
        
        # General static resources
        "*/static/*.js"
    ]

    def __init__(self):
        """Initialize the WebDriver class."""
        self.driver = None
        self.download_dir = None
        self._download_prefs = {}
        self._download_options = []

    def configure_downloads(self, download_dir: Union[str, Path]) -> None:
        """Configure download settings for the browser.

        Args:
            download_dir (Union[str, Path]): Directory where files should be downloaded.
        """
        # Convert to Path and ensure absolute path
        self.download_dir = Path(download_dir).absolute()
        self.download_dir.mkdir(exist_ok=True)
        
        # Set up Chrome preferences for downloads
        self._download_prefs = {
            "download.default_directory": str(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_settings.popups": 0,
            "download.open_pdf_in_system_reader": False
        }
        
        # Set up Chrome options for downloads
        self._download_options = [
            f"--download.default_directory={str(self.download_dir)}",
            "--no-sandbox",
            "--disable-dev-shm-usage"
        ]

    def start_driver(self, 
                    implicitly_wait_time: int = 2, 
                    headless: bool = False, 
                    additional_prefs: Optional[Dict] = None, 
                    additional_options: Optional[List[str]] = None, 
                    **kwargs) -> None:
        """Initialize and start the Chrome WebDriver with specified options.

        Args:
            implicitly_wait_time (int, optional): Time to wait for elements to be present. Defaults to 2.
            headless (bool, optional): Whether to run browser in headless mode. Defaults to False.
            additional_prefs (Dict, optional): Additional Chrome preferences. Defaults to None.
            additional_options (List[str], optional): Additional Chrome options. Defaults to None.
            **kwargs: Additional arguments to pass to ChromeOptions.

        Note:
            This method sets up Chrome with various security and performance optimizations.
            If download settings were configured, they will be included in the options.
        """
        logger.info('Starting Web Driver')

        # Set up Chrome options
        option_list = kwargs.get('option_list', []) + self.default_option_list
        if headless:
            option_list.append('--headless=new')
        
        options = uc.ChromeOptions()
        for option in option_list:
            options.add_argument(option)
            
        # Add download options if configured
        if self._download_options:
            for option in self._download_options:
                options.add_argument(option)
            
        if additional_options:
            for option in additional_options:
                options.add_argument(option)
        
        # Set up network logging
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        
        # Set up default preferences
        prefs = {
            'profile.default_content_setting_values': {
                'cookies': 2,  # 2 = block cookies
                'plugins': 2,  # 2 = block plugins
                'popups': 2,  # 2 = block popups
                'geolocation': 2,  # 2 = block geolocation
                'notifications': 2,  # 2 = block notifications
            },
            'profile.block_third_party_cookies': True,
            'profile.default_content_settings.popups': 0,
            'disk-cache-size': 4096,
            'history.clear_on_exit': True,
            'browser.cache.disk.enable': False,
            'browser.cache.memory.enable': False,
            'browser.cache.offline.enable': False,
            'network.cookie.lifetimePolicy': 2
        }

        # Merge download preferences if configured
        if self._download_prefs:
            prefs.update(self._download_prefs)

        # Merge additional preferences
        if additional_prefs:
            prefs.update(additional_prefs)
        
        options.add_experimental_option("prefs", prefs)
        
        # Set up capabilities
        capabilities = DesiredCapabilities.CHROME.copy()
        capabilities["timeouts"] = {"pageLoad": 10000, "script": 10000} 
        
        # Initialize the driver
        self.driver = uc.Chrome(options=options)
        self.driver.implicitly_wait(implicitly_wait_time)
        logger.debug('Started Web Driver')

    def enable_developer_tools(self) -> None:
        """Enable Chrome DevTools Protocol for network monitoring."""
        self.driver.execute_cdp_cmd('Network.enable', {})

    def prevent_file_extension_fetch(self, urls: Optional[List[str]] = None) -> None:
        """Block specific URLs and file types from loading.

        Args:
            urls (List[str], optional): List of URLs and file patterns to block. 
                                      If None, uses default_blocked_urls.
        """
        urls_to_block = urls if urls is not None else self.default_blocked_urls
        self.driver.execute_cdp_cmd('Network.setBlockedURLs', {"urls": urls_to_block})
        
    def allow_file_extension_fetch(self) -> None:
        """Remove all URL blocking rules."""
        self.driver.execute_cdp_cmd('Network.setBlockedURLs', {"urls": []})

    def set_headers(self, headers: Dict[str, str]) -> None:
        """Set custom HTTP headers for all requests.

        Args:
            headers (Dict[str, str]): Dictionary of header names and values.
        """
        logger.debug(f'Headers : {headers}')
        self.driver.execute_cdp_cmd('Network.setExtraHTTPHeaders', {'headers': headers})

    def quit_driver(self) -> None:
        """Safely close the browser and clean up resources."""
        logger.debug('Closing Web Driver')
        try:
            self.driver.quit()
        except:
            subprocess.run('killall chrome', shell=True)
            subprocess.run('killall chromedriver', shell=True)
        
    def close_tab(self) -> None:
        """Close the current tab and switch to the last remaining tab."""
        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[-1])
        
    def open_and_switch_tab(self) -> None:
        """Open a new tab and switch to it."""
        self.driver.execute_script(f"window.open('', '_blank');")
        self.driver.switch_to.window(self.driver.window_handles[-1])
        
    def get_network_log(self) -> List[Dict]:
        """Get network performance logs from the browser.

        Returns:
            List[Dict]: List of network log entries.

        Note:
            Waits for page to be fully loaded before collecting logs.
        """
        fully_rendered = False
        tries = 0
        while not fully_rendered and tries <= 20:
            if self.driver.execute_script("return document.readyState") == "complete":
                fully_rendered = True
            time.sleep(0.5)
            tries += 1
        return self.driver.get_log("performance")
    
    @staticmethod
    def get_data_usage(logs: List[Dict]) -> float:
        """Calculate total data usage from network logs.

        Args:
            logs (List[Dict]): List of network log entries.

        Returns:
            float: Total data usage in megabytes.
        """
        total_mb = 0
        for network_log in logs:
            network_message = json.loads(network_log['message'])['message']
            if "Network.loadingFinished" == network_message['method']:
                total_mb += network_message["params"]["encodedDataLength"]/1024/1024
        return total_mb

    @staticmethod
    def check_driver_exists(driver) -> bool:
        """Check if a WebDriver instance is still active.

        Args:
            driver: WebDriver instance to check.

        Returns:
            bool: True if driver exists and is responsive, False otherwise.
        """
        try:
            if driver:
                driver.window_handles
                return True
            return False
        except Exception as e:
            logger.debug(f'Driver not exists : {e}')
            return False
    
    @staticmethod
    def normalize_url(url: Optional[str]) -> Optional[str]:
        """Normalize a URL by ensuring proper formatting.

        Args:
            url (Optional[str]): URL to normalize.

        Returns:
            Optional[str]: Normalized URL or None if input is None.
        """
        if not url:
            return None
            
        parsed_url = urlparse(url)
        normalized_path = parsed_url.path if parsed_url.path else '/'
        
        return urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            normalized_path,
            parsed_url.params,
            parsed_url.query,
            parsed_url.fragment
        ))
        
    def set_window_to_desired(self, resolution: str) -> None:
        """Set browser window size to specified resolution.

        Args:
            resolution (str): Resolution in format "width,height".
        """
        width, height = resolution.split(',')
        self.driver.set_window_size(width=width, height=height)

    def wait_for_download_complete(self, timeout: int = 300) -> bool:
        """Wait for a download to complete.

        Args:
            timeout (int, optional): Maximum time to wait in seconds. Defaults to 300.

        Returns:
            bool: True if download completed, False if timed out.

        Note:
            Monitors the download directory for .crdownload files.
        """
        if not self.download_dir:
            raise ValueError("Download directory not set")

        seconds = 0
        dl_wait = True
        while dl_wait and seconds < timeout:
            time.sleep(1)
            dl_wait = False
            files = os.listdir(self.download_dir)
            for fname in files:
                if fname.endswith('.crdownload'):
                    dl_wait = True
            seconds += 1
        return seconds < timeout
    

    def download_file(self, url: str) -> bool:
        """Download a file using Selenium.

        Args:
            url (str): URL of the file to download.

        Returns:
            bool: True if download was successful, False otherwise.

        Note:
            Waits for the download to complete before returning.
        """
        try:
            # Navigate to the download URL
            self.driver.get(url)
            time.sleep(10)  # Wait for download to start
            
            # Wait for download to complete
            if not self.wait_for_download_complete():
                logger.error(f"Download timed out for {url}")
                return False
                
            logger.info(f"Download completed for {url}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading {url}: {str(e)}")
            return False