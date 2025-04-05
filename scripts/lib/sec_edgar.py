# Standard library imports
import os
import time
from turtle import left
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from ipaddress import IPv4Address
# Third-party imports
import pendulum
import polars as pl
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from fuzzywuzzy import fuzz
import pandas as pd
import country_converter as coco

# Local imports
from lib.logging_config import get_logger
from lib.web_driver import WebDriver
from lib.ip_retriever import IPRetriever


logger = get_logger(__name__)

class SECEdgar(WebDriver):
    """A class for downloading and processing SEC Edgar log files.
    
    This class provides functionality to:
    - Download SEC Edgar log files for specific dates
    - Extract CSV files from downloaded zip archives
    - Convert CSV files to Parquet format
    - Clean and filter data based on RPV (Requests Per Volume) conditions
    - Enrich IP addresses with geolocation information
    
    The class inherits from WebDriver to handle web-based downloads and uses
    IPRetriever to get geolocation data for IP addresses.
    
    Attributes:
        date (Optional[str]): Specific date to download logs for (YYYY-MM-DD format)
        year (int): Year for which to download logs
        url (str): Base URL for SEC Edgar logs
        force (bool): Whether to force reprocessing of existing files
        base_dir (str): Base directory for all operations
        ip_location_path (str): Path to IP location database
        tmp_dir (str): Directory for temporary files
        download_dir (str): Directory for downloaded files
        extract_dir (str): Directory for extracted files
        convert_dir (str): Directory for converted files
        no_bots_dir (str): Directory for filtered files
        output_dir (str): Directory for final output
        ip_retriever (IPRetriever): Instance for handling IP geolocation
    """

    def __init__(self, **kwargs):
        """Initialize the SECEdgar processor.

        Args:
            **kwargs: Keyword arguments including:
                - date (str, optional): Specific date to download logs for (YYYY-MM-DD format).
                                      If not provided, downloads the most recent log.
                - download_folder (str, optional): Folder name for downloaded files. 
                                                 Defaults to "downloads".
                - base_dir (str, optional): Base directory for all operations. 
                                          Defaults to current working directory.
                - force (bool, optional): If True, forces reprocessing of existing files.
                                        Defaults to False.
                - url (str, optional): Direct URL to download the log file.
                                     If provided with date, skips obtaining all links.
        """
        super().__init__()
        

        # Set up directories
        self.base_dir = kwargs.get('base_dir', os.getcwd())
        self.ip_location_path = kwargs.get('ip_location_path', os.path.join(self.base_dir, "ip_locations.parquet"))
        self.__setup_directories(kwargs.get('download_folder', "downloads"))

        # Initialize IP retriever
        self.ip_retriever = IPRetriever()
        self.country_mapping_path = os.path.join(self.base_dir, "country_mapping.parquet")

    def __get_year(self) -> int:
        """Get the year for the Edgar logs.

        Returns:
            int: The year to download logs for.
        """
        if self.date is None:
            return pendulum.now().year
        return pendulum.parse(self.date, strict=False).year

    def __setup_directories(self, download_folder: str) -> None:
        """Set up all required directories.

        Args:
            download_folder (str): Name of the folder for downloaded files.
        """
        # Define directory paths
        self.tmp_dir = os.path.join(self.base_dir, "tmp")
        self.download_dir = os.path.join(self.tmp_dir, download_folder)
        self.extract_dir = os.path.join(self.tmp_dir, "extracted")
        self.convert_dir = os.path.join(self.tmp_dir, "converted")
        self.no_bots_dir = os.path.join(self.tmp_dir, "no_bots")
        self.no_bots_ip_enriched_dir = os.path.join(self.no_bots_dir, "ip_enriched")
        self.output_dir = os.path.join(self.base_dir, "output")
        
        
        # Create directories
        for directory in [self.download_dir, self.extract_dir, self.convert_dir, self.no_bots_dir, self.no_bots_ip_enriched_dir, self.output_dir]:
            os.makedirs(directory, exist_ok=True)
        
        # Configure downloads
        self.configure_downloads(self.download_dir)

    def __get_file_paths(self, file_name: str) -> Tuple[str, str, str, str, str]:
        """Get paths for download, CSV, and Parquet files.

        Args:
            file_name (str): Name of the downloaded file.

        Returns:
            Tuple[str, str, str, str, str]: Paths for zip, CSV, Parquet, no-bots Parquet, and cleaned Parquet files.
        """
        file_path = os.path.join(self.download_dir, file_name)
        csv_path = os.path.join(self.extract_dir, file_name.replace('.zip', '.csv'))
        parquet_path = os.path.join(self.convert_dir, file_name.replace('.zip', '.parquet'))
        no_bots_parquet_path = os.path.join(self.no_bots_dir, file_name.replace('.zip', '.parquet'))
        no_bots_ip_enriched_parquet_path = os.path.join(self.no_bots_ip_enriched_dir, file_name.replace('.zip', '.parquet'))
        cleaned_parquet_path = os.path.join(self.output_dir, file_name.replace('.zip', '.parquet'))
        return file_path, csv_path, parquet_path, no_bots_parquet_path, no_bots_ip_enriched_parquet_path, cleaned_parquet_path

    def __extract_date_from_link(self, link: str) -> pendulum.DateTime:
        """Extract the date from a log file link.

        Args:
            link (str): The URL of the log file.

        Returns:
            pendulum.DateTime: The date extracted from the filename.

        Example:
            >>> link = "https://www.sec.gov/files/log20240320.zip"
            >>> date = __extract_date_from_link(link)
            >>> print(date)  # 2024-03-20 00:00:00+00:00
        """
        filename = link.split("/")[-1]
        date_str = filename.replace("log", "").split(".")[0]
        return pendulum.parse(date_str, strict=False)

    def obtain_edgar_log_links(self, url) -> Dict[str, str]:
        """Get all available Edgar log file links.

        Returns:
            List[str]: List of URLs for zip files containing Edgar logs.

        Note:
            Waits for links to be present on the page before extracting them.
        """
        self.driver.get(url)
        
        # Wait for links to be present
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "a"))
        )
        
        # Get all links and filter for zip files
        all_links = [a.get_attribute('href') for a in self.driver.find_elements(by=By.TAG_NAME, value="a")]
        zip_links = [link for link in all_links if link and link.endswith('.zip')]
        edgar_log_links_dict = {
            self.__extract_date_from_link(link).to_date_string(): link 
            for link in zip_links
        }
        return edgar_log_links_dict

    def __extract_csv_from_zip(self, zip_path: str) -> str:
        """Extract CSV file from zip archive.

        Args:
            zip_path (str): Path to the zip file.

        Returns:
            str: Path to the extracted CSV file.

        Raises:
            ValueError: If no CSV file is found in the zip archive.
        """
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError(f"No CSV file found in {zip_path}")
            
            csv_name = csv_files[0]
            zip_ref.extract(csv_name, self.extract_dir)
            return os.path.join(self.extract_dir, csv_name)

    def __convert_to_parquet(self, csv_path: str, parquet_path: str) -> None:
        """Convert CSV file to Parquet format.

        Args:
            csv_path (str): Path to the CSV file.
            parquet_path (str): Path where the Parquet file will be saved.
        """
        logger.info(f"Converting {csv_path} to Parquet")
        df = pl.read_csv(csv_path, low_memory=True, infer_schema_length=10000)
        df.write_parquet(parquet_path, compression="gzip")
        logger.info(f"Successfully converted to {parquet_path}")

    def cleaning_data(self, read_path: str, write_path: str) -> None:
        """Clean the data in the Parquet file by applying RPV conditions.

        This method:
        1. Filters out non-200 responses and non-zero indices
        2. Applies three RPV conditions to identify bot activity:
           - More than 25 requests per minute from same IP
           - Access to more than 3 unique CIKs per minute from same IP
           - More than 500 total requests from same IP
        3. Cleans IP addresses to standard format
        4. Saves the cleaned data to a new Parquet file

        Args:
            read_path (str): Path to the input Parquet file.
            write_path (str): Path to save the cleaned Parquet file.
        """
        # Load and filter data
        df = pl.scan_parquet(read_path)\
            .filter(
                (pl.col('code') == 200) & (pl.col('idx') == 0) & (pl.col('crawler') == 0)
            )\
            .with_columns(
                pl.concat_str([pl.col('date'), pl.col('time')], separator=' ')
                .str.to_datetime("%Y-%m-%d %H:%M:%S").alias('datetime')
            )

        # Get base columns needed for analysis
        rpv_base = df\
            .select(
                'datetime',
                'ip',
                'cik',
                'date',
                'accession'
            )

        # Apply RPV conditions
        rpv_cond1_ips = rpv_base\
            .group_by(
                pl.col('datetime').dt.truncate('1m'),
                'ip'
            )\
            .agg(
                pl.count().alias('no_of_requests_per_minute')
            )\
            .filter(pl.col('no_of_requests_per_minute') > 25)\
            .select('ip')\
            .unique()

        rpv_cond2_ips = rpv_base\
            .group_by(
                pl.col('datetime').dt.truncate('1m'),
                'ip'
            )\
            .agg(
                pl.col('cik').n_unique().alias('no_of_unique_ciks')
            )\
            .filter(pl.col('no_of_unique_ciks') > 3)\
            .select('ip')\
            .unique()

        rpv_cond3_ips = rpv_base\
            .group_by('ip')\
            .agg(
                pl.count().alias('no_of_requests_per_day')
            )\
            .filter(pl.col('no_of_requests_per_day') > 500)\
            .select('ip')\
            .unique()

        # Combine all bot IPs
        bot_ips = pl.concat([
            rpv_cond1_ips,
            rpv_cond2_ips, 
            rpv_cond3_ips
        ]).unique()

        # Get filtered IPs (non-bot IPs)
        filtered_df = df.join(bot_ips, on='ip', how='anti')
        
        # Clean IP addresses
        ipv4_pattern = r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.[^.\s]+$"
        cleaned_df = filtered_df\
            .with_columns(
                pl.when(pl.col("ip").str.extract_groups(ipv4_pattern).is_not_null())
                .then(
                    pl.concat_str(
                        [pl.col("ip").str.extract(ipv4_pattern, i) for i in range(1, 4)], separator="."
                    ) + ".0"
                )
                .otherwise(None)
                .alias("cleaned_ip")
            )\
            .with_columns(
                pl.col("cleaned_ip")\
                .map_elements(lambda x: int(IPv4Address(x)) if x else None, return_dtype=pl.Int64)
                .alias('cleaned_ip_int')
            )

        # Save the cleaned data
        cleaned_df.collect().write_parquet(write_path, compression="gzip")
        logger.info(f"Cleaned data saved to {write_path}")

    def __extract_ip(self, read_path: str, write_path: str) -> None:
        """Extract and enrich IP addresses with geolocation information.

        This method:
        1. Extracts unique IP addresses from the cleaned data
        2. Retrieves geolocation information for each IP
        3. Joins the geolocation data back with the original data
        4. Saves the enriched data to a new Parquet file

        Args:
            read_path (str): Path to the cleaned Parquet file.
            write_path (str): Path to save the enriched Parquet file.
        """
        # Extract unique IPs
        tmp_raw_ips_path = os.path.join(self.tmp_dir, "raw_ips.parquet")
        df = pl.scan_parquet(read_path)

        df\
            .select(
                pl.col('cleaned_ip'),
                pl.col('cleaned_ip_int')
            )\
            .unique()\
            .collect()\
            .write_parquet(tmp_raw_ips_path, compression="gzip")
        
        # Get IP geolocation data
        ip_with_country_code_df = self.ip_retriever.get_ip_from_ip2location(ip_df=pl.scan_parquet(tmp_raw_ips_path))

        # Join with original data and save
        df\
           .join(
                ip_with_country_code_df,
                left_on="cleaned_ip_int", 
                right_on=["ip_from"],  
                how="left"
            )\
            .collect()\
            .write_parquet(write_path, compression="gzip")
        
        logger.info(f"Extracted IP with country code to {write_path}")

    def preprocess(self, date:str, url=None, force=None) -> None:
        # Set up date and URL
        self.date = date
        self.year = self.__get_year()
        self.base_url = f'https://www.sec.gov/files/edgar{self.year}.html'
        self.url = url
        self.force = force
        
        """Download and process the specified Edgar log file.

        This method orchestrates the entire process:
        1. Gets all available log file links
        2. Filters for the requested date (or most recent if no date specified)
        3. Downloads the selected file (if not exists or force=True)
        4. Extracts the CSV from the zip (if not exists or force=True)
        5. Converts the CSV to Parquet format (if not exists or force=True)
        6. Cleans the data by applying RPV conditions
        7. Enriches IP addresses with geolocation information

        Raises:
            ValueError: If the specified date is not found in available logs.
        """
        # Construct file name based on date
        if self.date:
            file_name = f"log{self.date.replace('-', '')}.zip"
        else:
            # Use current date if no date specified
            from datetime import datetime
            current_date = datetime.now().strftime("%Y%m%d")
            file_name = f"log{current_date}.zip"

        # Get file paths
        zip_path, csv_path, parquet_path, no_bots_parquet_path, no_bots_ip_enriched_parquet_path, cleaned_parquet_path = self.__get_file_paths(file_name)

        # Only start driver and download if file doesn't exist
        if not os.path.exists(zip_path) or self.force:
            self.start_driver()
            try:
                if self.url is None:
                    # Get all available log links
                    edgar_log_links_dict = self.obtain_edgar_log_links(url=self.base_url)
                    logger.info(f"Found {len(edgar_log_links_dict)} zip files to download")
                    # Get target link based on date
                    if self.date:
                        if self.date not in edgar_log_links_dict:
                            raise ValueError(f"No log file found for date {self.date}")
                        target_link = edgar_log_links_dict[self.date]
                    else:
                        latest_date = max(edgar_log_links_dict.keys())
                        target_link = edgar_log_links_dict[latest_date]
                else:
                    target_link = self.url
                


                # Download the file
                logger.info(f"Starting download of {target_link}")
                self.download_file(target_link)
                logger.info(f"Download completed for {target_link}")
            finally:
                self.driver.quit()
        else:
            logger.info(f"Zip file already exists at {zip_path}, skipping download")
        
        # Extract CSV if it doesn't exist or force=True
        if not os.path.exists(csv_path) or self.force:
            logger.info(f"Extracting CSV from {zip_path}")
            extracted_csv = self.__extract_csv_from_zip(zip_path)
            logger.info(f"CSV extracted to {extracted_csv}")
        else:
            logger.info(f"CSV file already exists at {csv_path}, skipping extraction")
            extracted_csv = csv_path

        # Convert to Parquet if it doesn't exist or force=True
        if not os.path.exists(parquet_path) or self.force:
            logger.info(f"Converting CSV to Parquet")
            self.__convert_to_parquet(extracted_csv, parquet_path)
        else:
            logger.info(f"Parquet file already exists at {parquet_path}, skipping conversion")

        # Clean data and enrich IPs
        if not os.path.exists(no_bots_parquet_path) or self.force:
            logger.info(f"Cleaning data")
            self.cleaning_data(read_path=parquet_path, write_path=no_bots_parquet_path)
        else:
            logger.info(f"No bots Parquet file already exists at {no_bots_parquet_path}, skipping cleaning")

        if not os.path.exists(no_bots_ip_enriched_parquet_path) or self.force:
            logger.info(f"Extracting IP with country code")
            self.__extract_ip(read_path=no_bots_parquet_path, write_path=no_bots_ip_enriched_parquet_path)
        else:
            logger.info(f"No bots IP enriched Parquet file already exists at {no_bots_ip_enriched_parquet_path}, skipping extraction")

        logger.info(msg=f"Cleaning country names")
        self.__clean_country_names(read_path=no_bots_ip_enriched_parquet_path, write_path=cleaned_parquet_path)



    def __clean_country_names(self, read_path: str, write_path: str, country_col: str = 'country_name') -> None:
        """Clean and standardize country names in the dataset.

        This method standardizes country names by:
        1. Loading existing country name mappings or creating a new mapping
        2. Finding unmapped country names and converting them using country_converter
        3. Updating the master country mapping file
        4. Applying standardized names to the full dataset

        Args:
            read_path (str): Path to input Parquet file
            write_path (str): Path to output Parquet file
            country_col (str, optional): Name of column containing country names. Defaults to 'country_name'.
        """
        # Load input data
        df = pl.scan_parquet(read_path)

        # Load or create country mapping
        if os.path.exists(self.country_mapping_path):
            country_mapping_df = pl.scan_parquet(self.country_mapping_path)
        else:
            country_mapping_df = pl.LazyFrame(schema={
                "raw_country_name": pl.Utf8,
                "cleaned_country_name": pl.Utf8
            })

        # Find unmapped countries
        unmapped_countries = df\
            .select(pl.col(country_col))\
            .join(
                country_mapping_df,
                left_on=country_col,
                right_on="raw_country_name",
                how="anti"
            )\
            .unique()

        # Convert unmapped countries
        cc = coco.CountryConverter()
        def convert_country(name: str) -> str:
            try:
                return cc.convert(name, to='name')
            except:
                logger.warning(f"Could not convert country name: {name}")
                return name

        new_mappings = unmapped_countries.select(
            pl.col(country_col).alias("raw_country_name"),
            pl.col(country_col).map_elements(convert_country, return_dtype=pl.Utf8).alias('cleaned_country_name')
        )

        # Update master mapping file
        tmp_country_mapping_path = os.path.join(self.tmp_dir, "country_mapping.parquet")
        pl.concat([new_mappings, country_mapping_df])\
            .collect()\
            .write_parquet(tmp_country_mapping_path, compression="gzip")
        pl.scan_parquet(tmp_country_mapping_path)\
            .collect()\
            .write_parquet(self.country_mapping_path, compression="gzip")
        os.remove(tmp_country_mapping_path)

        # Verify mapping file
        country_mapping = pl.scan_parquet(self.country_mapping_path)
        if "raw_country_name" not in country_mapping.collect_schema().names() or "cleaned_country_name" not in country_mapping.collect_schema().names():
            logger.error("Country mapping file missing required columns")
            raise ValueError("Country mapping file missing required columns")

        # Apply mappings to full dataset
        df\
            .join(
                country_mapping,
                left_on=country_col,
                right_on="raw_country_name",
                how="left"
            )\
            .with_columns(
                pl.when(pl.col("cleaned_country_name").is_not_null())\
                .then(pl.col("cleaned_country_name"))\
                .otherwise(pl.col(country_col))\
                .alias(country_col)
            )\
            .drop(["raw_country_name", "cleaned_country_name"], strict=False)\
            .collect()\
            .write_parquet(write_path, compression="gzip")
            
