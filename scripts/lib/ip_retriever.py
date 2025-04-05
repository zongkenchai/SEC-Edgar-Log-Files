import ipaddress
import requests
import pendulum
import polars as pl
from tqdm import tqdm
import time
import os
from timezonefinder import TimezoneFinder
from concurrent.futures import ProcessPoolExecutor, as_completed
from lib.config import Config
from lib.logging_config import get_logger
logger = get_logger(__name__)


class IPRetriever:
    """A class to retrieve and manage IP geolocation information.
    
    This class handles the retrieval of IP geolocation data from multiple sources:
    1. IP2Location database (local parquet file)
    2. Geolocation DB API (external service)
    
    It maintains a combined database of IP locations and tracks which IPs have been
    attempted to be retrieved from the external API.
    
    Attributes:
        ip2_location_path (str): Path to the IP2Location database parquet file
        geolocation_db_api_key (str): API key for the geolocation service
        geolocation_db_url (str): URL for the geolocation API
        MAX_WORKERS (int): Maximum number of parallel workers for API calls
        combined_ip_location_path (str): Path to the combined IP location database
    """
    
    ip2_location_path = 'ip2_location.parquet'
    geolocation_db_api_key = Config.GEOLOCATION_DB_API_KEY
    geolocation_db_url = f"https://geolocation-db.com/json/{geolocation_db_api_key}"
    MAX_WORKERS = 16
    combined_ip_location_path = 'combined_ip_locations.parquet'
    
    def __init__(self):
        """Initialize the IPRetriever and load the combined IP location database.
        
        If the combined database doesn't exist, it creates it from the IP2Location database.
        Adds a 'geolocation_db_attempted' column if it doesn't exist.
        """
        if os.path.exists(self.combined_ip_location_path):
            self.combined_ip_location_df = pl.scan_parquet(self.combined_ip_location_path)
            if 'geolocation_db_attempted' not in self.combined_ip_location_df.collect_schema().names():
                self.combined_ip_location_df = self.combined_ip_location_df.with_columns(
                    pl.lit(False).alias('geolocation_db_attempted')
                )
        else:
            self.combined_ip_location_df = pl.scan_parquet(self.ip2_location_path).with_columns(
                pl.lit(False).alias('geolocation_db_attempted')
            )

    def get_ip_location_from_geolocation_db(self, ip: str) -> dict:
        """Fetch IP geolocation details from the external API.
        
        Args:
            ip (str): IP address to look up
            
        Returns:
            dict: Dictionary containing geolocation information for the IP, or None if lookup fails.
                 Keys include: ip_from, ip_to, country_code, country_name, region_name,
                 city_name, latitude, longitude, zip_code, timezone.
        """
        for attempt in range(5):
            try:
                response = requests.get(f'{self.geolocation_db_url}/{ip}')
                if response.status_code == 200:
                    break
            except Exception as e:
                logger.error(f"get_ip_from_ip2location - Error processing IP {ip}: {str(e)}")
                if attempt < 4:
                    time.sleep(1)
        else:
            return None

        data = response.json()
        lat = data.get('latitude') if data.get('latitude') != 'Not found' else None
        lng = data.get('longitude') if data.get('longitude') != 'Not found' else None
        
        timezone_str = None
        if lat is not None and lng is not None:
            tf = TimezoneFinder()
            timezone_str = tf.timezone_at(lat=lat, lng=lng)
        timezone = pendulum.timezone(timezone_str) if timezone_str else None
        
        return {
            'ip_from': int(ipaddress.IPv4Address(ip)),  
            'ip_to': int(ipaddress.IPv4Address(ip)),    
            'country_code': data.get('country_code') if data.get('country_code') != 'Not found' else None,
            'country_name': data.get('country_name') if data.get('country_name') != 'Not found' else None, 
            'region_name': data.get('state') if data.get('state') != 'Not found' else None,
            'city_name': data.get('city') if data.get('city') != 'Not found' else None,
            'latitude': lat,
            'longitude': lng,
            'zip_code': data.get('postal') if data.get('postal') != 'Not found' else None,
            'timezone': pendulum.now(timezone).format('Z') if timezone else None,
        }

    def process_single_ip(self, ip: str) -> dict:
        """Process a single IP address and return its geolocation data.
        
        Args:
            ip (str): IP address to process
            
        Returns:
            dict: Dictionary containing geolocation information for the IP
        """
        if ip is None:
            return {}
        return self.get_ip_location_from_geolocation_db(ip) or {}

    def get_ip_from_ip2location(self, ip_df: pl.DataFrame) -> pl.DataFrame:
        """Enrich IP data with location information from multiple sources.
        
        This method:
        1. First tries to get locations from the combined database
        2. Identifies IPs that need to be looked up from the external API
        3. Processes missing IPs in parallel
        4. Updates the combined database with new results
        5. Returns the enriched DataFrame
        
        Args:
            ip_df (pl.DataFrame): DataFrame containing IP addresses to enrich
            
        Returns:
            pl.DataFrame: DataFrame enriched with location information
        """
        # Join with combined database
        ip_df = ip_df.join(
            self.combined_ip_location_df, 
            left_on="cleaned_ip_int", 
            right_on=["ip_from"],  
            how="left"
        )
        
        # Get IPs that need processing
        ip_with_missing_country_code_list = ip_df\
            .filter(
                (
                    (pl.col("geolocation_db_attempted") == False) |
                    (pl.col("geolocation_db_attempted").is_null())
                ) &
                (pl.col("country_code").is_null())
            )\
            .filter(pl.col("cleaned_ip").is_not_null())\
            .select("cleaned_ip")\
            .unique()\
            .collect()\
            .to_series()\
            .to_list()

        if not ip_with_missing_country_code_list:
            return self.combined_ip_location_df

        # Process IPs in parallel
        with ProcessPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            unique_ips = list(set(ip_with_missing_country_code_list))
            ts = time.time()
            
            result_dict = {
                "ip_from": [], "ip_to": [], "country_code": [], "country_name": [],
                "region_name": [], "city_name": [], "latitude": [], "longitude": [],
                "zip_code": [], "timezone": [], "geolocation_db_attempted": []
            }
            
            with tqdm(total=len(unique_ips), desc="Processing IPs") as pbar:
                future_to_ip = {
                    executor.submit(self.process_single_ip, ip): ip 
                    for ip in unique_ips
                }
                
                for future in as_completed(future_to_ip):
                    ip = future_to_ip[future]
                    try:
                        result = future.result()
                        if result is None:
                            pbar.update(1)
                            continue
                            
                        ip_int = int(ipaddress.IPv4Address(ip))
                        result_dict["ip_from"].append(ip_int)
                        result_dict["ip_to"].append(ip_int)
                        result_dict["country_code"].append(result.get("country_code"))
                        result_dict["country_name"].append(result.get("country_name"))
                        result_dict["region_name"].append(result.get("region_name"))
                        result_dict["city_name"].append(result.get("city_name"))
                        result_dict["latitude"].append(result.get("latitude"))
                        result_dict["longitude"].append(result.get("longitude"))
                        result_dict["zip_code"].append(result.get("zip_code"))
                        result_dict["timezone"].append(result.get("timezone"))
                        result_dict["geolocation_db_attempted"].append(True)
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Error processing IP {ip}: {str(e)}")
                        pbar.update(1)

            logger.info(f"Processed {len(unique_ips)} IPs in {time.time() - ts:.2f} seconds")
            result_df = pl.LazyFrame(result_dict)

        # Update combined database
        combined_ip_location_df = self.combined_ip_location_df\
            .filter(pl.col('country_code').is_not_null() | (pl.col('geolocation_db_attempted') == True))
        
        combined_ip_location_df = pl.concat(
            [combined_ip_location_df, result_df], 
            how="diagonal_relaxed"
        )
        
        # Save with temporary file for safety
        tmp_path = self.combined_ip_location_path + '.tmp'
        combined_ip_location_df.collect().write_parquet(
            tmp_path,
            compression="gzip"
        )
        pl.scan_parquet(tmp_path).collect().write_parquet(
            self.combined_ip_location_path,
            compression="gzip" 
        )
        os.remove(tmp_path)

        return pl.scan_parquet(self.combined_ip_location_path)
