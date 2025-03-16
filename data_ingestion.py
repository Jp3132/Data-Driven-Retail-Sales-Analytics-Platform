import pandas as pd
import os
from fredapi import Fred
import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes
import numpy as np
from typing import Optional, Dict, List, Union
import requests
import yfinance as yf
from datetime import datetime, timedelta
import logging
import json
from pathlib import Path
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DataSource:
    name: str
    api_endpoint: str
    api_key: str
    category: str

class FredToDB:
    def __init__(self, fred_api_key, db_user, db_pass, db_name, instance_connection_name):

        self.fred_api_key = fred_api_key
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name
        self.instance_connection_name = instance_connection_name

    def get_sales(self):
        # get data from FRED
        fred = Fred(api_key=self.fred_api_key)
        # Divide the data by 1000 to convert from millions of dollars to billions of dollars
        retail_sales = fred.get_series('RSXFSN', observation_start='2023-05-01')/1000
        # test the update functionality for the dashboard
        # retail_sales = pd.Series(data=[800000, 900000], index=pd.date_range(start='2023-05-01', periods=2, freq='MS'))/1000

        # Convert the data to a Pandas DataFrame
        df = pd.DataFrame(retail_sales, columns=['sales_amount'])

        # add a monthly frequencey to index date
        df.index.freq = 'MS'

        # Convert the index and sales values of the DataFrame into a list of tuples
        sales_tuples = [(index.date(), float(sales)) for index, sales in df.itertuples()]

        return sales_tuples

    def store_sales(self, sales_tuples):
        # Get environment variables for database connection
        DB_USER = self.db_user
        DB_PASS = self.db_pass
        DB_NAME = self.db_name
        INSTANCE_CONNECTION_NAME = self.instance_connection_name
a
        # Establish connection to google cloud sql
        # initialize Connector object
        connector = Connector()
        ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC

        # function to return the database connection object
        def getconn():
            conn = connector.connect(
                INSTANCE_CONNECTION_NAME,
                "pg8000",
                user=DB_USER,
                password=DB_PASS,
                db=DB_NAME,
                ip_type=ip_type,
            )
            return conn

        # create connection pool with 'creator' argument to our connection object function
        pool = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )

        # insert entries into table
        with pool.connect() as db_conn:
            # Insert the data into the table
            # Insert the data into the table if it doesn't already exist
            insert_stmt = sqlalchemy.text(
                "INSERT INTO retail_sales (sales_date, sales_amount) "
                "SELECT :sales_date, :sales_amount "
                "WHERE NOT EXISTS (SELECT 1 FROM retail_sales "
                "WHERE sales_date = :sales_date)"
            )
            for sale in sales_tuples:
                # insert entries into table
                db_conn.execute(insert_stmt, parameters={"sales_date": sale[0], "sales_amount": sale[1]})

            # # commit transactions
            db_conn.commit()

        connector.close()

class MarketDataIngestion:
    """Advanced market data ingestion system with multiple data sources and async capabilities"""
    
    def __init__(self, config_path: str = "config/data_sources.json"):
        """
        Initialize the data ingestion system
        
        Args:
            config_path: Path to configuration file containing API keys and endpoints
        """
        self.config_path = Path(config_path)
        self.data_sources: Dict[str, DataSource] = {}
        self.cache_dir = Path("cache")
        self.cache_dir.mkdir(exist_ok=True)
        self._load_config()
        
    def _load_config(self) -> None:
        """Load configuration from JSON file"""
        try:
            if not self.config_path.exists():
                logger.warning(f"Config file not found at {self.config_path}")
                return
                
            with open(self.config_path) as f:
                config = json.load(f)
                
            for source in config.get("data_sources", []):
                self.data_sources[source["name"]] = DataSource(**source)
                
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            
    async def fetch_market_data(
        self,
        start_date: str,
        end_date: str,
        indicators: List[str]
    ) -> Dict[str, pd.DataFrame]:
        """
        Asynchronously fetch market data from multiple sources
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            indicators: List of economic indicators to fetch
            
        Returns:
            Dictionary of DataFrames containing market data
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for indicator in indicators:
                for source_name, source in self.data_sources.items():
                    if indicator in self._get_source_indicators(source):
                        tasks.append(
                            self._fetch_single_source(
                                session,
                                source,
                                indicator,
                                start_date,
                                end_date
                            )
                        )
                        
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return self._process_results(results, indicators)
            
    def _get_source_indicators(self, source: DataSource) -> List[str]:
        """Get available indicators for a data source"""
        # Implementation would depend on the specific data source
        return []
        
    async def _fetch_single_source(
        self,
        session: aiohttp.ClientSession,
        source: DataSource,
        indicator: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Fetch data from a single source"""
        cache_file = self.cache_dir / f"{source.name}_{indicator}_{start_date}_{end_date}.parquet"
        
        # Check cache first
        if cache_file.exists():
            return pd.read_parquet(cache_file)
            
        try:
            async with session.get(
                source.api_endpoint,
                params={
                    "indicator": indicator,
                    "start_date": start_date,
                    "end_date": end_date,
                    "api_key": source.api_key
                }
            ) as response:
                response.raise_for_status()
                data = await response.json()
                df = self._parse_response(data, source.name)
                
                # Cache the results
                df.to_parquet(cache_file)
                return df
                
        except Exception as e:
            logger.error(f"Error fetching data from {source.name}: {str(e)}")
            return pd.DataFrame()
            
    def _parse_response(self, data: Dict, source_name: str) -> pd.DataFrame:
        """Parse API response into DataFrame"""
        if source_name == "fred":
            return self._parse_fred_data(data)
        elif source_name == "yahoo":
            return self._parse_yahoo_data(data)
        else:
            return pd.DataFrame(data)
            
    def _parse_fred_data(self, data: Dict) -> pd.DataFrame:
        """Parse FRED API response"""
        df = pd.DataFrame(data.get("observations", []))
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        return df
        
    def _parse_yahoo_data(self, data: Dict) -> pd.DataFrame:
        """Parse Yahoo Finance API response"""
        df = pd.DataFrame(data.get("prices", []))
        df["date"] = pd.to_datetime(df["date"], unit="s")
        df.set_index("date", inplace=True)
        return df
        
    def _process_results(
        self,
        results: List[pd.DataFrame],
        indicators: List[str]
    ) -> Dict[str, pd.DataFrame]:
        """Process and combine results from different sources"""
        processed_data = {}
        
        for indicator, df in zip(indicators, results):
            if isinstance(df, Exception):
                logger.error(f"Error processing {indicator}: {str(df)}")
                continue
                
            if not df.empty:
                # Apply quality checks
                df = self._apply_quality_checks(df)
                # Handle missing values
                df = self._handle_missing_values(df)
                processed_data[indicator] = df
                
        return processed_data
        
    def _apply_quality_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply data quality checks"""
        # Remove duplicates
        df = df[~df.index.duplicated(keep='first')]
        
        # Remove outliers using IQR method
        Q1 = df.quantile(0.25)
        Q3 = df.quantile(0.75)
        IQR = Q3 - Q1
        df = df[~((df < (Q1 - 1.5 * IQR)) | (df > (Q3 + 1.5 * IQR))).any(axis=1)]
        
        return df
        
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the data"""
        # Forward fill for up to 3 periods
        df = df.fillna(method='ffill', limit=3)
        
        # For any remaining missing values, use interpolation
        df = df.interpolate(method='time')
        
        return df
        
    def get_available_indicators(self) -> List[str]:
        """Get list of all available indicators"""
        indicators = set()
        for source in self.data_sources.values():
            indicators.update(self._get_source_indicators(source))
        return sorted(list(indicators))
        
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate data quality"""
        if df.empty:
            return False
            
        # Check for minimum required data points
        if len(df) < 30:  # Minimum 30 data points required
            return False
            
        # Check for too many missing values
        missing_pct = df.isnull().mean()
        if (missing_pct > 0.1).any():  # More than 10% missing values
            return False
            
        return True
        
    def export_data(
        self,
        data: Dict[str, pd.DataFrame],
        format: str = "csv",
        output_dir: str = "exports"
    ) -> None:
        """Export data to various formats"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        for indicator, df in data.items():
            file_path = output_path / f"{indicator}.{format}"
            if format == "csv":
                df.to_csv(file_path)
            elif format == "parquet":
                df.to_parquet(file_path)
            elif format == "json":
                df.to_json(file_path)
            else:
                raise ValueError(f"Unsupported format: {format}")
                
        logger.info(f"Data exported to {output_dir}")

# Example usage
if __name__ == "__main__":
    async def main():
        ingestion = MarketDataIngestion()
        indicators = ["GDP", "UNEMPLOYMENT", "CPI"]
        start_date = "2020-01-01"
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        data = await ingestion.fetch_market_data(
            start_date=start_date,
            end_date=end_date,
            indicators=indicators
        )
        
        # Export the data
        ingestion.export_data(data, format="parquet")
        
    asyncio.run(main())
