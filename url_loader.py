## Url loader module for dr Kingo
## Class HAI5016 - by camphouse.me

import pandas as pd
from loguru import logger
from database import get_engine
import os

from dotenv import load_dotenv
load_dotenv()

logger.add("logs/url_loader.log", rotation="10 MB")

def get_url_df(url):
    try:
        df = pd.read_excel(url)
        logger.info(f"Excel file downloaded and loaded {len(df)} urls into DataFrame successfully.")
        return df
    except Exception as e:
        logger.error(f"Failed to download or load the Excel file: {e}")
        raise

def prepare_dataframe(df):
    try:
        df = df[['campus', 'info', 'url']]
        logger.info("DataFrame prepared for insertion successfully.")
        return df
    except Exception as e:
        logger.error(f"An error occurred while preparing the DataFrame: {e}")
        raise

def load_data_to_sql(df, engine):
    try:
        df.to_sql('skku_urls', engine, if_exists='replace', index=False, schema='public', method='multi')
        logger.info("Data loaded successfully into the 'skku_urls' table.")
    except Exception as e:
        logger.error(f"An error occurred while loading the data into the table: {e}")
        raise

def load_urls(excel_url):
    # Database engine
    try:
        engine = get_engine()
        logger.info("Database engine created successfully.")
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise
    df = get_url_df(excel_url)
    load_data_to_sql(df, engine)

if __name__ == "__main__":
    logger.info("URL loader module run directly.")
    excel_url = os.getenv('EXCEL_URL')
    load_urls('https://camphouse.me/assets/bin/skku-urls.xlsx')
    logger.info("URL loader module run completed.")
