import pandas as pd
import datetime
from datetime import date
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
import yaml
from etl import extract, transform, load, utils_etl
import psycopg2

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)


with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)
    config_co = config['CO_SA']
    config_etl = config['ETL_PRO']

# Construct the database URL
url_co = (f"{config_co['drivername']}://{config_co['user']}:{config_co['password']}@{config_co['host']}:"
          f"{config_co['port']}/{config_co['dbname']}")
url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:"
           f"{config_etl['port']}/{config_etl['dbname']}")
# Create the SQLAlchemy Engine
co_sa = create_engine(url_co)
etl_conn = create_engine(url_etl)

inspector = inspect(etl_conn)
tnames = inspector.get_table_names()




if config['LOAD_DIMENSIONS']:
    dim_product = extract.extract_product(co_sa)
    # transform
    dim_product = transform.transform_product(dim_product)
    dim_date = transform.transform_fecha()
    load.load_data_product(dim_product, etl_conn)
    load.load_data_fecha(dim_date, etl_conn)
    print('success all facts loaded')
