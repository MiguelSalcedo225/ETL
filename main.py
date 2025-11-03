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
    '''
    dim_product = extract.extract_product(co_sa)

    dim_salesterritory = extract.extract_salesterritory(co_sa)
    dim_salesreason = extract.extract_salesreason(co_sa)
    dim_currency = extract.extract_currency(co_sa)
    dim_promotion = extract.extract_promotion(co_sa)
    '''
    dim_geography = extract.extract_geography(co_sa)
    dim_customer = extract.extract_customer(co_sa)

    # transform
    '''
    dim_product = transform.transform_product(dim_product)

    dim_salesterritory = transform.transform_salesterritory(dim_salesterritory)
    dim_date = transform.transform_fecha()
    dim_salesreason = transform.transform_salesreason(dim_salesreason)
    dim_currency = transform.transform_currency(dim_currency)
    dim_promotion = transform.transform_promotion(dim_promotion)
    '''
    dim_geography = transform.transform_geography(dim_geography)
    dim_customer = transform.transform_customer(dim_customer, dim_geography)
    '''
    load.load_data_product(dim_product, etl_conn)

    load.load_data_fecha(dim_date, etl_conn)
    load.load_data_salesterritory(dim_salesterritory, etl_conn)
    load.load_data_salesreason(dim_salesreason, etl_conn)
    load.load_data_currency(dim_currency, etl_conn)
    load.load_data_promotion(dim_promotion, etl_conn)
    '''
    load.load_data_geography(dim_geography, etl_conn)
    load.load_data_customer(dim_customer, etl_conn)

    print('success all facts loaded')
    

