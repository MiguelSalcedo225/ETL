import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy import text
import yaml
from sqlalchemy.dialects.postgresql import insert


def load_data_product(dim_product: DataFrame, etl_conn):
    dim_product.to_sql('dim_product', etl_conn, if_exists='append', index=False)

def load_data_fecha(dim_fecha: DataFrame, etl_conn: Engine):
    dim_fecha.to_sql('dim_date', etl_conn, if_exists='append', index=False)

def load_data_salesterritory(dim_salesterritory: DataFrame, etl_conn: Engine):
    dim_salesterritory.to_sql('dim_salesterritory', etl_conn, if_exists='append', index=False)

def load_data_salesreason(dim_salesreason: DataFrame, etl_conn: Engine):
    dim_salesreason.to_sql('dim_salesreason', etl_conn, if_exists='append', index=False)

def load_data_currency(dim_currency: DataFrame, etl_conn: Engine):
    dim_currency.to_sql('dim_currency', etl_conn, if_exists='append', index=False)

def load_data_promotion(dim_promotion: DataFrame, etl_conn: Engine):
    dim_promotion.to_sql('dim_promotion', etl_conn, if_exists='append', index=False)

def load_data_customer(dim_customer: DataFrame, etl_conn: Engine):
    dim_customer.to_sql('dim_customer', etl_conn, if_exists='append', index=False)

def load_data_hecho_internetsales(fact_internet_sales: DataFrame, etl_conn: Engine):
    fact_internet_sales.to_sql('fact_internetsales', etl_conn, if_exists='append', index=False)

def load_data_geography(dim_geography: DataFrame, etl_conn: Engine):
    dim_geography.to_sql('dim_geography', etl_conn, if_exists='append', index= False)

def load_data_employee(dim_employee: DataFrame, etl_conn: Engine):
    dim_employee.to_sql('dim_employee', etl_conn, if_exists='append', index= False)

def load_data_reseller(dim_reseller: DataFrame, etl_conn: Engine):
    dim_reseller.to_sql('dim_reseller', etl_conn, if_exists='append', index=False)

def load_data_fact_internet_sales(fact_internet_sales: DataFrame, etl_conn: Engine):
    fact_internet_sales.to_sql('fact_internet_sales', etl_conn, if_exists='append', index=False)
    
def load_data_fact_internet_sales_reason(fact_internet_sales_reason: DataFrame, etl_conn: Engine):
    fact_internet_sales_reason.to_sql('fact_internet_sales_reason', etl_conn, if_exists='append', index=False)

def load(table: DataFrame, etl_conn: Engine, tname, replace: bool = False):
    """

    :param table: table to load into the database
    :param etl_conn: sqlalchemy engine to connect to the database
    :param tname: table name to load into the database
    :param replace:  when true it deletes existing table data(rows)
    :return: void it just load the table to the database
    """
    # statement = insert(f'{table})
    # with etl_conn.connect() as conn:
    #     conn.execute(statement)
    if replace :
        with etl_conn.connect() as conn:
            conn.execute(text(f'Delete from {tname}'))
            conn.close()
        table.to_sql(f'{tname}', etl_conn, if_exists='append', index=False)
    else :
        table.to_sql(f'{tname}', etl_conn, if_exists='append', index=False)