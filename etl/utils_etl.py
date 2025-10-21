from sqlalchemy import Engine, text



def push_dimensions(co_sa, etl_conn):
    dim_product = extract.extract_product(co_sa)
    dim_product = transform.transform_product(dim_product)
    load.load(dim_product, etl_conn, 'dim_product', replace=True)

    dim_date = transform.transform_fecha()
    load.load(dim_date, etl_conn, 'dim_date', replace=True)

    dim_salesterritory = extract.extract_salesterritory(co_sa)
    dim_salesterritory = transform.transform_salesterritory(dim_salesterritory)
    load.load(dim_salesterritory, etl_conn, 'dim_salesterritory', replace=True)

    dim_salesreason = extract.extract_salesreason(co_sa)
    dim_salesreason = transform.transform_salesreason(dim_salesreason)  
    load.load(dim_salesreason, etl_conn, 'dim_salesreason', replace=True)

    dim_currency = extract.extract_currency(co_sa)
    dim_currency = transform.transform_currency(dim_currency)
    load.load(dim_currency, etl_conn, 'dim_currency', replace=True)

    dim_promotion = extract.extract_promotion(co_sa)
    dim_promotion = transform.transform_promotion(dim_promotion)
    load.load(dim_promotion, etl_conn, 'dim_promotion', replace=True)

    dim_customer = extract.extract_customer(co_sa)
    dim_customer = transform.transform_customer(dim_customer)
    load.load(dim_customer, etl_conn, 'dim_customer', replace=True)

   