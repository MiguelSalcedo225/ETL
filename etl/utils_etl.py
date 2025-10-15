from sqlalchemy import Engine, text




def push_dimensions(co_sa, etl_conn):
    dim_product = extract.extract_product(co_sa)
    dim_product = transform.transform_product(dim_product)
    load.load(dim_product, etl_conn, 'dim_product', replace=True)

   