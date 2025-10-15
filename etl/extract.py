import pandas as pd
from sqlalchemy.engine import Engine


def extract(tables : list,conection: Engine)-> pd.DataFrame:
    """
    :param conection: the conectionnection to the database
    :param tables: the tables to extract
    :return: a list of tables in df format
    """
    a = []
    for i in tables:
        aux = pd.read_sql_table(i, conection)
        a.append(aux)
    return a



def extract_product(conection: Engine):
    product = pd.read_sql_table('product', conection, schema='production')
    productsubcategory = pd.read_sql_table('productsubcategory', conection, schema='production')
    productmodel = pd.read_sql_table('productmodel', conection, schema='production')
    productmodelproductdescriptionculture = pd.read_sql_table('productmodelproductdescriptionculture', conection, schema='production')
    productdescription = pd.read_sql_table('productdescription', conection, schema='production')
    productphoto = pd.read_sql_table('productphoto', conection, schema='production')
    culture = pd.read_sql_table('culture', conection, schema='production')
    productproductphoto = pd.read_sql_table('productproductphoto', conection, schema='production')
    return [product, productsubcategory, productmodel, productmodelproductdescriptionculture, productdescription, productphoto, culture, productproductphoto]



