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
    salesorderdetail = pd.read_sql_table('salesorderdetail', conection, schema='sales')
    productproductphoto = pd.read_sql_table('productproductphoto', conection, schema='production')
    productlistpricehistory = pd.read_sql_table('productlistpricehistory', conection, schema='production')
    return [product, productsubcategory, productmodel, productmodelproductdescriptionculture, productdescription, productphoto, salesorderdetail, 
            productproductphoto, productlistpricehistory ]


def extract_salesterritory(conection: Engine):
    salesterritory = pd.read_sql_table('salesterritory', conection, schema='sales')
    countryregion = pd.read_sql_table('countryregion', conection, schema='person')
    return salesterritory, countryregion

def extract_salesreason(conection: Engine):
    salesreason = pd.read_sql_table('salesreason', conection, schema='sales')
    return salesreason

def extract_currency(conection: Engine):
    currency = pd.read_sql_table('currency', conection, schema='sales')
    return currency

def extract_promotion(conection: Engine):
    promotion = pd.read_sql_table('specialoffer', conection, schema='sales')
    return promotion

def extract_geography(conection: Engine):
    address = pd.read_sql_table('address', conection, schema='person')
    stateprovince = pd.read_sql_table('stateprovince', conection, schema='person')
    countryregion = pd.read_sql_table('countryregion', conection, schema='person')
    salesterritory = pd.read_sql_table('salesterritory', conection, schema='sales')
    business_entity_address = pd.read_sql_table('businessentityaddress', conection, schema='person')
    customer = pd.read_sql_table('customer', conection, schema='sales')
    store = pd.read_sql_table('store', conection, schema='sales')
    return [address, stateprovince, countryregion, salesterritory, business_entity_address, customer, store]


def extract_customer(conection: Engine):
    customer = pd.read_sql_table('customer', conection, schema='sales')
    person = pd.read_sql_table('person', conection, schema='person')
    address = pd.read_sql_table('address', conection, schema='person')
    personphone = pd.read_sql_table('personphone', conection, schema='person')
    personemailaddress = pd.read_sql_table('emailaddress', conection, schema='person')
    businessentityaddress = pd.read_sql_table('businessentityaddress', conection, schema='person')
    stateprovince = pd.read_sql_table('stateprovince', conection, schema='person')
    countryregion = pd.read_sql_table('countryregion', conection, schema='person')
    return [customer, person, address, personphone, personemailaddress, businessentityaddress, stateprovince, countryregion]

def extract_employee(connection: Engine) -> list[pd.DataFrame]:
    employee = pd.read_sql_table('employee', connection, schema='humanresources')
    person = pd.read_sql_table('person', connection, schema='person')
    personemailaddress = pd.read_sql_table('emailaddress', connection, schema='person')
    personphone = pd.read_sql_table('personphone', connection, schema='person')
    employeepayhistory = pd.read_sql_table('employeepayhistory', connection, schema='humanresources')
    employeedepartmenthistory = pd.read_sql_table('employeedepartmenthistory', connection, schema='humanresources')
    department = pd.read_sql_table('department', connection, schema='humanresources')
    sales = pd.read_sql_table('salesperson', connection, schema='sales')
    return [
        employee,
        person,
        personemailaddress,
        personphone,
        employeepayhistory,
        employeedepartmenthistory,
        department,
        sales,
    ]

def extract_reseller(conection: Engine):
    store = pd.read_sql_table('store', conection, schema='sales')
    customer = pd.read_sql_table('customer', conection, schema='sales')
    businessentityaddress = pd.read_sql_table('businessentityaddress', conection, schema='person')
    address = pd.read_sql_table('address', conection, schema='person')
    person = pd.read_sql_table('person', conection, schema='person')
    stateprovince = pd.read_sql_table('stateprovince', conection, schema='person')
    countryregion = pd.read_sql_table('countryregion', conection, schema='person')
    personphone = pd.read_sql_table('personphone', conection, schema='person')
    businessentitycontact = pd.read_sql_table('businessentitycontact', conection, schema='person')
    salesorderheader = pd.read_sql_table('salesorderheader', conection, schema='sales')
    
    return [store, customer, businessentityaddress, address, person, stateprovince, countryregion, personphone, businessentitycontact, salesorderheader]

def extract_fact_internet_sales(conection: Engine):
    salesorderheader = pd.read_sql_table('salesorderheader', conection, schema='sales')
    salesorderdetail = pd.read_sql_table('salesorderdetail', conection, schema='sales')
    currencyrate = pd.read_sql_table('currencyrate', conection, schema='sales')
    product = pd.read_sql_table('product', conection, schema='production')
    customer = pd.read_sql_table('customer', conection, schema='sales')
    return [salesorderheader, salesorderdetail, currencyrate, product, customer]

def extract_fact_internet_sales_reason(conection: Engine):
    salesorderheadersalesreason = pd.read_sql_table('salesorderheadersalesreason', conection, schema='sales')
    salesorderheader = pd.read_sql_table('salesorderheader', conection, schema='sales')
    salesorderdetail = pd.read_sql_table('salesorderdetail', conection, schema='sales')
    return [salesorderheadersalesreason, salesorderheader, salesorderdetail]

def extract_fact_reseller_sales(conection: Engine):
    salesorderheader = pd.read_sql_table('salesorderheader', conection, schema='sales')
    salesorderdetail = pd.read_sql_table('salesorderdetail', conection, schema='sales')
    currencyrate = pd.read_sql_table('currencyrate', conection, schema='sales')
    product = pd.read_sql_table('product', conection, schema='production')
    customer = pd.read_sql_table('customer', conection, schema='sales')
    store = pd.read_sql_table('store', conection, schema='sales')
    employee = pd.read_sql_table('employee', conection, schema='humanresources')
    return [salesorderheader, salesorderdetail, currencyrate, product, customer, store, employee]