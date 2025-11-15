from datetime import timedelta, date, datetime
from typing import Tuple, Any
from deep_translator import GoogleTranslator
import numpy as np
import pandas as pd
from pandas import DataFrame
import xml.etree.ElementTree as ET


def transform_product(args) -> pd.DataFrame:
    (product, productsubcategory, productmodel, productmodelproductdescriptionculture,
     productdescription, productphoto, salesorderdetail, productproductphoto) = args
    product.rename(columns={
        'productid': 'productkey',
        'productnumber': 'productalternatekey',
        'productsubcategoryid': 'productsubcategorykey',
        'name': 'englishproductname',
        'sellstartdate': 'startdate',
        'sellenddate': 'enddate'
    }, inplace=True)
    product = product.merge(productsubcategory, left_on='productsubcategorykey',right_on='productsubcategoryid', how='left')
    productmodel = productmodel.drop(columns=['catalogdescription', 'instructions'])
    product = product.merge(productmodel[['productmodelid', 'name']].rename(columns={'name': 'modelname'}), left_on='productmodelid', right_on='productmodelid', how='left')
    product = product.merge(productproductphoto[['productid', 'productphotoid']], how='left',left_on='productkey', right_on='productid')
    product = product.merge(productphoto[['productphotoid', 'largephoto']], how='left', on='productphotoid')
    product['status'] = product['enddate'].apply(lambda x: 'Current' if pd.isna(x) else 'Discontinued')
    product['saved'] = pd.Timestamp.now()
    def generar_sizerange(size):
        if pd.isna(size):
            return 'NA'
        elif isinstance(size, str) and not size.isdigit():
            return size
        else:
            try:
                num = int(size)
                return f"{num}-{num+2} CM"
            except ValueError:
                return 'NA'
    product['sizerange'] = product['size'].apply(generar_sizerange)
    desc = (
        productmodelproductdescriptionculture
        .merge(productdescription, on='productdescriptionid', how='left')
        .drop_duplicates(subset=['productmodelid'])
        [['productmodelid', 'description']]
    )
    product = product.merge(desc, on='productmodelid', how='left')
    traduccion_cache = {}
    def traducir_a_ingles(texto):
        if pd.isna(texto) or texto.strip() == "":
            return ""
        if texto in traduccion_cache:
            return traduccion_cache[texto]
        try:
            traducido = GoogleTranslator(source='auto', target='en').translate(texto)
            traduccion_cache[texto] = traducido
            return traducido
        except Exception:
            traduccion_cache[texto] = ""
            return ""
    product['englishdescription'] = product['description'].apply(traducir_a_ingles)
    dealer_prices = (
        salesorderdetail
        .assign(dealerprice=lambda df: df['unitprice'] * (1 - df['unitpricediscount']))
        .groupby('productid', as_index=False)['dealerprice']
        .mean()
        .rename(columns={'productid': 'productkey'})
    )
    product = product.merge(dealer_prices, on='productkey', how='left')
    product['dealerprice'] = product['dealerprice'].round(4)

    product.drop(columns=[
        'rowguid_x', 'rowguid_y', 'modifieddate_y', 'modifieddate_x',
        'productmodelid', 'productsubcategoryid', 'productid',
        'productphotoid', 'name', 'productkey'
    ], inplace=True)
    dim_product = product[[
        'productalternatekey', 'productsubcategorykey',
        'weightunitmeasurecode', 'sizeunitmeasurecode',
        'englishproductname', 'standardcost', 'finishedgoodsflag',
        'color', 'safetystocklevel', 'reorderpoint', 'listprice',
        'size', 'sizerange', 'weight', 'daystomanufacture',
        'productline', 'dealerprice', 'class', 'style', 'modelname', 'largephoto',
        'englishdescription', 'startdate', 'enddate', 'status', 'saved'
    ]]
    dim_product.reset_index(drop=True, inplace=True)
    return dim_product

def transform_fecha() -> DataFrame:
    dimdate = pd.DataFrame({
        "fulldatealternatekey": pd.date_range(start='2005-01-01', end='2014-12-31', freq='D')
    })
    dimdate["datekey"] = dimdate["fulldatealternatekey"].dt.strftime("%Y%m%d").astype(int)
    dimdate["daynumberofweek"] = dimdate["fulldatealternatekey"].dt.weekday + 2
    dimdate["daynumberofweek"] = dimdate["daynumberofweek"].where(dimdate["daynumberofweek"] <= 7, 1)
    dimdate["daynumberofmonth"] = dimdate["fulldatealternatekey"].dt.day
    dimdate["daynumberofyear"] = dimdate["fulldatealternatekey"].dt.day_of_year
    dimdate["weeknumberofyear"] = dimdate["fulldatealternatekey"].dt.strftime("%U").astype(int) + 1
    dimdate["monthnumberofyear"] = dimdate["fulldatealternatekey"].dt.month
    dimdate["calendarquarter"] = dimdate["fulldatealternatekey"].dt.quarter
    dimdate["calendaryear"] = dimdate["fulldatealternatekey"].dt.year
    dimdate["calendarsemester"] = (dimdate["monthnumberofyear"] > 6).astype(int) + 1
    dimdate["fiscalyear"] = dimdate["fulldatealternatekey"].apply(lambda x: x.year if x.month >= 7 else x.year)
    dimdate["fiscalquarter"] = ((dimdate["fulldatealternatekey"].dt.month - 7) % 12 // 3 + 1)
    dimdate["fiscalsemester"] = (dimdate["fiscalquarter"] > 2).astype(int) + 1
    dimdate["englishdaynameofweek"] = dimdate["fulldatealternatekey"].dt.day_name()
    dimdate["englishmonthname"] = dimdate["fulldatealternatekey"].dt.month_name()
    dias_es = {
        'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles','Thursday': 'Jueves', 'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'}
    dias_fr = {
        'Monday': 'Lundi', 'Tuesday': 'Mardi', 'Wednesday': 'Mercredi','Thursday': 'Jeudi', 'Friday': 'Vendredi', 'Saturday': 'Samedi', 'Sunday': 'Dimanche'}
    meses_es = {
        'January': 'Enero', 'February': 'Febrero', 'March': 'Marzo', 'April': 'Abril','May': 'Mayo', 'June': 'Junio', 'July': 'Julio', 'August': 'Agosto',
        'September': 'Septiembre', 'October': 'Octubre', 'November': 'Noviembre', 'December': 'Diciembre'}
    meses_fr = {
        'January': 'Janvier', 'February': 'Février', 'March': 'Mars', 'April': 'Avril','May': 'Mai', 'June': 'Juin', 'July': 'Juillet', 'August': 'Août',
        'September': 'Septembre', 'October': 'Octobre', 'November': 'Novembre', 'December': 'Décembre'}
    dimdate["spanishdaynameofweek"] = dimdate["englishdaynameofweek"].map(dias_es)
    dimdate["frenchdaynameofweek"] = dimdate["englishdaynameofweek"].map(dias_fr)
    dimdate["spanishmonthname"] = dimdate["englishmonthname"].map(meses_es)
    dimdate["frenchmonthname"] = dimdate["englishmonthname"].map(meses_fr)
    dimdate = dimdate[[
        "datekey", "fulldatealternatekey", "daynumberofweek",
        "englishdaynameofweek", "spanishdaynameofweek", "frenchdaynameofweek",
        "daynumberofmonth", "daynumberofyear", "weeknumberofyear",
        "englishmonthname", "spanishmonthname", "frenchmonthname",
        "monthnumberofyear", "calendarquarter", "calendaryear", "calendarsemester",
        "fiscalquarter", "fiscalyear", "fiscalsemester"
    ]]
    return dimdate

def transform_salesterritory(args: DataFrame) -> DataFrame:
    (salesterritory, countryregion) = args
    salesterritory.drop(columns=['modifieddate', 'rowguid', 'costlastyear', 'costytd', 'saleslastyear', 'salesytd'], inplace=True)
    salesterritory.rename(columns={
        'territoryid': 'salesterritoryalternatekey',
        'name': 'salesterritoryregion',
        'countryregioncode': 'countryregioncode',
        'group': 'salesterritorygroup'
    }, inplace=True)
    countryregion.rename(columns={'name': 'salesterritorycountry'}, inplace=True)
    salesterritory = salesterritory.merge(countryregion[['countryregioncode', 'salesterritorycountry']], on='countryregioncode', how='left')
    salesterritory.drop(columns=['countryregioncode'], inplace=True)
    dim_salesterritory = salesterritory[[
        'salesterritoryalternatekey', 'salesterritoryregion', 'salesterritorycountry',
        'salesterritorygroup'
    ]]
    dim_salesterritory.reset_index(drop=True, inplace=True)
    return dim_salesterritory

def transform_salesreason(salesreason: DataFrame) -> DataFrame:
    salesreason.rename(columns = {
        'salesreasonid': 'salesreasonalternatekey',
        'name': 'salesreasonname',
        'reasontype': 'salesreasontype'
    }, inplace=True)
    salesreason.drop(columns='modifieddate', inplace=True)
    dim_salesreason = salesreason
    return dim_salesreason

def transform_currency(currency: DataFrame) -> DataFrame:
    currency.drop(columns = ['modifieddate'], inplace=True)
    currency.rename(columns = {
        'currencycode': 'currencyalternatekey',
        'name': 'currencyname'
    }, inplace=True)
    dim_currency = currency
    dim_currency = currency.sort_values('currencyname').reset_index(drop=True)
    return dim_currency

def transform_promotion(promotion: DataFrame) -> DataFrame:
    promotion.drop(columns = ['modifieddate','rowguid'], inplace=True)
    promotion.rename(columns = {
        'specialofferid': 'promotionalternatekey',
        'description': 'promotiondescription',
        'type': 'promotiontype',
        'category': 'promotioncategory',
        'discountpct': 'promotiondiscountpct'
    }, inplace=True)
    dim_promotion = promotion
    return dim_promotion


def transform_geography(args: DataFrame) -> DataFrame:
    (address, stateprovince, countryregion, salesterritory, business_entity_address, customer, store) = args

    address.drop(columns=['modifieddate','rowguid','addressline1','addressline2','spatiallocation'], inplace=True)
    stateprovince.rename(columns={'name': 'stateprovincename'}, inplace=True)
    address = address.merge(stateprovince[['stateprovinceid','stateprovincename', 'countryregioncode', 'stateprovincecode', 'territoryid']], on='stateprovinceid', how='left')
    address = address.merge(salesterritory[['territoryid']], on='territoryid', how='left')
    address.rename(columns={'territoryid': 'salesterritorykey'}, inplace=True)  
    address = address.merge(countryregion[['countryregioncode','name']].rename(columns={'name': 'countryregionname'}), on='countryregioncode', how='left')
    # Filtrar direcciones que tienen clientes o tiendas
    customer_business_ids = customer[['personid']].dropna().rename(columns={'personid': 'businessentityid'})
    store_business_ids = store[['businessentityid']].dropna()
    valid_business_ids = pd.concat([customer_business_ids, store_business_ids]).drop_duplicates()
    # Filtrar business_entity_address para obtener solo las direcciones con clientes o tiendas
    valid_addresses = business_entity_address.merge(valid_business_ids, on='businessentityid', how='inner')
    dim_geography = address.merge(valid_addresses[['addressid']].drop_duplicates(), on='addressid',how='inner')
    dim_geography.drop(columns=['stateprovinceid','addressid'], inplace=True)
    dim_geography = dim_geography.drop_duplicates(subset=[
        'city', 'postalcode', 'stateprovincecode', 'stateprovincename', 
        'countryregioncode', 'countryregionname', 'salesterritorykey'
    ])
    dim_geography = dim_geography.sort_values(['countryregioncode', 'stateprovincecode', 'city'])
    base_ip = "198.51.100.{}"
    ip_addresses = [base_ip.format(i + 2) for i in range(len(dim_geography))]
    dim_geography['ipaddresslocator'] = ip_addresses
    dim_geography['geographykey'] = range(1, len(dim_geography) + 1)
    dim_geography = dim_geography[[
        'geographykey','city', 'stateprovincecode', 'stateprovincename', 
        'countryregioncode', 'countryregionname', 'postalcode', 'salesterritorykey', 'ipaddresslocator'
    ]]   
    return dim_geography

def transform_customer(args: DataFrame, dim_geography: DataFrame) -> DataFrame:

    none_xml_columns_dict = {
    'birthdate': None,
    'gender': None,
    'maritalstatus': None,
    'yearlyincome': None,
    'totalchildren': None,
    'numberchildrenathome': None,
    'education': None,
    'occupation': None,
    'home_owner_flag': None,
    'numbercarsowned': None,
    'datefirstpurchase': None,
    'commutedistance': None,
}

    def extract_demographics(xml_string):
        if pd.isna(xml_string):
            return none_xml_columns_dict
        try:
            root = ET.fromstring(xml_string)
            ns = {'ns': root.tag.split('}')[0].strip('{')}
            
            def namespace_find(tag):
                el = root.find(f'ns:{tag}', ns)
                return el.text if el is not None else None

            return {
                'birthdate': namespace_find('BirthDate'),
                'gender': namespace_find('Gender'),
                'maritalstatus': namespace_find('MaritalStatus'),
                'yearlyincome': namespace_find('YearlyIncome'),
                'totalchildren': namespace_find('TotalChildren'),
                'numberchildrenathome': namespace_find('NumberChildrenAtHome'),
                'education': namespace_find('Education'),
                'occupation': namespace_find('Occupation'),
                'homeownerflag': namespace_find('HomeOwnerFlag'),
                'numbercarsowned': namespace_find('NumberCarsOwned'),
                'datefirstpurchase': namespace_find('DateFirstPurchase'),
                'commutedistance': namespace_find('CommuteDistance'),
            }

        except Exception:
            return none_xml_columns_dict
    
    (customer, person, address, personphone, personemailaddress, businessentityaddress, stateprovince, countryregion) = args
    customer.rename(columns={
        'customerid': 'customerkey',
        'accountnumber': 'customeralternatekey',
    }, inplace=True)
    person_clientes = person[person['persontype'] == 'IN']
    customer = customer.merge(person_clientes[['businessentityid','title','firstname', 'middlename', 'lastname','namestyle', 'suffix', 'demographics']], left_on='personid', right_on='businessentityid', how='inner')
    demographics_df = customer['demographics'].apply(extract_demographics).apply(pd.Series)
    customer = pd.concat([customer.drop('demographics', axis=1), demographics_df], axis=1)
    customer = customer.merge(personphone[['businessentityid','phonenumber']], left_on='businessentityid', right_on='businessentityid', how='left')
    customer = customer.merge(personemailaddress[['businessentityid','emailaddress']], left_on='businessentityid', right_on='businessentityid', how='left')

    customer_address = businessentityaddress[businessentityaddress['addresstypeid'] == 2 ].drop_duplicates('businessentityid')
    customer = customer.merge(customer_address[['businessentityid', 'addressid']],left_on='businessentityid', right_on='businessentityid', how='left')
    customer = customer.merge(address[['addressid', 'city', 'postalcode', 'stateprovinceid', 'addressline1','addressline2']],left_on='addressid', right_on='addressid', how='left')
    customer = customer.merge(stateprovince[['stateprovinceid', 'stateprovincecode', 'countryregioncode']],left_on='stateprovinceid', right_on='stateprovinceid', how='left')
    customer = customer.merge(countryregion[['countryregioncode', 'name']].rename(columns={'name': 'countryregionname'}),on='countryregioncode', how='left')
    
    customer = customer.merge(
        dim_geography[['geographykey', 'city', 'stateprovincecode', 'countryregioncode', 'postalcode']],
        on=['city', 'stateprovincecode', 'countryregioncode', 'postalcode'],
        how='left'
    )

    customer.drop(columns=[
        'businessentityid', 'storeid', 'rowguid', 'modifieddate', 'personid',
        'addressid', 'stateprovinceid', 'city', 'stateprovincecode', 'countryregioncode', 
        'postalcode', 'countryregionname', 'territoryid'
    ], inplace=True)
    customer = customer[customer['firstname'].notnull()].drop_duplicates('customerkey')
    dim_customer = customer
    dim_customer = dim_customer.drop_duplicates(subset=[
        'customerkey', 'customeralternatekey', 'title', 'firstname', 'middlename',
        'lastname', 'namestyle', 'birthdate', 'maritalstatus', 'suffix', 'gender',
        'emailaddress', 'yearlyincome', 'totalchildren', 'numberchildrenathome',
        'education', 'occupation', 'homeownerflag', 'numbercarsowned', 'addressline1',
        'addressline2', 'phonenumber', 'datefirstpurchase', 'commutedistance'
    ])

    dim_customer = dim_customer[[
        'customerkey', 'geographykey', 'customeralternatekey', 'title', 'firstname', 'middlename',
        'lastname', 'namestyle', 'birthdate', 'maritalstatus', 'suffix', 'gender',
        'emailaddress', 'yearlyincome', 'totalchildren', 'numberchildrenathome',
        'education', 'occupation', 'homeownerflag', 'numbercarsowned', 'addressline1',
        'addressline2', 'phonenumber', 'datefirstpurchase', 'commutedistance'
    ]]
    return dim_customer

def transform_employee(args: DataFrame) -> DataFrame:
    (employee,
    person,
    personemailaddress,
    personphone,
    employeepayhistory,
    employeedepartmenthistory,
    department,
    sales
    ) = args
    dim_employee = person[['businessentityid', 'firstname', 'lastname', 'middlename', 'namestyle']]
    dim_employee = dim_employee.merge(employee[['businessentityid',
                             'nationalidnumber',
                             'jobtitle', 
                             'hiredate', 
                             'birthdate', 
                             'loginid', 
                             'maritalstatus', 
                             'salariedflag',
                             'gender',
                             'vacationhours',
                             'sickleavehours',
                             'currentflag']], on = 'businessentityid')
    dim_employee = dim_employee.merge(personemailaddress[['businessentityid', 'emailaddress']], on = 'businessentityid')
    dim_employee = dim_employee.merge(personphone[['businessentityid', 'phonenumber']], on = 'businessentityid')
    dim_employee = dim_employee.merge(employeepayhistory[['businessentityid', 'payfrequency', 'rate']], on = 'businessentityid')
    dim_employee = dim_employee.merge(employeedepartmenthistory[['businessentityid', 'startdate', 'enddate', 'departmentid']], on = 'businessentityid')
    dim_employee = dim_employee.merge(department[['departmentid', 'name']], on = 'departmentid')
    dim_employee['salespersonflag'] = np.where(
        dim_employee['name'] == 'Sales',
        True,
        False
    )
    dim_employee = dim_employee.merge(sales[['businessentityid', 'territoryid']], on = 'businessentityid', how='left')

    dim_employee.rename(columns={'businessentityid' : 'employeekey',
                                 'nationalidnumber' : 'employeenationalidalternatekey',
                                 'jobtitle' : 'title',
                                 'phonenumber': 'phone',
                                 'rate': 'baserate',
                                 'name': 'departmentname',
                                 'territoryid' : 'salesterritorykey'                             
                                 }, 
                        inplace=True)
    
    dim_employee.drop('departmentid', axis=1, inplace=True)
    
    
    dim_employee.drop_duplicates(subset=['employeekey'], inplace=True)
    dim_employee['hiredate'] = pd.to_datetime(dim_employee['hiredate']).dt.date
    dim_employee['birthdate'] = pd.to_datetime(dim_employee['birthdate']).dt.date
    dim_employee['startdate'] = pd.to_datetime(dim_employee['startdate']).dt.date
    dim_employee['enddate'] = pd.to_datetime(dim_employee['enddate']).dt.date
    dim_employee['emergencycontactphone'] = dim_employee['phone']
    dim_employee['emergencycontactname'] = (
    dim_employee['firstname'].fillna('') + ' ' + dim_employee['lastname'].fillna('')
    )
    dim_employee['status'] = np.where(dim_employee['enddate'].isna(), 'current', pd.NA)
    dim_employee['parentemployeenationalidalternatekey'] = pd.NA

    column_order = [
    'employeekey',
    'employeenationalidalternatekey',
    'parentemployeenationalidalternatekey',
    'salesterritorykey',
    'firstname',
    'lastname',
    'middlename',
    'namestyle',
    'title',
    'hiredate',
    'birthdate',
    'loginid',
    'emailaddress',
    'phone',
    'maritalstatus',
    'emergencycontactname',
    'emergencycontactphone',
    'salariedflag',
    'gender',
    'payfrequency',
    'baserate',
    'vacationhours',
    'sickleavehours',
    'currentflag',
    'salespersonflag',
    'departmentname',
    'startdate',
    'enddate',
    'status'
    ]

    dim_employee = dim_employee.reindex(columns=column_order)

    dim_employee['salesterritorykey'] = dim_employee['salesterritorykey'].fillna(11).astype('int')
    dim_employee['firstname'] = dim_employee['firstname'].astype(str)
    dim_employee['lastname'] = dim_employee['lastname'].astype(str)
    dim_employee['namestyle'] = dim_employee['namestyle'].astype(bool)
    dim_employee['payfrequency'] = dim_employee['payfrequency'].astype('int16')
    dim_employee['baserate'] = dim_employee['baserate'].astype(float)
    dim_employee['vacationhours'] = dim_employee['vacationhours'].astype('int16')
    dim_employee['sickleavehours'] = dim_employee['sickleavehours'].astype('int16')
    dim_employee['currentflag'] = dim_employee['currentflag'].astype(bool)
    dim_employee['salespersonflag'] = dim_employee['salespersonflag'].astype(bool)
    
    return dim_employee

def transform_reseller(args: DataFrame, dim_geography: DataFrame) -> DataFrame:
    (store, customer, businessentityaddress, address, 
     person, stateprovince, countryregion, personphone, businessentitycontact, businessentity) = args  

    def extract_demographics(xml_string):
        if pd.isna(xml_string):
            return {}
        try:
            root = ET.fromstring(xml_string)
            ns = {'ns': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {'ns': ''}
            
            def namespace_find(tag):
                el = root.find(f'.//{tag}') if not ns['ns'] else root.find(f'ns:{tag}', ns)
                return el.text if el is not None else None
            
            return {
                'businesstype': namespace_find('BusinessType'),
                'numberemployees': namespace_find('NumberEmployees'),
                'annualsales': namespace_find('AnnualSales'),
                'bankname': namespace_find('BankName'),
                'minpaymenttype': namespace_find('MinPaymentType'),
                'minpaymentamount': namespace_find('MinPaymentAmount'),
                'annualrevenue': namespace_find('AnnualRevenue'),
                'productline': namespace_find('Specialty'),
                'yearopened': namespace_find('YearOpened')
            }
        except Exception as e:
            print(f"Error parsing demographics: {e}")
            return {}

    reseller = customer[
        customer['storeid'].notnull() & customer['personid'].isnull()].copy()
    
    
    reseller.drop(columns=['rowguid', 'modifieddate'], inplace=True)
    reseller.rename(columns={'accountnumber': 'reselleralternatekey'}, inplace=True)

    reseller = reseller.merge(
        store[['businessentityid', 'name', 'demographics']],
        left_on='storeid', right_on='businessentityid', how='left'
    )

    demo_data = reseller['demographics'].apply(extract_demographics).apply(pd.Series)
    reseller = pd.concat([reseller, demo_data], axis=1)

    reseller = reseller.merge(businessentity[['businessentityid']], on='businessentityid', how='left')
    reseller = reseller.merge(businessentitycontact[['businessentityid', 'personid']], on='businessentityid', how='left')
    reseller = reseller.merge(person[['businessentityid']], on='businessentityid', how='left')
    reseller = reseller.merge(personphone[['businessentityid', 'phonenumber']], on='businessentityid', how='left')
    reseller = reseller.merge(businessentityaddress[['businessentityid', 'addressid']], on='businessentityid', how='left')
    reseller = reseller.merge(address[['addressid', 'city', 'postalcode', 'stateprovinceid', 'addressline1','addressline2']], on='addressid', how='left') 
    reseller = reseller.merge(stateprovince[['stateprovinceid', 'stateprovincecode', 'countryregioncode']],left_on='stateprovinceid', right_on='stateprovinceid', how='left')
    reseller = reseller.merge(countryregion[['countryregioncode', 'name']].rename(columns={'name': 'countryregionname'}),on='countryregioncode', how='left')
    reseller = reseller.merge(
        dim_geography[['geographykey', 'city', 'stateprovincecode', 'countryregioncode', 'postalcode']],
        on=['city', 'stateprovincecode', 'countryregioncode', 'postalcode'],
        how='left'
    )

    reseller.rename(columns={
        'name': 'resellername',
        'phonenumber': 'phone'
    }, inplace=True)

    dim_reseller = reseller

    dim_reseller = dim_reseller.drop_duplicates(subset=[
        'reselleralternatekey', 'phone', 'resellername',
        'businesstype', 'numberemployees', 'annualsales',
        'bankname', 'minpaymenttype', 'minpaymentamount',
        'annualrevenue', 'yearopened'
    ])
    dim_reseller['resellerkey'] = range(1, len(dim_reseller) + 1)

    dim_reseller = dim_reseller[[
        'resellerkey', 'geographykey', 'reselleralternatekey', 'phone', 'businesstype',
        'resellername','numberemployees', 'productline', 'addressline1','addressline2', 'annualsales',
        'bankname', 'minpaymenttype', 'minpaymentamount',
        'annualrevenue', 'yearopened'
    ]]


    return dim_reseller

