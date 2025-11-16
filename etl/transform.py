from datetime import timedelta, date, datetime
from typing import Tuple, Any
from deep_translator import GoogleTranslator
import numpy as np
import pandas as pd
from pandas import DataFrame
import xml.etree.ElementTree as ET


def transform_product(args) -> pd.DataFrame:
    (product, productsubcategory, productmodel, productmodelproductdescriptionculture,
     productdescription, productphoto, salesorderdetail, productproductphoto, productlistpricehistory) = args
    product.rename(columns={
        'productid': 'productkey',
        'productnumber': 'productalternatekey',
        'productsubcategoryid': 'productsubcategorykey',
        'name': 'englishproductname'
    }, inplace=True)
    product.drop(columns=['listprice'], inplace=True)
    product = product.merge(productsubcategory, left_on='productsubcategorykey',right_on='productsubcategoryid', how='left')
    productmodel = productmodel.drop(columns=['catalogdescription', 'instructions'])
    product = product.merge(productlistpricehistory[['productid', 'startdate', 'enddate', 'listprice']], left_on='productkey', right_on='productid', how='left')
    product = product.merge(productmodel[['productmodelid', 'name']].rename(columns={'name': 'modelname'}), left_on='productmodelid', right_on='productmodelid', how='left')
    product = product.merge(productproductphoto[['productid', 'productphotoid']], how='left',left_on='productkey', right_on='productid')
    product = product.merge(productphoto[['productphotoid', 'largephoto']], how='left', on='productphotoid')
    product['status'] = product['enddate'].apply(lambda x: 'Current' if pd.isna(x) else None)
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
        'productmodelid', 'productsubcategoryid',
        'productphotoid', 'name', 'productkey'
    ], inplace=True)
    dim_product = product
    dim_product['productkey'] = range(1, len(dim_product) + 1)
    dim_product = dim_product[[
        'productkey','productalternatekey', 'productsubcategorykey',
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
    dim_salesterritory = salesterritory
    dim_salesterritory['salesterritorykey'] = range(1, len(dim_salesterritory) + 1)
    dim_salesterritory = salesterritory[[
        'salesterritorykey','salesterritoryalternatekey', 'salesterritoryregion', 'salesterritorycountry',
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
    dim_salesreason['salesreasonkey'] = range(1, len(dim_salesreason) + 1)
    dim_salesreason = dim_salesreason[[
        'salesreasonkey', 'salesreasonalternatekey', 'salesreasonname', 'salesreasontype'
    ]]
    return dim_salesreason

def transform_currency(currency: DataFrame) -> DataFrame:
    currency = currency.copy()
    currency.drop(columns = ['modifieddate'], inplace=True)
    currency.rename(columns = {
        'currencycode': 'currencyalternatekey',
        'name': 'currencyname'
    }, inplace=True)
    currency['currencyname'] = currency['currencyname'].str.lower().str.replace(' ', '')
    dim_currency = currency
    dim_currency = currency.sort_values('currencyname').reset_index(drop=True)
    dim_currency['currencykey'] = range(1, len(dim_currency) + 1)
    dim_currency = dim_currency[[
        'currencykey', 'currencyalternatekey', 'currencyname'
    ]]
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
    dim_promotion['promotionkey'] = range(1, len(dim_promotion) + 1)
    dim_promotion = dim_promotion[[
        'promotionkey', 'promotionalternatekey', 'promotiondescription',
        'promotiondiscountpct','promotiontype', 'promotioncategory',
        'startdate', 'enddate'
    ]]
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
    dim_employee = dim_employee.merge(personemailaddress[['businessentityid', 'emailaddress']], on = 'businessentityid', how='left')
    personphone_sorted = (
        personphone
        .sort_values(by=["businessentityid", "phonenumbertypeid"])  )
    dim_employee = dim_employee.merge(
        personphone_sorted[['businessentityid', 'phonenumber']],
        on='businessentityid',
        how='left'
    )
    pay_last = (
        employeepayhistory
        .sort_values(by=["businessentityid", "ratechangedate"], ascending=[True, False])
        .drop_duplicates(subset=['businessentityid'], keep='first'))
    dim_employee = dim_employee.merge(
        pay_last[['businessentityid', 'payfrequency', 'rate']],
        on='businessentityid',
        how='left'
    )

    dept_hist = (
        employeedepartmenthistory
        .sort_values(by=["businessentityid", "startdate"], ascending=[True, False]))
        
    
    dim_employee = dim_employee.merge(
        dept_hist[['businessentityid', 'startdate', 'enddate', 'departmentid']],
        on='businessentityid',
        how='left'
    )
    dim_employee = dim_employee.merge(department[['departmentid', 'name']], on = 'departmentid')
    dim_employee['salespersonflag'] = np.where(
        dim_employee['name'] == 'Sales',
        True,
        False
    )
    dim_employee = dim_employee.merge(sales[['businessentityid', 'territoryid']], on = 'businessentityid', how='left')

    dim_employee.rename(columns={
                                 'nationalidnumber' : 'employeenationalidalternatekey',
                                 'jobtitle' : 'title',
                                 'phonenumber': 'phone',
                                 'rate': 'baserate',
                                 'name': 'departmentname',
                                 'territoryid' : 'salesterritorykey'                             
                                 }, 
                        inplace=True)
    
    dim_employee.drop('departmentid', axis=1, inplace=True)
    
    
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

    dim_employee['employeekey'] = range(1, len(dim_employee) + 1)

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
     person, stateprovince, countryregion, personphone, businessentitycontact, salesorderheader) = args  

 
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
    
    business_type_transform = {
        'BM': 'Warehouse',
        'OS': 'Value Added Reseller',
        'BS': 'Specialty Bike Shop'
    }


    reseller = customer[
        customer['storeid'].notnull() & customer['personid'].isnull()
    ].copy()

    reseller.drop(columns=['rowguid', 'modifieddate'], inplace=True)
    reseller.rename(columns={'accountnumber': 'reselleralternatekey'}, inplace=True)

    reseller = reseller.merge(
        store[['businessentityid', 'name', 'demographics']],
        left_on='storeid', right_on='businessentityid', how='left'
    )


    demo_data = reseller['demographics'].apply(extract_demographics).apply(pd.Series)
    reseller = pd.concat([reseller, demo_data], axis=1)

    reseller['businesstype'] = reseller['businesstype'].map(business_type_transform).fillna(reseller['businesstype'])

    orders = customer[['customerid', 'storeid']].merge(
        salesorderheader[['customerid', 'orderdate']], on='customerid', how='left'
    )

    orders['year'] = orders['orderdate'].dt.year
    orders['month'] = orders['orderdate'].dt.month

    reseller_info = orders.groupby('storeid').agg(
        first_order_year=('year', 'min'),
        last_order_year=('year', 'max'),
        order_month=('month', 'max'),
    ).reset_index()

    reseller_info['order_frequency'] = "0"

    reseller = reseller.merge(
        reseller_info,
        left_on='storeid',
        right_on='storeid',
        how='left'
    )


    ranked_contacts = (
        businessentitycontact[['businessentityid', 'personid', 'contacttypeid']]
        .sort_values(['businessentityid', 'contacttypeid'])
        .assign(rn=lambda x: x.groupby('businessentityid').cumcount() + 1)
    )

    ranked_contacts = ranked_contacts[ranked_contacts['rn'] == 1]

    ranked_contacts = ranked_contacts.merge(
        person[['businessentityid']], left_on='personid', right_on='businessentityid', how='left'
    )

    ranked_contacts = ranked_contacts.merge(
        personphone[['businessentityid', 'phonenumber']],
        left_on='businessentityid_y',
        right_on='businessentityid',
        how='left'
    )

    ranked_contacts = ranked_contacts[['businessentityid_x', 'phonenumber']]
    ranked_contacts.rename(columns={'businessentityid_x': 'businessentityid'}, inplace=True)


    reseller = reseller.merge(ranked_contacts, on='businessentityid', how='left')

    reseller = reseller.merge(
        businessentityaddress[['businessentityid', 'addressid']], 
        on='businessentityid', how='left'
    )

    reseller = reseller.merge(
        address[['addressid', 'city', 'postalcode', 'stateprovinceid', 'addressline1','addressline2']], 
        on='addressid', how='left'
    )

    reseller = reseller.merge(
        stateprovince[['stateprovinceid', 'stateprovincecode', 'countryregioncode']],
        on='stateprovinceid', how='left'
    )

    reseller = reseller.merge(
        countryregion[['countryregioncode', 'name']].rename(columns={'name': 'countryregionname'}),
        on='countryregioncode', how='left'
    )

    reseller = reseller.merge(
        dim_geography[['geographykey', 'city', 'stateprovincecode', 'countryregioncode', 'postalcode']],
        on=['city', 'stateprovincecode', 'countryregioncode', 'postalcode'],
        how='left'
    )

    reseller.rename(columns={
        'name': 'resellername',
        'phonenumber': 'phone'
    }, inplace=True)

    dim_reseller = reseller.drop_duplicates(subset=[
        'reselleralternatekey', 'phone', 'resellername',
        'businesstype', 'numberemployees', 'annualsales',
        'bankname', 'minpaymenttype', 'minpaymentamount',
        'annualrevenue', 'yearopened'
    ])

    dim_reseller['resellerkey'] = range(1, len(dim_reseller) + 1)

    dim_reseller = dim_reseller[[
        'resellerkey', 'geographykey', 'reselleralternatekey', 'phone', 'businesstype',
        'resellername','numberemployees', 'order_frequency', 'order_month', 'first_order_year', 'last_order_year', 
        'productline', 'addressline1','addressline2', 'annualsales',
        'bankname', 'minpaymenttype', 'minpaymentamount',
        'annualrevenue', 'yearopened'
    ]]

    return dim_reseller

def transform_fact_internet_sales(args: DataFrame ,  dim_product: DataFrame,
                                dim_customer: DataFrame,
                                dim_promotion: DataFrame,
                                dim_salesterritory: DataFrame,
                                dim_currency: DataFrame,
                                dim_salesreason: DataFrame,
                                dim_date: DataFrame) -> DataFrame:
    (salesorderheader,salesorderdetail,currencyrate,product,customer) = args

    fact = salesorderdetail.merge(
        salesorderheader,
        on="salesorderid",
        how="inner"
    )
    dim_product = dim_product.drop_duplicates(subset=['productalternatekey'])

    fact = fact[fact["onlineorderflag"] == True]

    fact = fact.merge(
        product[["productid", "productnumber", "standardcost"]],
        on="productid",
        how="inner"
    )

    fact=fact.merge(
    dim_product[['productkey','productalternatekey']],
    left_on='productnumber',
    right_on='productalternatekey',
    how='left'
    
    )

    fact = fact.merge(
        customer[["customerid", "accountnumber"]],
        on="customerid",
        how="inner"
    )

    fact.rename(columns={
        "accountnumber_y": "accountnumber"
    }, inplace=True)

    fact.drop(columns=["accountnumber_x"], inplace=True)
    
    fact = fact.merge(
        currencyrate[["currencyrateid", "tocurrencycode"]],
        on="currencyrateid",
        how="left"
    )

    fact["tocurrencycode"] = fact["tocurrencycode"].fillna("USD")

    fact=fact.merge(
        dim_currency[['currencykey','currencyalternatekey']],
        left_on='tocurrencycode',
        right_on='currencyalternatekey',
        how='left'
    )

    fact=fact.merge(
    dim_promotion[['promotionalternatekey','promotionkey']],
    left_on='specialofferid',
    right_on='promotionalternatekey',
    how='left')

    fact = fact.merge(
        dim_salesterritory[['salesterritorykey', 'salesterritoryalternatekey']],
        left_on='territoryid',
        right_on='salesterritoryalternatekey',
        how='left'
    )

    fact=fact.merge(
        dim_customer[['customerkey','customeralternatekey']],
        left_on='accountnumber',
        right_on='customeralternatekey',
        how='left' 
    )

    fact["salesorderlinenumber"] = (fact.sort_values("salesorderdetailid").groupby("salesordernumber").cumcount() + 1)

    fact["orderquantity"] = fact["orderqty"]
    fact["unitpricediscountpct"] = fact["unitpricediscount"]
    fact["extendedamount"] = fact["unitprice"] * fact["orderqty"]
    fact["discountamount"] = fact["unitprice"] * fact["unitpricediscount"] * fact["orderqty"]
    fact["productstandardcost"] = fact["standardcost"]
    fact["totalproductcost"] = fact["standardcost"] * fact["orderqty"]
    fact["salesamount"] = fact["unitprice"] * (1 - fact["unitpricediscount"]) * fact["orderqty"]

    fact['orderdatekey'] = fact['orderdate'].dt.strftime('%Y%m%d').astype(int)
    fact['duedatekey'] = fact['duedate'].dt.strftime('%Y%m%d').astype(int)
    fact['shipdatekey'] = fact['shipdate'].dt.strftime('%Y%m%d').astype(int)


    fact = fact.merge(
        dim_date[["datekey"]],
        left_on="orderdatekey",
        right_on="datekey",
        how="left"
    )

    fact = fact.merge(
        dim_date[["datekey"]],
        left_on="duedatekey",
        right_on="datekey",
        how="left"
    )

    fact = fact.merge(
        dim_date[["datekey"]],
        left_on="shipdatekey",
        right_on="datekey",
        how="left"
    )

    fact = fact[[
        'productkey',
        'orderdatekey',
        'duedatekey',
        'shipdatekey',
        'customerkey',
        'promotionkey',
        'currencykey',
        'salesterritorykey',
        'salesordernumber',
        "salesorderlinenumber",
        "revisionnumber",
        "orderquantity",
        "unitprice",
        "extendedamount",
        "unitpricediscountpct",
        "discountamount",
        "productstandardcost",
        "totalproductcost",
        "salesamount",
        "taxamt",
        "freight",
        "carriertrackingnumber",
        "purchaseordernumber",
        "orderdate",
        "duedate",
        "shipdate"
    ]]

    return fact

def transform_fact_internet_sales_reason(args: DataFrame,
                                         dim_salesreason: DataFrame) -> DataFrame:
    (salesorderheadersalesreason, salesorderheader, salesorderdetail) = args

    detail = salesorderdetail[['salesorderid', 'salesorderdetailid']].copy()
    detail = detail.sort_values(['salesorderid', 'salesorderdetailid'])
    detail['salesorderlinenumber'] = detail.groupby('salesorderid').cumcount() + 1

    fact = salesorderheadersalesreason.merge(
        salesorderheader[['salesorderid', 'salesordernumber']],
        on='salesorderid',
        how='left'
    )

    fact = fact.merge(
        detail[['salesorderid', 'salesorderlinenumber']],
        on='salesorderid',
        how='left'
    )

    fact = fact[['salesordernumber', 'salesorderlinenumber', 'salesreasonid']]

    return fact

def transform_fact_reseller_sales(
    args: DataFrame,
    dim_product: DataFrame,
    dim_reseller: DataFrame,
    dim_employee: DataFrame,
    dim_currency: DataFrame,
    dim_promotion: DataFrame,
    dim_salesterritory: DataFrame,
    dim_date: DataFrame
) -> DataFrame:

    (salesorderheader, salesorderdetail, currencyrate,
     product, customer, store, employee) = args

    employee_mapping = employee[[
        "businessentityid", "nationalidnumber"
    ]].rename(columns={
        "businessentityid": "sales_person_id",
        "nationalidnumber": "employee_national_id"
    })

    reseller_account_mapping = (
        customer[customer["storeid"].notna()]
        .groupby("storeid")["accountnumber"]
        .min()
        .reset_index()
        .rename(columns={
            "storeid": "reseller_id",
            "accountnumber": "reseller_account_number"
        })
    )

    salesorderheader.rename(columns={
        "purchaseordernumber": "customerponumber"
    }, inplace=True)
    fact = salesorderdetail.merge(
        salesorderheader,
        on="salesorderid",
        how="inner"
    )


    fact=fact.merge(
        dim_promotion[['promotionalternatekey','promotionkey']],
        left_on='specialofferid',
        right_on='promotionalternatekey',
        how='left'
    )

    fact = fact[(fact["onlineorderflag"] == False) &
                (fact["salespersonid"].notna())]

    fact = fact.merge(
        product[["productid", "productnumber", "standardcost"]],
        on="productid",
        how="inner"
    )

    fact = fact.merge(
        customer[["customerid", "storeid"]],
        on="customerid",
        how="inner"
    )

    fact = fact.merge(
        store[["businessentityid"]],
        left_on="storeid",
        right_on="businessentityid",
        how="inner"
    ).rename(columns={"businessentityid": "reseller_id"})

    fact = fact.merge(
        reseller_account_mapping,
        on="reseller_id",
        how="left"
    )

    fact = fact.merge(
        employee_mapping,
        left_on="salespersonid",
        right_on="sales_person_id",
        how="left"
    )

    fact["salesorderlinenumber"] = (fact.sort_values("salesorderdetailid").groupby("salesordernumber").cumcount() + 1)


    fact["orderquantity"] = fact["orderqty"]
    fact["extendedamount"] = fact["unitprice"] * fact["orderqty"]
    fact["unitpricediscountpct"] = fact["unitpricediscount"]
    fact["discountamount"] = fact["unitprice"] * fact["unitpricediscount"] * fact["orderqty"]
    fact["productstandardcost"] = fact["standardcost"]
    fact["totalproductcost"] = fact["standardcost"] * fact["orderqty"]
    fact["salesamount"] = fact["unitprice"] * (1 - fact["unitpricediscount"]) * fact["orderqty"]

    fact["orderdatekey"] = fact["orderdate"].dt.strftime("%Y%m%d").astype(int)
    fact["duedatekey"] = fact["duedate"].dt.strftime("%Y%m%d").astype(int)
    fact["shipdatekey"] = fact["shipdate"].dt.strftime("%Y%m%d").astype(int)

    fact = fact.merge(
        dim_product[["productkey", "productalternatekey", 'startdate', 'enddate']],
        left_on="productnumber",
        right_on="productalternatekey",
        how="left"
    )

    fact = fact.merge(
        dim_reseller[["resellerkey", "reselleralternatekey"]],
        left_on="reseller_account_number",
        right_on="reselleralternatekey",
        how="left"
    )

    fact = fact.merge(
        dim_employee[["employeekey", 'employeenationalidalternatekey']],
        left_on="employee_national_id",
        right_on='employeenationalidalternatekey',
        how="left"
    )

    fact = fact.merge(
        currencyrate[["currencyrateid", "tocurrencycode"]],
        on="currencyrateid",
        how="left"
    )
    fact["tocurrencycode"] = fact["tocurrencycode"].fillna("USD")

    fact = fact.merge(
        dim_currency[["currencykey", "currencyalternatekey"]],
        left_on="tocurrencycode",
        right_on="currencyalternatekey",
        how="left"
    )

    fact = fact.merge(
        dim_salesterritory[["salesterritorykey", "salesterritoryalternatekey"]],
        left_on="territoryid",
        right_on="salesterritoryalternatekey",
        how="left"
    )

    for key in ["orderdatekey", "duedatekey", "shipdatekey"]:
        fact = fact.merge(
            dim_date[["datekey"]],
            left_on=key,
            right_on="datekey",
            how="left"
        )
    
    merged_df = fact.copy()

    condicion_historica = (
        (merged_df['orderdate'] >= merged_df['startdate']) &
        (merged_df['orderdate'] <= merged_df['enddate'])
    )

    condicion_actual = (
        (merged_df['orderdate'] >= merged_df['startdate']) &
        (merged_df['enddate'].isnull())
    )

    fact = merged_df[condicion_historica | condicion_actual].copy()

    fact = fact[[
        "productkey",
        "orderdatekey",
        "duedatekey",
        "shipdatekey",
        "resellerkey",
        "employeekey",
        "promotionkey",
        "currencykey",
        "salesterritorykey",
        "salesordernumber",
        "salesorderlinenumber",
        "revisionnumber",
        "orderquantity",
        "unitprice",
        "extendedamount",
        "unitpricediscountpct",
        "discountamount",
        "productstandardcost",
        "totalproductcost",
        "salesamount",
        "taxamt",
        "freight",
        "carriertrackingnumber",
        "customerponumber",
        "orderdate",
        "duedate",
        "shipdate"
    ]]

    return fact

