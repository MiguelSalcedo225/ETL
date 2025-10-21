from datetime import timedelta, date, datetime
from typing import Tuple, Any
from deep_translator import GoogleTranslator
import numpy as np
import pandas as pd
from pandas import DataFrame


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
        "fulldatealternatekey": pd.date_range(start='2005-01-01', end='2008-12-31', freq='D')
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
    dim_currency = currency.sort_values('currencyalternatekey').reset_index(drop=True)
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


def transform_customer(args: DataFrame) -> DataFrame:
    (customer, person, salesorderheader, address, personphone, personemailaddress) = args
    customer.rename(columns={
        'customerid': 'customerkey',
        'accountnumber': 'customeralternatekey',
    }, inplace=True)
    customer = customer.merge(person[['businessentityid','title','firstname', 'middlename', 'lastname','namestyle', 'suffix']], left_on='personid', right_on='businessentityid', how='left')
    customer = customer.merge(personphone[['businessentityid','phonenumber']], left_on='businessentityid', right_on='businessentityid', how='left')
    customer = customer.merge(personemailaddress[['businessentityid','emailaddress']], left_on='businessentityid', right_on='businessentityid', how='left')
    customer = customer.drop(columns=['businessentityid', 'storeid', 'rowguid','modifieddate','personid'])
    salesorder_unique = salesorderheader[['customerid','billtoaddressid']].drop_duplicates('customerid')
    customer = customer.merge(
        salesorder_unique,
        left_on='customerkey', right_on='customerid', how='left'
    ).merge(
        address[['addressid','addressline1','addressline2']],
        left_on='billtoaddressid', right_on='addressid', how='left'
    )
    customer.drop(columns=['customerid', 'billtoaddressid', 'addressid','territoryid'], inplace=True)
    customer = customer[customer['firstname'].notnull()].drop_duplicates('customerkey')
    dim_customer = customer
    return dim_customer

