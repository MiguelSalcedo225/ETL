from datetime import timedelta, date, datetime
from typing import Tuple, Any
from deep_translator import GoogleTranslator
import numpy as np
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
from pandas import DataFrame


def transform_product(args) -> pd.DataFrame:
    (product,productsubcategory,productmodel,productmodelproductdescriptionculture,productdescription,productphoto,culture,productproductphoto) = args
    product.rename(columns={
        'productid': 'productkey',
        'productnumber': 'productalternatekey',
        'productsubcategoryid': 'productsubcategorykey',
        'name': 'englishproductname',
        'sellstartdate': 'startdate',
        'sellenddate': 'enddate'
    }, inplace=True)
    product = product.merge(productsubcategory, left_on='productsubcategorykey', right_on='productsubcategoryid', how='left')
    product = product.merge(productmodel[['productmodelid', 'name']].rename(columns={'name': 'modelname'}),left_on='productmodelid',right_on='productmodelid',how='left')
    product = product.merge(productproductphoto[['productid', 'productphotoid']], how='left', left_on='productkey', right_on='productid')
    product = product.merge(productphoto[['productphotoid', 'largephoto']],how='left', on='productphotoid')
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
        .merge(culture, on='cultureid', how='left')  
    )
    desc = (
        productmodelproductdescriptionculture
        .merge(productdescription, on='productdescriptionid', how='left')
        .drop_duplicates(subset=['productmodelid'])
        [['productmodelid', 'description']]
    )

    product = product.merge(desc, on='productmodelid', how='left')
    idiomas = {
        'englishdescription': 'en','frenchdescription': 'fr','chinesedescription': 'zh-CN',
        'arabicdescription': 'ar','hebrewdescription': 'he','thaidescription': 'th',
        'germandescription': 'de',   'japanesedescription': 'ja','turkishdescription': 'tr' 
    }
    traduccion_cache = {}
    def traducir_texto(texto, target, source='auto'):
        if pd.isna(texto) or texto.strip() == "":
            return ""
        clave = (texto, target)
        if clave in traduccion_cache:
            return traduccion_cache[clave]
        try:
            traducido = GoogleTranslator(source=source, target=target).translate(texto)
            traduccion_cache[clave] = traducido
            return traducido
        except Exception:
            traduccion_cache[clave] = ""
            return ""
    descripciones_unicas = product['description'].dropna().unique()
    detecciones = {}
    for texto in descripciones_unicas:
        try:
            lang_detectado = GoogleTranslator(source='auto', target='en').detect(texto)
            detecciones[texto] = lang_detectado
        except Exception:
            detecciones[texto] = 'auto'

    # Aplicar traducción por idioma
    for col, lang in idiomas.items():
        product[col] = product['description'].apply(lambda x: traducir_texto(x, lang, source=detecciones.get(x, 'auto')))

    idiomasname = {
        'english': 'en',
        'french': 'fr',
        'spanish': 'es'
    }
    for prefix, lang in idiomasname.items():
        product[f"{prefix}productname"] = product['englishproductname'].apply(lambda x: traducir_texto(x, lang, source='en'))
    product.drop(
        columns=['rowguid_x','rowguid_y','modifieddate_y','modifieddate_x'], inplace=True)
    product.drop(
        columns=['productmodelid','productsubcategoryid','productid','productphotoid','name','productkey'], inplace=True)
    dim_product = product[[
        'productalternatekey', 'productsubcategorykey',
        'weightunitmeasurecode', 'sizeunitmeasurecode',
        'englishproductname', 'spanishproductname', 'frenchproductname','standardcost', 'finishedgoodsflag', 'color', 'safetystocklevel',
        'reorderpoint', 'listprice', 'size', 'sizerange', 'weight',
        'daystomanufacture', 'productline', 'class','style', 'modelname', 'largephoto',
        'englishdescription', 'frenchdescription','chinesedescription', 'arabicdescription', 'hebrewdescription', 
        'thaidescription', 'germandescription', 'japanesedescription', 'turkishdescription','startdate', 'enddate', 'status', 'saved'
    ]]
    dim_product.reset_index(drop=True, inplace=True)
    return dim_product

def transform_fecha() -> DataFrame:
    dimdate = pd.DataFrame({
        "fulldatealternatekey": pd.date_range(start='2005-01-01', end='2008-12-31', freq='D')
    })

 
    dimdate["datekey"] = dimdate["fulldatealternatekey"].dt.strftime("%Y%m%d").astype(int)


    dimdate["daynumberofweek"] = dimdate["fulldatealternatekey"].dt.weekday + 1  # Lunes=1
    dimdate["daynumberofmonth"] = dimdate["fulldatealternatekey"].dt.day
    dimdate["daynumberofyear"] = dimdate["fulldatealternatekey"].dt.day_of_year
    dimdate["weeknumberofyear"] = dimdate["fulldatealternatekey"].dt.isocalendar().week.astype(int)
    dimdate["monthnumberofyear"] = dimdate["fulldatealternatekey"].dt.month
    dimdate["calendarquarter"] = dimdate["fulldatealternatekey"].dt.quarter
    dimdate["calendaryear"] = dimdate["fulldatealternatekey"].dt.year
    dimdate["calendarsemester"] = dimdate["monthnumberofyear"].apply(lambda m: 1 if m <= 6 else 2)


    dimdate["fiscalyear"] = dimdate["fulldatealternatekey"].apply(
        lambda x: x.year if x.month >= 7 else x.year - 1
    )
    dimdate["fiscalquarter"] = dimdate["fulldatealternatekey"].apply(
        lambda x: ((x.month - 7) % 12) // 3 + 1
    )
    dimdate["fiscalsemester"] = dimdate["fiscalquarter"].apply(lambda q: 1 if q <= 2 else 2)

    dimdate["englishdaynameofweek"] = dimdate["fulldatealternatekey"].dt.day_name()
    dimdate["englishmonthname"] = dimdate["fulldatealternatekey"].dt.month_name()

    def traducir(texto, idioma):
        try:
            return GoogleTranslator(source='en', target=idioma).translate(texto)
        except Exception:
            return texto

    dimdate["spanishdaynameofweek"] = dimdate["englishdaynameofweek"].apply(lambda x: traducir(x, "es"))
    dimdate["frenchdaynameofweek"] = dimdate["englishdaynameofweek"].apply(lambda x: traducir(x, "fr"))

    dimdate["spanishmonthname"] = dimdate["englishmonthname"].apply(lambda x: traducir(x, "es"))
    dimdate["frenchmonthname"] = dimdate["englishmonthname"].apply(lambda x: traducir(x, "fr"))

    dimdate = dimdate[[
        "datekey", "fulldatealternatekey", "daynumberofweek",
        "englishdaynameofweek", "spanishdaynameofweek", "frenchdaynameofweek",
        "daynumberofmonth", "daynumberofyear", "weeknumberofyear",
        "englishmonthname", "spanishmonthname", "frenchmonthname",
        "monthnumberofyear", "calendarquarter", "calendaryear", "calendarsemester",
        "fiscalquarter", "fiscalyear", "fiscalsemester"
    ]]

    return dimdate

