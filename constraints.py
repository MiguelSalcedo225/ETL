from sqlalchemy import text

def create_constraints(engine):
    sql_statements = [

        # PRIMARY KEYS DIMENSIONES
        "ALTER TABLE dim_product ADD CONSTRAINT pk_dim_product PRIMARY KEY (productkey)",
        "ALTER TABLE dim_customer ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customerkey)",
        "ALTER TABLE dim_employee ADD CONSTRAINT pk_dim_employee PRIMARY KEY (employeekey)",
        "ALTER TABLE dim_promotion ADD CONSTRAINT pk_dim_promotion PRIMARY KEY (promotionkey)",
        "ALTER TABLE dim_currency ADD CONSTRAINT pk_dim_currency PRIMARY KEY (currencykey)",
        "ALTER TABLE dim_salesreason ADD CONSTRAINT pk_dim_salesreason PRIMARY KEY (salesreasonkey)",
        "ALTER TABLE dim_salesterritory ADD CONSTRAINT pk_dim_salesterritory PRIMARY KEY (salesterritorykey)",
        "ALTER TABLE dim_date ADD CONSTRAINT pk_dim_date PRIMARY KEY (datekey)",
        "ALTER TABLE dim_reseller ADD CONSTRAINT pk_dim_reseller PRIMARY KEY (resellerkey)",
        "ALTER TABLE dim_geography ADD CONSTRAINT pk_dim_geography PRIMARY KEY (geographykey)",

        # PRIMARY KEYS FACT TABLES
        "ALTER TABLE fact_internet_sales ADD CONSTRAINT pk_fact_internet_sales PRIMARY KEY (salesordernumber, salesorderlinenumber)",
        "ALTER TABLE fact_reseller_sales ADD CONSTRAINT pk_fact_reseller_sales PRIMARY KEY (salesordernumber, salesorderlinenumber)",

        # FOREIGN KEYS DIMENSIONES
        "ALTER TABLE dim_customer ADD CONSTRAINT fk_customer_geography FOREIGN KEY (geographykey) REFERENCES dim_geography(geographykey)",
        "ALTER TABLE dim_employee ADD CONSTRAINT fk_employee_parent FOREIGN KEY (parentemployeekey) REFERENCES dim_employee(employeekey)",
        "ALTER TABLE dim_employee ADD CONSTRAINT fk_employee_territory FOREIGN KEY (salesterritorykey) REFERENCES dim_salesterritory(salesterritorykey)",
        "ALTER TABLE dim_reseller ADD CONSTRAINT fk_reseller_geography FOREIGN KEY (geographykey) REFERENCES dim_geography(geographykey)",

        # FOREIGN KEYS FACT INTERNET SALES
        "ALTER TABLE fact_internet_sales ADD CONSTRAINT fk_internet_product FOREIGN KEY (productkey) REFERENCES dim_product(productkey)",
        "ALTER TABLE fact_internet_sales ADD CONSTRAINT fk_internet_customer FOREIGN KEY (customerkey) REFERENCES dim_customer(customerkey)",
        "ALTER TABLE fact_internet_sales ADD CONSTRAINT fk_internet_promotion FOREIGN KEY (promotionkey) REFERENCES dim_promotion(promotionkey)",
        "ALTER TABLE fact_internet_sales ADD CONSTRAINT fk_internet_currency FOREIGN KEY (currencykey) REFERENCES dim_currency(currencykey)",
        "ALTER TABLE fact_internet_sales ADD CONSTRAINT fk_internet_territory FOREIGN KEY (salesterritorykey) REFERENCES dim_salesterritory(salesterritorykey)",

        "ALTER TABLE fact_internet_sales ADD CONSTRAINT fk_internet_orderdate FOREIGN KEY (orderdatekey) REFERENCES dim_date(datekey)",
        "ALTER TABLE fact_internet_sales ADD CONSTRAINT fk_internet_duedate FOREIGN KEY (duedatekey) REFERENCES dim_date(datekey)",
        "ALTER TABLE fact_internet_sales ADD CONSTRAINT fk_internet_shipdate FOREIGN KEY (shipdatekey) REFERENCES dim_date(datekey)",

        # FOREIGN KEYS FACT RESELLER SALES
        "ALTER TABLE fact_reseller_sales ADD CONSTRAINT fk_reseller_product FOREIGN KEY (productkey) REFERENCES dim_product(productkey)",
        "ALTER TABLE fact_reseller_sales ADD CONSTRAINT fk_reseller_reseller FOREIGN KEY (resellerkey) REFERENCES dim_reseller(resellerkey)",
        "ALTER TABLE fact_reseller_sales ADD CONSTRAINT fk_reseller_employee FOREIGN KEY (employeekey) REFERENCES dim_employee(employeekey)",
        "ALTER TABLE fact_reseller_sales ADD CONSTRAINT fk_reseller_currency FOREIGN KEY (currencykey) REFERENCES dim_currency(currencykey)",
        "ALTER TABLE fact_reseller_sales ADD CONSTRAINT fk_reseller_promotion FOREIGN KEY (promotionkey) REFERENCES dim_promotion(promotionkey)",
        "ALTER TABLE fact_reseller_sales ADD CONSTRAINT fk_reseller_territory FOREIGN KEY (salesterritorykey) REFERENCES dim_salesterritory(salesterritorykey)",

        "ALTER TABLE fact_reseller_sales ADD CONSTRAINT fk_reseller_orderdate FOREIGN KEY (orderdatekey) REFERENCES dim_date(datekey)",
        "ALTER TABLE fact_reseller_sales ADD CONSTRAINT fk_reseller_duedate FOREIGN KEY (duedatekey) REFERENCES dim_date(datekey)",
        "ALTER TABLE fact_reseller_sales ADD CONSTRAINT fk_reseller_shipdate FOREIGN KEY (shipdatekey) REFERENCES dim_date(datekey)",
    ]

    with engine.connect() as conn:
        for stmt in sql_statements:
            try:
                conn.execute(text(stmt))
            except Exception as e:
                print(f"Error ejecutando: {stmt[:70]} -> {e}")
        conn.commit()

    print("Constraints creadas correctamente en PostgreSQL.")
