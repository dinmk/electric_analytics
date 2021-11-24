#!/usr/bin/python
import psycopg2
import psycopg2.extras


# Build Connection: We usally keep the connection param in env variable 
# But kept the connection paramter open here - not to disturb my env variable in my local machine
connection = psycopg2.connect(dbname='osftlpak', 
                                      user='osftlpak', 
                                    host='abul.db.elephantsql.com', 
                                    password='A7WCXtwaF3vz5PXm5G8C57YG3CWHn2_b',
                                    connect_timeout=1,
                                    options='-c search_path=RAW_TR'
                                    )
cur=connection.cursor()
create_schema_raw='''CREATE SCHEMA IF NOT EXISTS RAW_TR'''
create_schema_analytics='''CREATE SCHEMA IF NOT EXISTS ANALYTICS_TR'''
create_schema_datamart='''CREATE SCHEMA IF NOT EXISTS SALES_TR'''
create_table_product='''CREATE TABLE IF NOT EXISTS RAW_TR.TBL_PRODUCT_RAW
                    (
                          MARKET               VARCHAR(5)
                        , PRODUCT_NAME         VARCHAR(250)
                        , PARENT_SKU           VARCHAR(50)
                        , VARIATION_SKU        VARCHAR(50)
                        , PURCHASE_PRICE       NUMERIC(38,10)
                        , INSTALLATION         BOOLEAN
                        , BATCH_JOB_ID         VARCHAR(25) NOT NULL DEFAULT 'Apache Airflow'
                        , BATCH_JOB_TIMESTAMP  TIMESTAMP NOT NULL DEFAULT NOW()
                    )'''
create_table_exhange='''CREATE TABLE IF NOT EXISTS RAW_TR.TBL_EXCHANGE_RATE_RAW
                    (
                          TARGET_CURRENCY_KEY  VARCHAR(5)
                        , EXCHANGE_RATE_DATE   DATE
                        , BASE_CURRENCY_KEY    VARCHAR(5)
                        , EXCHANGE_RATE_VALUE  NUMERIC(38,10)
                        , BATCH_JOB_ID         VARCHAR(25) NOT NULL DEFAULT 'Apache Airflow'
                        , BATCH_JOB_TIMESTAMP  TIMESTAMP NOT NULL DEFAULT NOW()
                    )'''
create_table_market='''CREATE TABLE IF NOT EXISTS RAW_TR.TBL_MARKET_RAW
                    (
                          MARKET                  VARCHAR(20)
                        , MARKET_NAME             VARCHAR(50)
                        , MARKET_CURRENCY         VARCHAR(50)
                        , IS_MARKET_ACTIVE        VARCHAR(50)
                        , BATCH_JOB_ID            VARCHAR(25) NOT NULL DEFAULT 'Apache Airflow'
                        , BATCH_JOB_TIMESTAMP     TIMESTAMP NOT NULL DEFAULT NOW()
                    )'''

create_table_fpdct='''CREATE TABLE IF NOT EXISTS ANALYTICS_TR.TBL_PRODUCT
                    (
                          MARKET                        VARCHAR(20)
                        , PRODUCT_IDENTIFIER            VARCHAR(50)    PRIMARY KEY
                        , PRODUCT_NAME                  VARCHAR(250)   NOT NULL
                        , PRODUCT_PURCHASE_PRICE        NUMERIC(38,10) NOT NULL
                        , INSTALLATION                  BOOLEAN        NOT NULL
                        , PRODUCT_LOCAL_CURRENCY        VARCHAR(50)    NOT NULL
                        , INSERT_BATCH_JOB_ID           VARCHAR(25) NOT NULL DEFAULT 'Apache Airflow'
                        , INSERT_JOB_TIMESTAMP          TIMESTAMP NOT NULL DEFAULT NOW()
                        , UPDATE_BATCH_JOB_ID           VARCHAR(25)
                        , UPDATE_JOB_TIMESTAMP          TIMESTAMP 
                        , CONSTRAINT PRODUCT_IDENTIFIER_uq UNIQUE (PRODUCT_IDENTIFIER)
                    )'''

create_table_fxchg='''CREATE TABLE IF NOT EXISTS ANALYTICS_TR.TBL_EXCHANGE_RATE
                    (
                          TARGET_CURRENCY_KEY           VARCHAR(5)
                        , EXCHANGE_RATE_DATE            DATE
                        , BASE_CURRENCY_KEY             VARCHAR(5)
                        , EXCHANGE_RATE_VALUE           NUMERIC(38,10)
                        , IS_CURRENT_ACTIVE             BOOLEAN
                        , INSERT_BATCH_JOB_ID           VARCHAR(25) NOT NULL DEFAULT 'Apache Airflow'
                        , INSERT_JOB_TIMESTAMP          TIMESTAMP NOT NULL DEFAULT NOW()
                        , UPDATE_BATCH_JOB_ID           VARCHAR(25)
                        , UPDATE_JOB_TIMESTAMP          TIMESTAMP
                        , CONSTRAINT EXCHG_uq UNIQUE (TARGET_CURRENCY_KEY,EXCHANGE_RATE_DATE) 
                    )'''
cur.execute(create_schema_raw)
print("Schema RAW_TR created successfully.....")
connection.commit()
cur.execute(create_schema_analytics)
print("Schema ANALYTICS_TR created successfully.....")
connection.commit()
cur.execute(create_schema_datamart)
print("Schema DATAMART_TR created successfully.....")
connection.commit()
cur.execute(create_table_product)
connection.commit()
print("Table RAW_TR.TBL_PRODUCT_RAW created successfully.....")
cur.execute(create_table_exhange)
connection.commit()
print("Table RAW_TR.TBL_EXCHANGE_RATE_RAW created successfully.....")
cur.execute(create_table_market)
connection.commit()
print("Table RAW_TR.TBL_MARKET_RAW created successfully.....")
cur.execute(create_table_fpdct)
connection.commit()
print("Table ANALYTICS_TR.TBL_PRODUCT created successfully.....")
cur.execute(create_table_fxchg)
connection.commit()
print("Table ANALYTICS_TR.TBL_EXCHANGE_RATE created successfully.....")
cur.close()
connection.close()
