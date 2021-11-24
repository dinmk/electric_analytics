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
cur.execute('''INSERT INTO ANALYTICS_TR.TBL_PRODUCT AS TGT (
                          MARKET
                        , PRODUCT_NAME
                        , PRODUCT_PURCHASE_PRICE
                        , INSTALLATION
                        , PRODUCT_LOCAL_CURRENCY
                        , PRODUCT_IDENTIFIER)
(SELECT DISTINCT src_market
               , src_product_name
               , src_purchase_price
               , src_installation
               , src_market_currency
               , src_product_identiifer
    FROM
    (select           pdct.market AS src_market
                    , product_name AS src_product_name
                    , purchase_price AS src_purchase_price
                    , installation as src_installation
                    , market.market_currency as src_market_currency
                    , pdct.market||'-'||variation_sku as src_product_identiifer
                    from RAW_TR.TBL_PRODUCT_RAW pdct
                        left join RAW_TR.tbl_market_raw market
                     on  pdct.market = market.market
                    )  SRC )
ON CONFLICT ON CONSTRAINT PRODUCT_IDENTIFIER_uq
DO UPDATE
SET   MARKET = excluded.MARKET
    , PRODUCT_NAME = excluded.PRODUCT_NAME
    , PRODUCT_PURCHASE_PRICE = excluded.PRODUCT_PURCHASE_PRICE
    , INSTALLATION = excluded.INSTALLATION
    , PRODUCT_LOCAL_CURRENCY = excluded.PRODUCT_LOCAL_CURRENCY
    , UPDATE_BATCH_JOB_ID = excluded.UPDATE_BATCH_JOB_ID
    , UPDATE_JOB_TIMESTAMP = excluded.UPDATE_JOB_TIMESTAMP;
    ''')
print("The Table ANALYTICS_TR.TBL_PRODUCT is populated from the RAW tables")
connection.commit()
cur.close()
connection.close()