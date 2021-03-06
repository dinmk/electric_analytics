import psycopg2
import psycopg2.extras

# Build Connection: We usally keep the connection param in env variable 
# But kept the connection paramter open here - not to disturb my env variable in my local machine
connection = psycopg2.connect(dbname='osftlpak', 
                                      user='osftlpak', 
                                    host='abul.db.elephantsql.com', 
                                    password='A7WCXtwaF3vz5PXm5G8C57YG3CWHn2_b',
                                    connect_timeout=1,
                                    options='-c search_path=SALES_TR'
                                    )
cur=connection.cursor()


cur.execute('''CREATE OR REPLACE VIEW SALES_TR.V_PRODUCT_ANALYSIS
AS
SELECT  market
      , product_identifier
      , product_name
      , product_purchase_price
      , (product_purchase_price/exchange_rate_value) AS purchase_price_nok
      , installation AS IS_INSTALLATION_REQUIRED
FROM analytics_tr.tbl_product pdct
LEFT JOIN ANALYTICS_TR.TBL_EXCHANGE_RATE excgh
        ON (pdct.product_local_currency = target_currency_key)
WHERE excgh.is_current_active = 'TRUE';''')
connection.commit()

#Create a query returning the top-3 most expensive products
#that do not require the installation service and can be bought in Norway.

cur.execute('''
select  distinct product_name
 FROM (
select product_name
      , RANK() OVER (order by purchase_price_nok desc) rnk
from SALES_TR.V_PRODUCT_ANALYSIS
where IS_INSTALLATION_REQUIRED = 'FALSE'
and MARKET = 'NO'
  ) AS SRC
where rnk <=10
fetch first 3 rows only;
    ''')
rows = cur.fetchall()
connection.commit()

#Create a query returning the average product price in Sweden
#without taking into account Easee products.

cur.execute('''
SELECT product_name,AVG(purchase_price_nok) average_product_price_SE FROM SALES_TR.V_PRODUCT_ANALYSIS
where upper(product_name) not like 'EASEE -%'
and market = 'SE'
group by product_name;
    ''')
rows = cur.fetchall()
connection.commit()
cur.close()
connection.close()
