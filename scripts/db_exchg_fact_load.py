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
cur.execute('''INSERT INTO ANALYTICS_TR.TBL_EXCHANGE_RATE AS TGT
(  TARGET_CURRENCY_KEY
 , EXCHANGE_RATE_DATE
 , BASE_CURRENCY_KEY
 , EXCHANGE_RATE_VALUE
 , IS_CURRENT_ACTIVE)

select TARGET_CURRENCY_KEY
 , EXCHANGE_RATE_DATE
 , BASE_CURRENCY_KEY
 , EXCHANGE_RATE_VALUE
 , 'Y' AS IS_CURRENT_ACTIVE
from RAW_TR.TBL_EXCHANGE_RATE_RAW
ON CONFLICT  (TARGET_CURRENCY_KEY,EXCHANGE_RATE_DATE)
DO UPDATE
SET   BASE_CURRENCY_KEY = excluded.BASE_CURRENCY_KEY
    , EXCHANGE_RATE_VALUE = excluded.EXCHANGE_RATE_VALUE
    , UPDATE_BATCH_JOB_ID = excluded.UPDATE_BATCH_JOB_ID
    , UPDATE_JOB_TIMESTAMP = excluded.UPDATE_JOB_TIMESTAMP;
    ''')
print("The Table ANALYTICS_TR.TBL_EXCHANGE_RATE is populated from the RAW tables")
connection.commit()
cur.execute('''
WITH tbl_exchg AS (
    SELECT MAX(EXCHANGE_RATE_DATE) max_date
    FROM ANALYTICS_TR.TBL_EXCHANGE_RATE
    )
UPDATE ANALYTICS_TR.TBL_EXCHANGE_RATE
SET IS_CURRENT_ACTIVE ='FALSE'
    , UPDATE_BATCH_JOB_ID = 'Apache Airflow'
    , UPDATE_JOB_TIMESTAMP = NOW()    
FROM tbl_exchg
WHERE tbl_exchg.max_date > EXCHANGE_RATE_DATE;''')
connection.commit()
cur.close()
connection.close()
