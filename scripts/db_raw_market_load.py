import psycopg2
import psycopg2.extras
import csv

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
truncate_table_market='''TRUNCATE TABLE RAW_TR.TBL_MARKET_RAW'''
cur.execute(truncate_table_market)
connection.commit()
cur.execute('''INSERT INTO RAW_TR.TBL_MARKET_RAW(
                  MARKET         
                , MARKET_NAME          
                , MARKET_CURRENCY       
                , IS_MARKET_ACTIVE       
                ) VALUES ('SE','Sweden','SEK','Y');
                ''')
print("The Table RAW_TR.TBL_MARKET_RAW is populated from the static python script")
connection.commit()

cur.execute('''INSERT INTO RAW_TR.TBL_MARKET_RAW(
                  MARKET         
                , MARKET_NAME          
                , MARKET_CURRENCY       
                , IS_MARKET_ACTIVE       
                ) VALUES ('NO','Norway','NOK','Y');
                ''')
print("The Table RAW_TR.TBL_MARKET_RAW is populated from the static python script")
connection.commit()
cur.close()
connection.close()