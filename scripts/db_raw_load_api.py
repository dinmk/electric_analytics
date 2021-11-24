import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values
import psycopg2
import psycopg2.extras as extras

# Retrieve Json Data from Within API

r = requests.get('https://api.vatcomply.com/rates?base=NOK')
data = r.text
df = pd.read_json(data, orient='records')
df = df.reset_index()
df = df.rename({"index" : "target_currency_key",'date': 'exchange_rate_date', 'base': 'base_currency_key', 'rates': 'exchange_rate_value'}, axis='columns')
print(df)
# Build Connection: We usally keep the connection param in env variable 
# But kept the connection paramter open here - not to disturb my env variable in my local machine

table='RAW_TR.TBL_EXCHANGE_RATE_RAW'

connection = psycopg2.connect(dbname='osftlpak', 
                                      user='osftlpak', 
                                    host='abul.db.elephantsql.com', 
                                    password='A7WCXtwaF3vz5PXm5G8C57YG3CWHn2_b',
                                    connect_timeout=1,
                                    options='-c search_path=RAW_TR'
                                    )
cur=connection.cursor()
truncate_table_exchange='''TRUNCATE TABLE RAW_TR.TBL_EXCHANGE_RATE_RAW'''
cur.execute(truncate_table_exchange)
connection.commit()
    # Create a list of tupples from the dataframe values
tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
cols = ','.join(list(df.columns))
    # SQL quert to execute
query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s)" % (table, cols)
print(query)
cur.executemany(query, tuples)
connection.commit()
cur.close()
connection.close()