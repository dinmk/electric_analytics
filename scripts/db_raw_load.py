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
truncate_table_product='''TRUNCATE TABLE RAW_TR.TBL_PRODUCT_RAW'''
cur.execute(truncate_table_product)
connection.commit()
with open('/Users/dinesh.murugesan/postgres/data/Code Test - Data Engineer - Data.csv', 'r',encoding='utf-8') as tbl_product_raw:
        dr = csv.DictReader(tbl_product_raw, delimiter=',')
        to_db = [(i['market'], i['product_name'], i["parent_sku"], i["variation_sku"], i["purchase_price"], i["installation"]) for i in dr]
        cur.executemany("INSERT INTO RAW_TR.TBL_PRODUCT_RAW VALUES (%s,%s,%s,%s,%s,%s);", to_db)
        print("The Table RAW_TR.TBL_PRODUCT_RAW is populated from the input CSV file")
        connection.commit()
        cur.close()
        connection.close()