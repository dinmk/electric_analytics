# Author : Dinesh Murugesan (dinesh714@gmail.com)
# This Script is the master script to run the full load process
# Assumption : The input csv file is kept in /data folder
#!/usr/bin/python
import os

os.system('python /Users/dinesh.murugesan/postgres/scripts/db_script.py')  
os.system('python /Users/dinesh.murugesan/postgres/scripts/db_raw_load.py') 
os.system('python /Users/dinesh.murugesan/postgres/scripts/db_raw_load_api.py')  
os.system('python /Users/dinesh.murugesan/postgres/scripts/db_raw_market_load.py') 
os.system('python /Users/dinesh.murugesan/postgres/scripts/db_exchg_fact_load.py')
os.system('python /Users/dinesh.murugesan/postgres/scripts/db_fact_product_load.py')
os.system('python /Users/dinesh.murugesan/postgres/scripts/db_report_generation.py')
