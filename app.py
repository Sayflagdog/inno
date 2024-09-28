import logging
from clickhouse_connect import get_client
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)

client = get_client(host='clickhouse', port=8123)

logging.info(f"{datetime.now()} start")
logging.info(f"{datetime.now()} table_dataset1")
table_dataset1 = client.query('SELECT * FROM table_dataset1 LIMIT 100')
logging.info(f"{datetime.now()} table_dataset2")
table_dataset2 = client.query('SELECT * FROM table_dataset2')
logging.info(f"{datetime.now()} table_dataset3")
table_dataset3 = client.query('SELECT * FROM table_dataset3')
logging.info(f"{datetime.now()} load end")

print("ddddddddddddyyyyyyyyyyyyyyyy")



