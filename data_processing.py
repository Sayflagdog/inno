from clickhouse_connect import get_client
import pandas as pd
from datetime import datetime

DF1_COLS = ['uid','full_name','email','address','sex','birthdate','phone']
DF2_COLS = ['uid','first_name','middle_name','last_name','birthdate','phone','address']
DF3_COLS = ['uid','name','email','birthdate','sex']

client = get_client(host='clickhouse', port=8123)



def load_table(table_name, columns, df_name="df"):
    print(f"{datetime.now()} {df_name}")
    offset = 0
    limit = 1000000
    while True:
        table_dataset = client.query(f'SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}')
        result_rows = table_dataset.result_rows
        if not result_rows:
            break 
        df = pd.DataFrame(result_rows, columns=columns)
        offset += limit 
        print(offset)

    return df



def write_result(df):
    client = get_client(host='clickhouse', port=8123)
    
    create_table_query = '''
    CREATE OR REPLACE TABLE table_results
    (
        id_is1 Array(UUID),
        id_is2 Array(UUID),
        id_is3 Array(UUID)
    )
    ENGINE = MergeTree()
    ORDER BY id_is1;
    '''
    
    client.command(create_table_query)
    
    insert_query = '''
    INSERT INTO table_results (id_is1, id_is2, id_is3) VALUES
    '''
    
    values = []
    for index, row in df.iterrows():
        values.append(f"({row['id_is1']}, {row['id_is2']}, {row['id_is3']})")
    insert_query += ', '.join(values) + ';'
    client.command(insert_query)
    
    print("Данные успешно записаны в таблицу table_results")


def clear_table(table_name='table_results'):
    client = get_client(host='clickhouse', port=8123)
    clear_query = f'TRUNCATE TABLE {table_name};'
    client.command(clear_query)
    print(f"Таблица {table_name} очищена")


print(f"{datetime.now()} df start")
df1 = load_table('table_dataset1', DF1_COLS, 'df1')
print(df1.head())
# df2 = load_table('table_dataset2', DF2_COLS, 'df2')
# df3 = load_table('table_dataset3', DF3_COLS, 'df3')
# print(f"{datetime.now()} df end")
# print(df3.head())
# write_result(df3[['uid', 'name', 'email']])
# df_result = load_table('table_results', ['uid', 'name', 'email'], 'table_results')
# print(df_result.head())
# clear_table()
# print(df1.info())
# print(df1['sex'].unique())
print(f"{datetime.now()} df end")




"""
print(f"{datetime.now()} table_dataset3")
table_dataset3 = client.query('SELECT * FROM table_dataset3')
print(f"{datetime.now()} load end")

print(f"{datetime.now()} df start")

print(f"{datetime.now()} df1")
columns = ['uid','full_name','email','address','sex','birthdate','phone']
df1 = pd.DataFrame(table_dataset1.result_rows, columns=columns)

"""


