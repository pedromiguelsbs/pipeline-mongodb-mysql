import os
import mysql.connector
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

def connect_mysql(host, user, password):
    connection = mysql.connector.connect(host=host, user=user,password=password)
    return connection

def create_cursor(connection):
    cursor = connection.cursor()
    return cursor

def create_database(cursor, db_name):
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name};')

def show_databases(cursor):
    cursor.execute('SHOW DATABASES;')
    for db in cursor:
        print(db)

def create_table(cursor, db_name, tb_name):
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{tb_name}(
               id VARCHAR(100),
               Produto VARCHAR(100),
               Categoria_Produto VARCHAR(100),
               Preco FLOAT(10,2),
               Frete FLOAT(10,2),
               Data_Compra DATE,
               Vendedor VARCHAR(100),
               Local_Compra VARCHAR(100),
               Avaliacao_Compra INT,
               Tipo_Pagamento VARCHAR(100),
               Qntd_Parcelas INT,
               Latitude FLOAT(10,2),
               Longitude FLOAT(10,2),
               PRIMARY KEY (id)
    );
""")

def show_tables(cursor, db_name):
    cursor.execute(f'USE {db_name};')
    cursor.execute('SHOW TABLES;')
    for tb in cursor:
        print(tb)

def read_csv(path):
    df = pd.read_csv(path)
    return df

def add_data(connection, cursor, df, db_name, tb_name):
    data_list = [tuple(row) for i, row in df.iterrows()]
    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    cursor.executemany(sql, data_list)
    connection.commit()

host = os.getenv("DB_HOST")
user = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

connection = connect_mysql(host, user, password)
cursor = create_cursor(connection)
create_database(cursor, "db_produtos")
show_databases(cursor)
create_table(cursor, "db_produtos", "tb_livros")
create_table(cursor, "db_produtos", "tb_produtos2021")
show_tables(cursor, "db_produtos")
df_livros = read_csv("/home/pedro/documents/pipeline-mongodb-mysql/data/tabela_livros.csv")
add_data(connection, cursor, df_livros, "db_produtos", "tb_livros")
df_produtos2021 = read_csv("/home/pedro/documents/pipeline-mongodb-mysql/data/tabela_produtos_2021_em_diante.csv")
add_data(connection, cursor, df_produtos2021, "db_produtos", "tb_produtos2021")
cursor.close()
connection.close()