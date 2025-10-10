# %%
import mysql.connector
import pandas as pd
from extract_and_save_data import settings

# %%

def connect_mysql(host_name, user_name, pw):
    cnx = mysql.connector.connect(
        host=host_name,
        user=user_name,
        password=pw
    )
    return cnx

def create_cursor(cnx):
    return cnx.cursor()

def create_database(cursor, db_name):
    return cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")

def show_databases(cursor):
    cursor.execute("SHOW DATABASES;")
    for db in cursor:
        print(db)

def create_product_table(cursor, db_name, tb_name):
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
    cursor.execute(f"USE {db_name};")
    cursor.execute(f"SHOW TABLES;")
    for db in cursor:
        print(db)

def read_csv(path):
    return pd.read_csv(path)

def add_product_data(cnx, cursor, df, db_name, tb_name):

    lista_dados = [tuple(row) for _, row in df.iterrows()]
    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.executemany(sql, lista_dados)
    cnx.commit()

# %%

if __name__ == "__main__":
    host = settings()['mysql_host']
    user = settings()['mysql_user']
    pw = settings()['mysql_password']
    cnx = connect_mysql(host, user, pw)
    cursor = create_cursor(cnx)
    create_database(cursor, 'dbprodutos')
    show_databases(cursor)
    create_product_table(cursor, 'dbprodutos', 'tb_livros')
    create_product_table(cursor, 'dbprodutos', 'tb_produtos_2021_em_diante')
    show_tables(cursor, 'dbprodutos')
    df_livros = read_csv("../data/tabela_livros.csv")
    add_product_data(cnx, cursor, df_livros, 'dbprodutos', 'tb_livros')

    df_2021_em_diante = read_csv("../data/tabela_2021_em_diante.csv")
    df_2021_em_diante['Data da Compra'] = pd.to_datetime(df_2021_em_diante['Data da Compra'], dayfirst=True)

    add_product_data(cnx, cursor, df_2021_em_diante, 'dbprodutos', 'tb_livros')
    

# %%
