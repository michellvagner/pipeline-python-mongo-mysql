# %%
import pandas as pd
from extract_and_save_data import settings, connect_mongo, create_connect_db, create_connect_collection


def visualize_collection(col):
    for doc in col.find():
        print(doc)

def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {col_name: new_name, col_name:new_name}})

def select_category(col, category):
    query = {"Categoria do Produto": category}

    lista_livros = []

    for doc in col.find(query):
        lista_livros.append(doc)

    return lista_livros

def make_regex(col, regex):
    query = {"Data da Compra": {"$regex": regex}}

    lista_produtos = []
    for doc in col.find(query):
        lista_produtos.append(doc)

    return lista_produtos

def create_dataframe(lista):
    return pd.DataFrame(lista)

def format_date(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format='%d/%m/%Y')
    df["Data da Compra"] = df["Data da Compra"].dt.strftime('%Y-%m-%d')
    return df

def save_csv(df, path):
    df.to_csv(path, index=False)

# %%
if __name__ == "__main__":

    uri = settings()['uri']
    client = connect_mongo(uri)
    db = create_connect_db(client, "db_produtos")
    collection = create_connect_collection(db, "produtos")
    visualize_collection(collection)
    colunas = {"lat": "Latitude", "lon":"Longitude"}
    rename_column(collection, "lat", "Latitude")
    rename_column(collection, "lon", "Longitude")
    lista_livros = select_category(collection, "livros")
    lista_livros = make_regex(collection, r"/202[1-9]")
    df_livros = create_dataframe(lista_livros)
    save_csv(df_livros, '../data/tabela_2021_em_diante.csv')

# %%
