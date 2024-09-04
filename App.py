import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

#voglio creare un database sqlite locale con questa struttura
#definisco una classe che mapperà la struttura della tabella prodotti
# per vscode ho installato l'estensione Name: SQLite
'''
Id: alexcvzz.vscode-sqlite
Description: Explore and query SQLite databases.
Version: 0.14.1
Publisher: alexcvzz
VS Marketplace Link: https://marketplace.visualstudio.com/items?itemName=alexcvzz.vscode-sqlite
'''

class Prodotti(Base):
    __tablename__ = 'prodotti'
    id = Column(Integer, primary_key=True, autoincrement=False)
    title = Column(String)
    description = Column(String)
    category = Column(String)
    price = Column(Float)
    discountPercentage = Column(Float)
    rating = Column(Float)
    stock = Column(Integer)
    brand = Column(String)
    SKU = Column(String)

# Creo il db ed avvio la sessione dell'ORM
engine = create_engine('sqlite:///dbprodotti.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# funzione per creare un upsert (provo ad inserire qualora dovesse esistere sovrascrivo altrimenti inserisco come nuovo)
def upsertProdotti(session, data):
    for record in data:
        existing_product = session.query(Prodotti).filter_by(id=record['id']).first() #ID è la mia chiave primaria
        if existing_product:
            for key, value in record.items():
                setattr(existing_product, key, value)
        else:
            #Inserisco un nuovo prodotto
            new_product = Prodotti(**record)
            session.add(new_product)
    session.commit()



#utilizzo la pagina https://dummyjson.com/docs/products#products-limit_skip per fare un esempio di lettura di un dato 
#dalle specifiche vediamo che 
#### -> You can pass limit and skip params to limit and skip the results for pagination, and use limit=0 to get all items.

limit = 30
skip = 0
url_template = 'https://dummyjson.com/products?limit={limit}&skip={skip}' #limit e skip sono parametri che verranno incrementati/decrementati

def GetProdotti(url):
    response = requests.get(url)
    data = response.json()

    products = data.get('products',[]) #utilizzo la funzione json.get per evitare di ricevere un errore in caso che products non esista
    total = data.get('total',0)
    
    # devo prendere le chiavi che mi servono 
    processed_data = []

    if len(products) > 0:
        for product in products:
            processed_product = {
                'id': product.get('id', 0),
                'title': product.get('title', ''),
                'description': product.get('description', ''),
                'category': product.get('category', ''),
                'price': product.get('price', 0.0),
                'discountPercentage': product.get('discountPercentage', 0.0),
                'rating': product.get('rating', 0.0),
                'stock': product.get('stock', 0),
                'brand': product.get('brand', ''),
                'SKU': product.get('sku', '')
            }
            processed_data.append(processed_product)

    return processed_data, total


all_products = []
total_products = 0

while True:
    url = url_template.format(limit=limit, skip=skip)
    products, total = GetProdotti(url)
    all_products.extend(products)
    total_products += len(products)
    skip += limit
    if total_products >= total:
        break

# creo il dataframe dalla lista di dizionari, non passo il parametro columns, perchè viene preso dalle chiavi dei dizionari
df = pd.DataFrame(all_products)

#rimuovo i duplicati della chiave primaria
df.drop_duplicates(subset=['id'],inplace=True) # mettendo inplace=True non crea una copia del dataframe ma la modifica avviene sul dataframe stesso

# Upsert del dato
upsertProdotti(session, df.to_dict(orient='records'))

# Chiudo la sessione
session.close()



#esercizio utilizza pandas per leggere il json direttamente come dataframe

df_from_sqlite = pd.read_sql('prodotti', engine)
print(df_from_sqlite)