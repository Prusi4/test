import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Users(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement= False)
    name = Column(String)
    username = Column(String)
    email = Column(String)
    address_street = Column(String)
    address_suite = Column(String)
    address_city = Column(String)
    address_zipcode = Column(String)
    phone = Column(String)
    website = Column(String)
    company_name = Column(String)
    company_catchPhrase = Column(String)
    company_bs = Column(String)
    

engine = create_engine('sqlite:///dbusers.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


def upsertUsers(session, data):
    for record in data:
        existing_user = session.query(Users).filter_by(id=record['id']).first()
        if existing_user:
            for key, value in record.items():
                setattr(existing_user, key, value)
        else:
            new_user = Users(**record)
            session.add(new_user)
    session.commit()
    
    
url = 'https://jsonplaceholder.typicode.com/users'

def GetUsers(url):
    response = requests.get(url)
    data = response.json()
    
    processed_data = []
    for user in data:
        processed_user = {
            'id': user.get('id', 0),
            'name': user.get('name', ''),
            'username': user.get('username', ''),
            'email': user.get('email', ''),
            'address_street': user['address'].get('street', ''),
            'address_suite': user['address'].get('suite', ''),
            'address_city': user['address'].get('city', ''),
            'address_zipcode': user['address'].get('zipcode', ''),
            'phone': user.get('phone', ''),
            'website': user.get('website', ''),
            'company_name': user['company'].get('name', ''),
            'company_catchPhrase': user['company'].get('catchPhrase', ''),
            'company_bs': user['company'].get('bs', '')
        }
        processed_data.append(processed_user)
    
    return processed_data

usuarios = GetUsers(url)

df = pd.DataFrame(usuarios)

df.drop_duplicates(subset=['id'], inplace=True)

upsertUsers(session, df.to_dict(orient='records'))

session.close()

df_from_sqlite = pd.read_sql('users', engine)
print(df_from_sqlite)

esto aqui random a ver si se actualiza en github # type: ignore

