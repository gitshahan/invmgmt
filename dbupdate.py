import json
from datetime import datetime

import pytz
from sqlalchemy import create_engine, MetaData, Table, insert, update, select, func

from dotenv import load_dotenv
import os
from woocommerce import API

if __name__ == '__main__':
    load_dotenv()

    timezone = pytz.timezone('Asia/Karachi')

    # DB credentials
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    server = os.getenv("DB_HOST")
    port = 1433
    dbname = "btcstakingdb"

    # WooCommerce credentials
    wck = os.getenv('WCK')
    wcs = os.getenv('WCS')
    appurl = "https://store.autonicals.org"

    wcapi = API(
        url=appurl,
        consumer_key=wck,
        consumer_secret=wcs
    )

    connection_string = f"mssql+pyodbc://{user}:{password}@{server}:{port}/{dbname}?driver=ODBC Driver 17 for SQL Server"
    engine = create_engine(connection_string)
    metadata = MetaData()
    metadata.reflect(bind=engine)
    product_table = Table("wproducts", metadata, autoload=True, autoload_with=engine)

    products = wcapi.get(f"products/1214").json()
    print(json.dumps(products, indent=4))
    exit()

    for product in products:
        id = product['id']
        sku = product['sku']
        name = product['name']

        where_condition = product_table.c.id == id
        stmt = select(product_table).where(where_condition)
        with engine.connect() as connection:
            db_product = connection.execute(stmt).fetchall()
            connection.close()

        if len(db_product) > 0:
            current_datetime = datetime.now(timezone)
            stmt = update(product_table).where(where_condition).values(sku=sku, name=name, updatedon=current_datetime)
            print(f"{id} Updated")
        else:
            current_datetime = datetime.now(timezone)
            stmt = insert(product_table).values(id=id, sku=sku, name=name, updatedon=current_datetime)
            print(f"{id} Inserted")

        with engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()
            connection.close()


