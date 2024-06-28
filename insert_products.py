import math
import os
from ftplib import FTP
from ftplib import FTP_TLS

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select, update, insert

from emailutil import send_email


def readcsv(csv):
    pdf = pd.read_csv(csv, on_bad_lines="skip")
    return pdf


def download_zvt_csv(host, user, password, remotepath, localpath):
    ftp = FTP()
    ftp.connect(host=host)
    ftp.login(user=user, passwd=password)
    with open(localpath, 'wb') as local_file:
        ftp.retrbinary(f"RETR {remotepath}", local_file.write)

    ftp.quit()


def download_mvn_csv(host, user, password, ftpfilename, foldername, localfilename):
    ftp = FTP_TLS()
    ftp.connect(host=host, port=5151)
    ftp.login(user=user, passwd=password)
    # Change to the remote directory if needed
    ftp.cwd(foldername)

    # Use the PROT command to set the data channel protection level to private
    ftp.prot_p()

    with open(localfilename, 'wb') as local_file:
        ftp.retrbinary('RETR ' + ftpfilename, local_file.write)
    ftp.quit()


def zvt_insert(products_pdf):
    products = products_pdf[["Style", "Short Description"]]
    products = products.rename(columns={"Style": "sku", "Short Description": "name"})
    products["enabled"] = True
    products["brand"] = "zavate"
    products = products.groupby("sku").head(1).reset_index(drop=True)

    metadata = MetaData()
    metadata.reflect(bind=engine)
    product_table = Table("product", metadata, autoload=True, autoload_with=engine)
    variation_table = Table("variation", metadata, autoload=True, autoload_with=engine)

    for idx, product in products.iterrows():
        where_condition = product_table.c.sku == str(product["sku"])
        stmt = select(product_table).where(where_condition)

        with engine.connect() as connection:
            result = connection.execute(stmt).fetchall()
            connection.close()

        if len(result) > 0:
            stmt = update(product_table).where(where_condition).values(name=product["name"],
                                                                       enabled=product["enabled"],
                                                                       brand=product["brand"])
        else:
            stmt = insert(product_table).values(sku=str(product["sku"]), name=product["name"],
                                                enabled=product["enabled"],
                                                brand=product["brand"])

        with engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()
            connection.close()

        condition = products_pdf['Style'] == str(product["sku"])
        variations = products_pdf[condition]

        variations = variations[["Size", "Color Description", "MAP", "Available", "Cost", "Vendor Number", "UPC"]]
        variations = variations.rename(
            columns={"Vendor Number": "idsku", "Size": "size", "Color Description": "color", "MAP": "price",
                     "Available": "qty",
                     "Cost": "cog",
                     "UPC": "upc"
                     })
        variations["idsku"] = variations["idsku"].str.replace(" ", "-")

        for idx, variation in variations.iterrows():
            variation["cog"] = float(variation["cog"][1:])
            variation["enabled"] = True
            variation["sku"] = str(product["sku"])
            if math.isnan(variation["qty"]):
                variation["qty"] = 0

            where_condition = variation_table.c.idsku == variation["idsku"]
            stmt = select(variation_table).where(where_condition)
            with engine.connect() as connection:
                var_result = connection.execute(stmt).fetchall()
                connection.close()

            if len(var_result) > 0:
                stmt = update(variation_table).where(where_condition).values(price=variation["price"],
                                                                             qty=variation["qty"],
                                                                             cog=variation["cog"],
                                                                             upc=variation["upc"])
            else:
                stmt = insert(variation_table).values(idsku=variation["idsku"],
                                                      size=variation["size"],
                                                      color=variation["color"],
                                                      price=variation["price"],
                                                      qty=variation["qty"],
                                                      cog=variation["cog"],
                                                      enabled=variation["enabled"],
                                                      sku=variation["sku"],
                                                      upc=variation["upc"]
                                                      )

            with engine.connect() as connection:
                connection.execute(stmt)
                connection.commit()
                connection.close()


def mvn_insert(products_pdf):
    products = products_pdf[["Style", "ProductTitle"]]
    products = products.rename(columns={"Style": "sku", "ProductTitle": "name"})
    products["enabled"] = True
    products["brand"] = "maevn"
    products = products.groupby("sku").head(1).reset_index(drop=True)

    metadata = MetaData()
    metadata.reflect(bind=engine)
    product_table = Table("product", metadata, autoload=True, autoload_with=engine)
    variation_table = Table("variation", metadata, autoload=True, autoload_with=engine)
    varimg_table = Table("varimg", metadata, autoload=True, autoload_with=engine)

    for idx, product in products.iterrows():
        where_condition = product_table.c.sku == str(product["sku"])
        stmt = select(product_table).where(where_condition)

        with engine.connect() as connection:
            result = connection.execute(stmt).fetchall()
            connection.close()

        if len(result) > 0:
            stmt = update(product_table).where(where_condition).values(name=product["name"],
                                                                       enabled=product["enabled"],
                                                                       brand=product["brand"])
        else:
            stmt = insert(product_table).values(sku=str(product["sku"]), name=product["name"],
                                                enabled=product["enabled"],
                                                brand=product["brand"])

        with engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()
            connection.close()

        products_pdf["Style"] = products_pdf["Style"].apply(str)
        condition = products_pdf['Style'] == str(product["sku"])
        variations = products_pdf[condition]

        variations = variations[["SKU", "Size", "ColorName", "MAP", "Onhand", "WholeSales", "UPC", "image1", "image2",
                                 "image3", "image4", "image5", "image6"]]
        variations = variations.rename(
            columns={"SKU": "idsku", "Size": "size", "ColorName": "color", "MAP": "price",
                     "Onhand": "qty",
                     "WholeSales": "cog",
                     "UPC": "upc"})

        for idx, variation in variations.iterrows():
            variation["enabled"] = True
            variation["sku"] = str(product["sku"])
            if math.isnan(variation["qty"]):
                variation["qty"] = 0

            where_condition = variation_table.c.idsku == variation["idsku"]
            stmt = select(variation_table).where(where_condition)
            with engine.connect() as connection:
                var_result = connection.execute(stmt).fetchall()
                connection.close()

            if len(var_result) > 0:
                stmt = update(variation_table).where(where_condition).values(price=variation["price"],
                                                                             qty=variation["qty"],
                                                                             cog=variation["cog"],
                                                                             upc=variation["upc"]
                                                                             )
            else:
                stmt = insert(variation_table).values(idsku=variation["idsku"],
                                                      size=variation["size"],
                                                      color=variation["color"],
                                                      price=variation["price"],
                                                      qty=variation["qty"],
                                                      cog=variation["cog"],
                                                      enabled=variation["enabled"],
                                                      sku=variation["sku"],
                                                      upc=variation["upc"])

            with engine.connect() as connection:
                connection.execute(stmt)
                connection.commit()
                connection.close()

            where_condition = varimg_table.c.idsku == variation["idsku"]
            stmt = select(varimg_table).where(where_condition)
            with engine.connect() as connection:
                varimg_result = connection.execute(stmt).fetchall()
                connection.close()

            if len(varimg_result) == 0:
                if len(str(variation["image1"])) > 0:
                    stmt = insert(varimg_table).values(idsku=variation["idsku"], imgurl=variation["image1"])
                    with engine.connect() as connection:
                        connection.execute(stmt)
                        connection.commit()
                        connection.close()

                if len(str(variation["image2"])) > 0:
                    stmt = insert(varimg_table).values(idsku=variation["idsku"], imgurl=variation["image2"])
                    with engine.connect() as connection:
                        connection.execute(stmt)
                        connection.commit()
                        connection.close()

                if len(str(variation["image3"])) > 0:
                    stmt = insert(varimg_table).values(idsku=variation["idsku"], imgurl=variation["image3"])
                    with engine.connect() as connection:
                        connection.execute(stmt)
                        connection.commit()
                        connection.close()

                if len(str(variation["image4"])) > 0:
                    stmt = insert(varimg_table).values(idsku=variation["idsku"], imgurl=variation["image4"])
                    with engine.connect() as connection:
                        connection.execute(stmt)
                        connection.commit()
                        connection.close()

                if len(str(variation["image5"])) > 0:
                    stmt = insert(varimg_table).values(idsku=variation["idsku"], imgurl=variation["image5"])
                    with engine.connect() as connection:
                        connection.execute(stmt)
                        connection.commit()
                        connection.close()

                if len(str(variation["image6"])) > 0:
                    stmt = insert(varimg_table).values(idsku=variation["idsku"], imgurl=variation["image6"])
                    with engine.connect() as connection:
                        connection.execute(stmt)
                        connection.commit()
                        connection.close()


def cbi_insert(products_pdf):
    products = products_pdf[["Style", "Style Description"]]
    products = products.rename(columns={"Style": "sku", "Style Description": "name"})
    products["enabled"] = True
    products["brand"] = "cbi"
    products = products.groupby("sku").head(1).reset_index(drop=True)

    metadata = MetaData()
    metadata.reflect(bind=engine)
    product_table = Table("product", metadata, autoload=True, autoload_with=engine)
    variation_table = Table("variation", metadata, autoload=True, autoload_with=engine)

    for idx, product in products.iterrows():
        where_condition = product_table.c.sku == str(product["sku"])
        stmt = select(product_table).where(where_condition)

        with engine.connect() as connection:
            result = connection.execute(stmt).fetchall()
            connection.close()

        if len(result) > 0:
            stmt = update(product_table).where(where_condition).values(name=product["name"],
                                                                       enabled=product["enabled"],
                                                                       brand=product["brand"])
        else:
            stmt = insert(product_table).values(sku=str(product["sku"]), name=product["name"],
                                                enabled=product["enabled"],
                                                brand=product["brand"])

        with engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()
            connection.close()

        products_pdf["Style"] = products_pdf["Style"].apply(str)
        condition = products_pdf['Style'] == str(product["sku"])
        variations = products_pdf[condition]

        variations = variations[
            ["Product ID", "Size", "Color Desc", "MSRP_USD", "Qty On Hand", "Cost", "UPC", "Status"]]
        variations = variations.rename(
            columns={"Product ID": "idsku", "Size": "size", "Color Desc": "color", "MSRP_USD": "price",
                     "Qty On Hand": "qty",
                     "Cost": "cog",
                     "UPC": "upc",
                     "Status": "status"
                     })

        for idx, variation in variations.iterrows():
            variation["enabled"] = True
            variation["sku"] = str(product["sku"])
            if math.isnan(variation["qty"]):
                variation["qty"] = 0
            if variation["status"] == "Discontinued":
                variation["qty"] = 0

            if variation["sku"] == "MC2411" and variation["color"] == "Perfectly Pink":
                variation["price"] = variation["price"] + 4

            where_condition = variation_table.c.idsku == variation["idsku"]
            stmt = select(variation_table).where(where_condition)
            with engine.connect() as connection:
                var_result = connection.execute(stmt).fetchall()
                connection.close()

            if len(var_result) > 0:
                stmt = update(variation_table).where(where_condition).values(price=variation["price"],
                                                                             qty=variation["qty"],
                                                                             cog=variation["cog"],
                                                                             upc=variation["upc"]
                                                                             )
            else:
                stmt = insert(variation_table).values(idsku=variation["idsku"],
                                                      size=variation["size"],
                                                      color=variation["color"],
                                                      price=variation["price"],
                                                      qty=variation["qty"],
                                                      cog=variation["cog"],
                                                      enabled=variation["enabled"],
                                                      sku=variation["sku"],
                                                      upc=variation["upc"])

            with engine.connect() as connection:
                connection.execute(stmt)
                connection.commit()
                connection.close()


def download_cbi_csv(host, user, password, remotepath, localpath):
    ftp = FTP()
    ftp.connect(host=host)
    ftp.login(user=user, passwd=password)

    with open(localpath, 'wb') as local_file:
        ftp.retrbinary(f"RETR {remotepath}", local_file.write)

    ftp.quit()


if __name__ == '__main__':
    host: str = os.getenv("ZVT_HOST")
    user: str = os.getenv("ZVT_USER")
    password: str = os.getenv("ZVT_PASS")
    zvt_localpath: str = os.getenv("ZVT_PATH")

    remotepath: str = "Data/UM/Inventory.csv"
    download_zvt_csv(host=host, user=user, password=password, remotepath=remotepath, localpath=zvt_localpath)

    host: str = os.getenv("MVN_HOST")
    user: str = os.getenv("MVN_USER")
    password: str = os.getenv("MVN_PASS")
    mvn_localpath: str = os.getenv("MVN_PATH")

    filename: str = "IRG_Group_and_Maevn_MasterFile.csv"
    foldername: str = "googlesheet"

    download_mvn_csv(host=host, user=user, password=password,
                     ftpfilename=filename, foldername=foldername, localfilename=mvn_localpath)

    host: str = os.getenv("CBI_HOST")
    user: str = os.getenv("CBI_USER")
    password: str = os.getenv("CBI_PASS")
    cbi_localpath: str = os.getenv("CBI_PATH")
    remotepath: str = "Inventory/SPIInventory.xlsx"

    download_cbi_csv(host=host, user=user, password=password, remotepath=remotepath, localpath=cbi_localpath)

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    server = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")

    connection_string = f"mssql+pyodbc://{user}:{password}@{server}:{port}/{dbname}?driver=ODBC Driver 17 for SQL Server"
    engine = create_engine(connection_string)

    try:
        zvt_insert(products_pdf=readcsv(csv=zvt_localpath))
        print("Zavate Execution completed!")
        mvn_insert(products_pdf=readcsv(csv=mvn_localpath))
        print("Maevn Execution completed!")
        cbi_insert(products_pdf=pd.read_excel(cbi_localpath))
        print("CBI Execution completed!")
    except Exception as e:
        print("Error:", e)
        send_email(subject="Autonicals eCommerce", body="Failed to updated products in DB.",
                   from_email="shahan@autonicals.org", to_email="shahan@autonicals.com",
                   cc_emails=["shahan@autonicals.com", "shahan.mehboob@outlook.com"])
        exit()
