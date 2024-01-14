# -- coding: utf-8 --
"""
Created on Wed Jan 10 14:53:48 2024

@author: Adalberto González
"""

# Python Libraries
import os
import json
import requests
import pandas as pd
from math import floor
from datetime import datetime
from sqlalchemy import create_engine

# Python Modules
import constants as C


def insert_into_rds(data):
    """Connects to Postgres instance and uploads incoming Pandas dataframe"""
    try:
        host = os.getenv("RDS_HOSTNAME")
        user = os.getenv("RDS_USER")
        password = os.getenv("RDS_PASSWORD")
        db = os.getenv("RDS_DB")
        port = os.getenv("RDS_PORT")
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
        for df, table_name in data:
            df.to_sql(
                con=engine,
                name=table_name,
                # schema=os.getenv("POSTGRES_DEFAULT_SCHEMA"),
                if_exists="append",
                index=False,
            )
        print("Finished!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    except Exception as e:
        print(f"ERROR: {e}")


def read_postgres_table_to_dataframe(table_name):
    try:
        # Obtener variables de entorno
        host = os.getenv("POSTGRES_SERVICE_NAME")
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        db = os.getenv("POSTGRES_DB")
        port = os.getenv("POSTGRES_PORT")

        # Verificar si alguna variable de entorno es None
        if None in (host, user, password, db, port):
            print(
                "Error: No se han proporcionado todas las variables de entorno necesarias."
            )
            return None
        # Crea una conexión al motor de la base de datos PostgreSQL
        engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        )
        # Consulta SQL para seleccionar todos los datos de la tabla
        query = f"SELECT * FROM {table_name};"
        # Lee la tabla en un DataFrame de pandas
        df = pd.read_sql_query(query, engine.raw_connection(), coerce_float=True)

        return df
    except Exception as e:
        print(f"Error al leer la tabla: {e}")
        return None


def add_updated_at_column(df):
    # Obtén la fecha y hora actual
    current_datetime = datetime.now()

    # Añade la nueva columna 'updated_at' al DataFrame
    df["updated_at"] = current_datetime

    return df


def main():
    school_df = read_postgres_table_to_dataframe(os.getenv("SCHOOLS_TABLE_NAME"))
    city_df = read_postgres_table_to_dataframe(os.getenv("CITIES_TABLE_NAME"))
    school_df = add_updated_at_column(school_df)
    city_df = add_updated_at_column(city_df)
    print(school_df)
    print(city_df)
    insert_into_rds(
        [
            [school_df, os.getenv("RDS_SCHOOLS_TABLE_NAME")],
            [city_df, os.getenv("RDS_CITIES_TABLE_NAME")],
        ]
    )


if __name__ == "__main__":
    main()
