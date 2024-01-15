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
                if_exists="replace",
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


def normalization_cities_table(df):
    # Eliminar filas con datos vacíos
    df = df.dropna()

    # Verificar y corregir la primera letra de cada palabra en la columna "name"
    df["name"] = df["name"].apply(
        lambda x: " ".join(word.capitalize() for word in x.split())
    )

    # Verificar y convertir la columna "latitude" a float
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")

    # Verificar y convertir la columna "longitude" a float
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # Eliminar filas en las que la columna "country" no sea 'US'
    df = df[df["country"] == "US"]

    # Convertir la columna "population" a tipo string y cambiar el punto por coma
    df["population"] = df["population"].astype(str).str.replace(".", ",")

    # Convertir la columna "updated_at" a tipo de dato datetime
    df["updated_at"] = pd.to_datetime(df["updated_at"]).dt.floor("T")

    return df


def normalization_schools_table(df):
    # Eliminar filas con datos vacíos
    df = df.dropna()

    # Verificar y eliminar filas con id repetido
    df = df.drop_duplicates(subset="id", keep="first")

    # Verificar y corregir la primera letra de cada palabra en la columna "city"
    df["city"] = df["city"].apply(
        lambda x: " ".join(word.capitalize() for word in x.split())
    )

    # Convertir la columna 'zip' a tipo string y luego eliminar puntos
    df["zip"] = df["zip"].astype(str).str.replace(".", "")

    # Verificar y corregir la columna "url"
    df["url"] = df["url"].apply(
        lambda x: "https://" + x if not x.startswith("https://") else x
    )

    # Convertir las siguientes columnas a tipo de dato integer
    int_columns = [
        "ownership",
        "main_campus",
        "online_only",
        "open_admissions_policy",
        "degrees_awarded_predominant",
        "degrees_awarded_predominant_recoded",
    ]

    # Convertir a tipo de dato numérico antes de la conversión a entero
    for col in int_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convertir la columna "updated_at" a tipo de dato datetime y eliminar los segundos
    df["updated_at"] = pd.to_datetime(df["updated_at"]).dt.floor("T")

    return df


def main():
    s_df = read_postgres_table_to_dataframe(os.getenv("SCHOOLS_TABLE_NAME"))
    c_df = read_postgres_table_to_dataframe(os.getenv("CITIES_TABLE_NAME"))
    s_df = add_updated_at_column(s_df)
    city_df = add_updated_at_column(c_df)
    print("Antes de la normalizacion")
    print(s_df)
    print(c_df)
    print("Despues de la normalizacion:")
    s_df = normalization_schools_table(s_df)
    c_df = normalization_cities_table(c_df)
    print(s_df)
    print(c_df)
    insert_into_rds(
        [
            [s_df, os.getenv("RDS_SCHOOLS_TABLE_NAME")],
            [c_df, os.getenv("RDS_CITIES_TABLE_NAME")],
        ]
    )


if __name__ == "__main__":
    main()
