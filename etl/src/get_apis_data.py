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
from sqlalchemy import create_engine

# Python Modules
import constants as C


def get_city_data(cities):
    """
    get_citie_data recibe como parametro un df de escuelas obtiene la columna city y genera un df de la informacion de las ciudades a través de una API

    """
    city_data = []
    for city in cities:
        api_url = f"{C.CITIES_API_ENDPOINT}?name={city}"
        response = requests.get(api_url, headers={"X-Api-Key": C.CITIES_API_KEY})
        if response.status_code == requests.codes.ok:
            print(response.text)
            if len(json.loads(response.text)) > 0:
                city_data.append(json.loads(response.text)[0])
        else:
            print("Error:", response.status_code, response.text)
    df = pd.DataFrame(city_data)
    pd.set_option("display.max_columns", None)
    return df


def get_school_data(params, page_limit, url):
    """
    get_school_data recibe parametros para la conexión con la API, un limite de paginas para que no traiga toda la informacion, y el url del endpoint.
    """
    # Inicializar una lista para almacenar los resultados
    results_list = []
    while True:
        # Realizar solicitud a la API
        response = requests.get(url, params=params)
        # Verificar el estado de la solicitud
        if response.status_code == 200:
            # Convertir la respuesta a formato JSON
            data = response.json()
            max_page = floor(
                int(data["metadata"]["total"]) / int(data["metadata"]["per_page"])
            )
            if (
                int(data["metadata"]["page"]) > max_page
                or data["metadata"]["page"] == page_limit
            ):
                break
            print(params["page"])
            params["page"] += 1
            # print(data)
            # print(data['metadata']['page'])

            # Iterar sobre los resultados y extraer la información relevante
            for result in data["results"]:
                # Crear un diccionario para almacenar la información de esta fila
                row = {
                    "id": result["id"],
                    "name": result["school.name"],
                    "city": result["school.city"],
                    "state": result["school.state"],
                    "zip": result["school.zip"],
                    "url": result["school.school_url"],
                    "ownership": result["school.ownership"],
                    "main_campus": result["school.main_campus"],
                    "online_only": result["school.online_only"],
                    "open_admissions_policy": result["school.open_admissions_policy"],
                    "degrees_awarded_predominant": result[
                        "school.degrees_awarded.predominant"
                    ],
                    "degrees_awarded_predominant_recoded": result[
                        "school.degrees_awarded.predominant_recoded"
                    ],
                }

                # Agregar la fila a la lista de resultados
                results_list.append(row)
        else:
            # Imprimir un mensaje de error si la solicitud no fue exitosa
            print(f"Error en la solicitud. Código de estado: {response.status_code}")
            return None
    # Crear un DataFrame a partir de la lista de resultados
    df = pd.DataFrame(results_list)
    pd.set_option("display.max_columns", None)
    # Mostrar el DataFrame
    return df


def insert_into_postgres(data):
    """Connects to Postgres instance and uploads incoming Pandas dataframe"""
    try:
        host = os.getenv("POSTGRES_SERVICE_NAME")
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        db = os.getenv("POSTGRES_DB")
        port = os.getenv("POSTGRES_PORT")
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

        for df, table_name in data:
            df.to_sql(
                con=engine,
                name=table_name,
                schema=os.getenv("POSTGRES_DEFAULT_SCHEMA"),
                if_exists="append",
                index=False,
            )
        print("Finished!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    except Exception as e:
        print(f"ERROR: {e}")


def main():
    university_df = get_school_data(
        C.UNIVERSITY_PARAMS, C.SCHOOLS_API_PAGE_LIMIT, C.SCHOOLS_ENDPOINT
    )
    school_cities = set(university_df.city.tolist())
    city_df = get_city_data(school_cities)
    insert_into_postgres(
        [
            [university_df, os.getenv("SCHOOLS_TABLE_NAME")],
            [city_df, os.getenv("CITIES_TABLE_NAME")],
        ]
    )


if __name__ == "__main__":
    main()
