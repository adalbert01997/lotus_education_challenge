# -- coding: utf-8 --
"""
Created on Wed Jan 10 14:53:48 2024

@author: Adalberto González
"""

# Python Libraries
import os
import requests
import pandas as pd
from math import floor

# Python Modules
import constants as C

def get_school_data(params, page_limit, url):
    # Inicializar una lista para almacenar los resultados
    results_list = []
    while True:
        # Realizar solicitud a la API
        response = requests.get(url, params=params)
        # Verificar el estado de la solicitud
        if response.status_code == 200:
            # Convertir la respuesta a formato JSON
            data = response.json()
            max_page = floor(int(data['metadata']['total']) / int(data['metadata']['per_page']))
            if int(data['metadata']['page']) > max_page or data['metadata']['page'] == page_limit:
                break
            print(params['page'])
            params['page'] += 1
            # print(data)
            # print(data['metadata']['page'])

            # Iterar sobre los resultados y extraer la información relevante
            for result in data['results']:
                # Crear un diccionario para almacenar la información de esta fila
                row = {
                    'id': result['id'],
                    'name': result['school.name'],
                    'city': result['school.city'],
                    'state': result['school.state'],
                    'zip': result['school.zip'],
                    'url': result['school.school_url'],
                    'ownership': result['school.ownership'],
                    'main_campus': result['school.main_campus'],
                    'online_only': result['school.online_only'],
                    'open_admissions_policy': result['school.open_admissions_policy'],
                    'degrees_awarded_predominant':result['school.degrees_awarded.predominant'],
                    'degrees_awarded_predominant_recoded':result['school.degrees_awarded.predominant_recoded'],
                }


                # Agregar la fila a la lista de resultados
                results_list.append(row)
        else:
            # Imprimir un mensaje de error si la solicitud no fue exitosa
            print(f"Error en la solicitud. Código de estado: {response.status_code}")
            return None
    # Crear un DataFrame a partir de la lista de resultados
    df = pd.DataFrame(results_list)
    pd.set_option('display.max_columns', None)
    # Mostrar el DataFrame
    return df

def main():
    university_df = get_school_data(C.UNIVERSITY_PARAMS, C.SCHOOLS_API_PAGE_LIMIT, C.SCHOOLS_ENDPOINT)
    print(university_df)

if __name__ == "__main__":
    main()