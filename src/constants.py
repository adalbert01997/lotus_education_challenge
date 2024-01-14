import os

SCHOOLS_API_PAGE_LIMIT = 1
SCHOOLS_ENDPOINT = os.environ.get("SCHOOLS_ENDPOINT")
SCHOOLS_API_KEY = os.environ.get("SCHOOLS_API_KEY")

UNIVERSITY_PARAMS = {
    "api_key": SCHOOLS_API_KEY,
    "fields": "id,school.name,school.city,school.state,school.zip,school.school_url,school.ownership,school.main_campus,school.online_only,school.open_admissions_policy,school.degrees_awarded.predominant,school.degrees_awarded.predominant_recoded,",  # Número de resultados por página
    "page": 0,
}

CITIES_API_ENDPOINT = os.environ.get("CITIES_API_ENDPOINT")
CITIES_API_KEY = os.environ.get('CITIES_API_KEY')
