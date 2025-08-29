import logging
from utils.normalize_string import normalize_string, normalize_string_special_chars
from utils.constants import GAFI_COUNTRIES


def append_gafi_results(entity_list, hasGAFI):
    gafi_results = []
    countries_lower = [country.lower() for country in GAFI_COUNTRIES]
    if not hasGAFI:
        return gafi_results
    for entity in entity_list:
        try:
            country_value = entity.get('country')
            if country_value in (None,""):
                continue

            variable_to_check = normalize_string(country_value.lower()) if country_value else None
            result_conditional = (
                'El país pertenece a la lista de países no cooperantes'
                if variable_to_check in countries_lower
                else ''
            )

            if result_conditional:
                results = [{
                    "entityIdNumber": entity.get('entityIdNumber', ''),
                    "name": entity.get('name', ''),
                    "commercialName": entity.get('commercialName', ''),
                    "country": entity.get('country', ''), 
                    "coincidence": result_conditional,
                    "RequestStatus": "200",
                    "DescripcionStatus": "OK",
                    "URL": "https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions/increased-monitoring-october-2024.html"
                }]
                gafi_results.append({
                    "entityIdNumber": entity.get('entityIdNumber', ''),
                    "name": entity.get('name', ''),
                    "commercialName": entity.get('commercialName', ''),
                    'requestStatus': "200",
                    'results': results
                })
        except Exception as e:
            logging.error(f"Error processing entity: {e}")
            
    return gafi_results
