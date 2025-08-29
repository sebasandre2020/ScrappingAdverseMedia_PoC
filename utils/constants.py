JURIDICAL_PERSON = "Juridical"
NATURAL_PERSON = "Natural"
ENTITY_TYPE_ORG = "organization"
# ENDPOINT_URL = "http://localhost:7072/api/SearchSource"
ENDPOINT_URL = "https://searchsourcedd.azurewebsites.net/api/searchsource"
SOURCECODE_ADVERSE_MEDIA = "BG1"
SOURCECODE_AD_MEDIA = "BG57"
TIMEOUT = 200
ENDPOINT_TIMEOUT = 200
WEBSCRAP_QUEUE = 'dd_queue_bus-env_1'
SOURCE_CODE_GAFI = '2101'
GAFI_COUNTRIES = [
    "Argelia", "Angola", "Bulgaria", "Burkina Faso", "Camerún", "Costa de Marfil", "Croacia",
    "República Democrática del Congo", "Haití", "Kenia", "Líbano", "Malí", "Mónaco",
    "Mozambique", "Namibia", "Nigeria", "Filipinas", "Sudáfrica", "Sudán del Sur", "Siria",
    "Tanzania", "Venezuela", "Vietnam", "Yemen"
]
USER_AGENTS = [
    # Latest Microsoft Edge (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",    
    # Latest Microsoft Edge (Mac)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",    
    # Google Chrome (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",    
    # Google Chrome (Mac)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Mozilla Firefox (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",    
    # Mozilla Firefox (Mac)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",    
    # Apple Safari (Mac)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15"
]

NEWS_WEBSITES = [
#   {
#       "name": "Convoca",
#       "site": "convoca.pe",
#   },
  {
      "name": "El Comercio",
      "site": "elcomercio.pe",
  }
#   {
#       "name": "Gestión",
#       "site": "gestion.pe",
#   },
#   {
#       "name": "IDL Reporteros",
#       "site": "idl-reporteros.pe",
#   },
#   {
#       "name": "La República",
#       "site": "larepublica.pe",
#   },
#   {
#       "name": "Perú 21",
#       "site": "peru21.pe",
#   }
]
KEYWORDS_LIST = ['denuncia']
#KEYWORDS_LIST = ['denuncia', 'delito', 'corrupción', 'soborno', 'lavado de activos', 'financiamiento del terrorismo', 'financiamiento a la proliferación de armas de destrucción masiva']
MAX_SEARCH_RESULTS = 100
MAX_RETRIES = 5
RETRY_DELAY = 1
EXPONENTIAL_BACKOFF = 2
REQUEST_SLEEP_TIME = 1.5

GOOGLE_SEARCH = "GOOGLE_SEARCH"
NEWS_SEARCH = "NEWS_SEARCH"