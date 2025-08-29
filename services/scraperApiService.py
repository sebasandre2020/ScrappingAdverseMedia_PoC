import json
import os
import logging
import requests
import asyncio
import aiohttp
from utils.constants import NEWS_SEARCH

class ScraperApiService:
    __API_KEY = os.environ.get("SCRAPER_API_KEY")

    # private methods
    def _getKeys(self):
        keys: str = os.environ.get("SCRAPER_API_KEY")
        if(keys is None):
            return []
            
        return keys.split(',')
    
    def _setNewApiKey(self):
        keys: list = self._getKeys()
        for key in keys:
            # TODO: validar si el current key está agotado
            self.__API_KEY = key
            return key

    def _get_structured_data_request(self, keyword: str, search:str):
        url = f'https://api.scraperapi.com/structured/google/search?api_key={self.__API_KEY}&country_code=PE&query={search}'
        payload = {
            'api_key': self.__API_KEY,
            'query': keyword,
            'output_format': 'json',
            'autoparse': 'true',
            'num': '100'
        }
        return url, payload
    
    def _get_api_request(self, web_url: str):
        url = 'https://api.scraperapi.com/' 
        payload = {
            'api_key': self.__API_KEY,
            'url': web_url,
            'output_format': 'json',
            'autoparse': 'true'
        }
        return url, payload
    
    def _send_scraper_api_request(self, url: str, payload: dict[str, object]):
        try:
            logging.info(f'REQUEST AL SCRAPER API: {payload}')
            r = requests.get(url, params = payload, verify=False)
            res = self._parseNewsResponse(r.text)
            logging.info(r.text)
        except Exception as e:
            logging.error(f"An error has ocurred in <Adverse Media> using ScraperAPI: {e}")
            return {
                'status': 'Error',
                'description': str(e),
                'url': url,
                'content': None
            }, url
        
        return res, url

    def _parseNewsResponse(self, response: str):
        result = json.loads(response)
        return result
    
    def _format_name(self, text: str):
        position = text.find('"')
        if position != -1:
            return text[:position].replace("+", " ")
        return text.replace("+", " ")
    
    def _parse_result(self, result, keywords: tuple):
        json_response = json.loads(result)

        if 'organic_results' not in result:
            msg = 'No Organic results from Scraper'

            if('query_result_mismatch_message' in result):
                msg = json_response['search_information']['query_result_mismatch_message']
            
            elif('error' in json_response):
                msg = json_response['error']
            
            logging.error(f'No results from ScraperApi for {keywords[1]} "{keywords[0]}" - {msg}')
            return []        

        results = [
            {
                'Titulo': str(res['title']).replace("\"","'"),
                'Resumen': str(res['snippet']).replace("\"","'"),
                'URL': res['link'],
                'KeyWord': keywords[0],
                'Nombre o Razón Social': keywords[1],
                'RequestStatus': 200,
                'DescriptionStatus': 'Ok'
            } for res in json_response['organic_results']
        ]
        return results
    
    def _parse_news_result(self, result, query):
        json_response = json.loads(result)

        if 'organic_results' not in result:
            msg = 'No Organic results from Scraper'

            if('query_result_mismatch_message' in result):
                msg = json_response['search_information']['query_result_mismatch_message']

            elif('error' in json_response):
                msg = json_response['error']
            
            site = query[2]['name']
            logging.error(f'No results from ScraperApi for "{query[1]}" "{query[0]}" site:{site} - {msg}')
            return []

        results = [
            {
                'Fuente': query[2]['name'],
                'Sitio': f"https://{query[2]['site']}",
                'Nombre o Razón Social': query[1],
                'Titulo': str(res['title']).replace("\"","'"),
                'Fecha': str(res['snippet']).replace("\"","'"),
                'URL': res['link'],
                'Keyword': query[0],
                'RequestStatus': 200,
                'DescriptionStatus': "Ok"
            } for res in json_response['organic_results']
        ]
        return results

    # Async methods
    def _get_structured_data_request_post(self, queries: list):
        url = 'https://async.scraperapi.com/structured/google/search'
        payload = {
            'api_key': self.__API_KEY,
            'queries': queries
        }
        return url, payload

    def _send_async_scraper_api_request(self, url: str, payload: dict[str, object]):
        headers = {
            "Content-Type": "application/json"
        }
        try:
            logging.info(f'REQUEST AL SCRAPER API: {payload}')
            r = requests.post(url, json=payload, headers=headers, verify=False)
            res = self._parseNewsResponse(r.text)
            logging.info(res)
        except Exception as e:
            logging.error(f"An error has ocurred in <Adverse Media> using ScraperAPI: {e}")
            return {
                'status': 'Error',
                'description': str(e),
                'url': url,
                'content': None
            }, url
        
        return res, url
    
    async def _fetch(self, session: aiohttp.ClientSession, url, params):
        async with session.get(url) as response:
            return await response.text()
    
    async def execute_requests(self, search_queries: list):
        #print("Entró al _build_async_requests")
        try:
            async with aiohttp.ClientSession() as session:
                tasks = []
                for query in search_queries:
                    search = f'"{query[1]}" "{query[0]}"'
                    url, payload = self._get_structured_data_request(query[0], search)
                    tasks.append(self._fetch(session, url, payload))
                results = await asyncio.gather(*tasks, return_exceptions=True)
                response = []
                for query, result in zip(search_queries, results):
                    new_res = self._parse_result(result, query)
                    logging.info(f"Results for '{query}': {len(new_res)}")
                    response.extend(new_res)
                return response

        except Exception as e:
            logging.error(f"Unexpected error in <Adverse Media>: {e}")
            return []

    async def news_execute_requests(self, search_queries: list):
        try:        
            async with aiohttp.ClientSession() as session:
                tasks = []
                for query in search_queries:
                    site = query[2]['site'] 
                    search = f'"{query[1]}" "{query[0]}" site:{site}'
                    url, payload = self._get_structured_data_request(query[0], search)
                    tasks.append(self._fetch(session, url, payload))
                results = await asyncio.gather(*tasks, return_exceptions=True)
                response = []
                for query, result in zip(search_queries, results):
                    name = query[2]['name'] 
                    logging.info(f"Results for <Adverse Media News - '{name}'>:")
                    #print(result)
                    new_res = self._parse_news_result(result, query)
                    response.extend(new_res)
                return response
        
        except Exception as e:
            logging.error(f"Unexpected error in <Adverse Media News>: {e}")
            return []

    # public methods
    def get_from_google(self, keywords: tuple[str, str]):
        url, payload = self._get_structured_data_request(keywords[1])
        logging.info(f'Request (get_from_google): {url} - {payload} - {keywords[0]}')
        response = self._send_scraper_api_request(url, payload)
        return response, keywords[0]
    
    def get_from_news(self, keywords: tuple[str, str]):
        return self._scraper_api_request(keywords, NEWS_SEARCH)

    def get_from_google_async(self, keywords: list):
        queries = [keyword[1] for keyword in keywords]

        url, payload = self._get_structured_data_request_post(queries)
        logging.info(f'Request (get_from_google_async): {url} - {payload} - {queries}')
        try:
            response = self._send_async_scraper_api_request(url, payload)
        except Exception as e:
            logging.error(f"An error has ocurred in <get_from_google_async> using ScraperAPI: {e}")
        
        return response, keywords[0]