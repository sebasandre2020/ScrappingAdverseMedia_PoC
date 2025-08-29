import aiohttp.client_exceptions
import pytz
from datetime import datetime, timedelta, timezone
import re
import json
import logging
import pandas as pd
from async_ip_rotator import ClientSession
import asyncio
import aiohttp
import ssl
from bs4 import BeautifulSoup
from lxml import etree
from utils import request_status_counter
from collections import defaultdict

# Search a given entity by keyword
async def web_scraper_elcomercio_by_keyword(sess: ClientSession, keyword_empresa: str, keyword: str, ahora_en_lima: datetime, num_results: int, dataframe_global: list, _xpath_base: str, _xpath_titulo: str, _xpath_fecha: str, _xpath_enlace: str, dict_tracker):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    try:
        # URL
        url = "https://www.google.com/search"

        params = {                    
            "q": "allintext:" + '"' + keyword_empresa + '"' + "+" + '"' + keyword + '"' + " " + "site:elcomercio.pe",
            "num": num_results,
            "tbs": f"cd_min:01/01/2019",
            "start": 0,
        }

        # Headers
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }

        while True:
            try:
                # response= session.get(base_url, params=params, headers=headers, verify=False)
                response_news = await sess.get(url, params=params, headers=headers, ssl=ssl_context)
                response_news_text = await response_news.text()
                request_status_counter.agregar_o_actualizar(dict_tracker, str(response_news.status))

                if response_news.status == 200:
                    soup = BeautifulSoup(response_news_text, 'html.parser')

                    # Ver si se llegó al final
                    search_results = soup.select(".tF2Cxc")
                    if not search_results:
                        print("No hay más resultados.")
                        break

                    content = etree.HTML(str(soup))
                    if content is None:
                        return []
                    xpath_objeto = content.xpath(_xpath_base)
                    content = xpath_objeto if isinstance(xpath_objeto, list) else [xpath_objeto]
                    
                else:
                    content = []
                
                for resultado in content:
                    if resultado.xpath(_xpath_titulo):
                        title = resultado.xpath(_xpath_titulo)[0].xpath('string(.)')
                        patron_enlace = r'https?://[^\s›]+'
                        title = re.sub(patron_enlace, '', title)
                    else: 
                        title = ''

                    date = resultado.xpath(_xpath_fecha)[0].xpath('string(.)') if resultado.xpath(_xpath_fecha) else ''
                    if "ago" in str(date):
                        # si es day o days
                        if "days" in str(date) or "day" in str(date):                                                        
                            dias = int(str(date).split()[0])                                                        
                            fecha_calculada = ahora_en_lima - timedelta(days=dias)     
                            date = str(fecha_calculada.strftime('%Y-%m-%d'))                                                                                                           
                            # si es horas o hora
                        elif "hours" in str(date) or "hour" in str(date):                                                        
                            horas = int(str(date).split()[0])
                            fecha_calculada = ahora_en_lima - timedelta(hours=horas)
                            date = str(fecha_calculada.strftime('%Y-%m-%d'))
                            # si es minuto o minutos
                        elif "minutes" in str(date) or "minute" in str(date):                                                        
                            minutos = int(str(date).split()[0])
                            fecha_calculada = ahora_en_lima - timedelta(minutes=minutos)
                            date = str(fecha_calculada.strftime('%Y-%m-%d'))    
                        else:
                            date = ''                                                  
                    elif str(date) != '':
                        try:
                            date_obj = datetime.strptime(str(date), "%b %d, %Y")
                            date = date_obj.strftime("%Y-%m-%d")
                        except:
                            date = ''

                    if date != '' and int(date[:4]) < datetime.now().year - 5:
                        pass                            
                    else:
                        form_data = {
                            'Fuente': "El Comercio",
                            'Titulo': title,
                            'Fecha': date,
                            'URL':  resultado.xpath(_xpath_enlace)[0] if resultado.xpath(_xpath_enlace) else '',                                            
                            'Keyword': keyword, 
                            "RequestStatus": "200",
                            "DescripcionStatus": "OK"                                                                                                      
                        }                        
                        dataframe_global.append(form_data)
                params["start"] += num_results                 
            except Exception as e:
                logging.error(f"Error in fetch_news loop: {e}")
                continue
    except aiohttp.client_exceptions.ClientError as e:
        print(f"POST request failed: {e}")

# Search a given entity by keyword concurrently
async def web_scraper_elcomercio_batch(sess: ClientSession, keyword_empresa: str, secondary_keyword_empresa: str, entityIdNumber: str):
    dict_tracker = defaultdict(request_status_counter.default_value)
  
    keywords_list = ['denuncia', 'delito', 'corrupción', 'soborno', 'lavado de activos', 'financiamiento del terrorismo', 'financiamiento a la proliferación de armas de destrucción masiva']
    
    lima_timezone = pytz.timezone('America/Lima')
    ahora_en_lima = datetime.now(lima_timezone)
    dataframe_global = []
    _xpath_base = '//div[@class="N54PNb BToiNc"]'
    _xpath_titulo = './/h3[@class="LC20lb MBeuO DKV0Md"]'
    _xpath_enlace = './/a[@jsname="UWckNb"]/@href'
    _xpath_fecha = './/span[@class="LEwnzc Sqrs4e"]/span'
    num_results = 50
    
    try:
        logging.info(f'Starting "El Comercio" search process for entity "{keyword_empresa}"')
        tasks = [web_scraper_elcomercio_by_keyword(sess, keyword_empresa, keyword, ahora_en_lima, num_results, dataframe_global, _xpath_base, _xpath_titulo, _xpath_fecha, _xpath_enlace, dict_tracker) for keyword in keywords_list]
        await asyncio.gather(*tasks)
        logging.info(f'Requests status counter in "El Comercio" from "{keyword_empresa}": {dict_tracker}')
        logging.info(f'Successfully finishing "El Comercio" search process for entity "{keyword_empresa}"')
        df = pd.DataFrame(dataframe_global)
        #df.to_csv('data.csv',index=False)
        data_list = df.to_dict(orient='records')
        return {
            "entityIdNumber": entityIdNumber,
            "name": keyword_empresa,
            "commercialName": secondary_keyword_empresa,
            "requestStatus": 200,
            "results": data_list,
            'createdOn': datetime.now(timezone.utc).isoformat(),
            'updatedOn': datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logging.error(f"Finishing with errors: {str(e)}")
        return {
            "entityIdNumber": entityIdNumber,
            "name": keyword_empresa,
            "commercialName": secondary_keyword_empresa,
            "requestStatus": 400,
            "results": [],
            'createdOn': datetime.now(timezone.utc).isoformat(),
            'updatedOn': datetime.now(timezone.utc).isoformat()
        }
