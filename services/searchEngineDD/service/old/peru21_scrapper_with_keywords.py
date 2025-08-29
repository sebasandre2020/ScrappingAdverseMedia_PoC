import aiohttp.client_exceptions
import pytz
from datetime import datetime, timedelta, timezone
import re
import json
import logging
import pandas as pd
from async_ip_rotator import IpRotator, ClientSession
import asyncio
import aiohttp
import ssl
from utils import request_status_counter
from collections import defaultdict

# Search a given entity by keyword
async def web_scraper_peru21_by_keyword(sess: ClientSession, keyword_empresa: str, keyword: str, ahora_en_lima: datetime, meses_es_a_en: dict[str, str], dataframe_global: list, dict_tracker):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    try:
        # URL
        url = "https://peru21.pe/buscar/"

        params = {
            "q": '"' + keyword_empresa + '"' + " " + '"' +keyword + '"'
        }

        # Headers
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }

        try:
            async with aiohttp.ClientSession(headers=headers) as aiohttp_session:
                async with aiohttp_session.get(url, params=params, ssl=ssl_context) as res:
                    response_status = res.status
                    response_url = str(res.url)
                    response = await res.text()
            
            # Llamado para obtener el cse_tok
            url_cse =  "https://cse.google.com/cse.js"
            # Parámetros de la consulta
            params_cse = {
                "cx": "7649cc61549d84f49"
            }

            headers_cse = {
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-US,en;q=0.9,es-PE;q=0.8,es;q=0.7",
                "Connection": "keep-alive",
                "Cookie": "__Secure-3PSID=g.a000rAgp4orkFDJktu7LEE-VLTENwhy8ItWYrJCsHXex_muB97ly92wyeANklhaUTHcg7oNoowACgYKAW4SARASFQHGX2MicZ1hhqGkdtTMFB6qg0rkchoVAUF8yKo9aKMVTLWIJW3WCOukfOJd0076; __Secure-3PAPISID=PuMPbkD-eQL7PcvS/AaP4iU91o79PZGtpi; NID=520=ZApW0HBZ36Pfd9rt_NW1GGLTcD7rJ-cDfeNIeYWEMPrPeBBDdBjzDITnmWoY_UuP8Y2J8V9AXxK23CYqfZ5fzNb-wlWwNQgc89SFQ61ymob3gcSNkWOqmeNwRIhBdxDUQ4LtAg7VwVajjSNctnnzBvRCWPX1L5HrK4Pvy6AS8Gh7vKjAs9B6E0cYUomdT2rKnaj7yWSWZJs3nBIJbwx1xFkckz6f8c7MW_Fd__Mm9vHxTuDRk28s3zhjbMysTrDC0yZhuHTg3z_K4RFDnzE5CzYnlWUNU3lTjZD9tDLsvE8qojmEQ9vzVrVsQJNTfhHKNzFwpZhO2yEKAjUYcRUmHb0z-KzcVu4gSxyKR1350IFQc1KbGPumIoePX4B6OxdXjyaE_mUpNk5LSX3_ia9ecez9wc0znJek7bN395hJiESLHGJjL0Br6gapZMbHonChgCJRAx3PnlTGqIqhHVwHWlE9BOQTx63B-CYFsjwzlZzJ6sBgzojyhZR2wZbfXpzuJ_98erR6XzBnPLiisXj0dRpNdX7kYKncpsGzSTdLIvPO9mOmR3fyH5LJfmyGi-6JBF2KG3P2KhXAaBJzfgrtWoS1COkvzwFwjOUozRCYn_JG5nGB_WrR2jHkmqRn_6q2BzsW6SQKkMXzzNfrfTRswURCUNCf8d_JCbmGw-sjFqGjiWuLN-E; __Secure-3PSIDCC=AKEyXzVI0VmM4el_hON2bmxGtfP-XrhxjHhxUCroLr169ObjctwXnHxepRniqaxWwq8gPOBW_g",
                "Host": "cse.google.com",
                "Referer": "https://peru21.pe/",
                "Sec-CH-UA": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
                "Sec-CH-UA-Mobile": "?0",
                "Sec-CH-UA-Platform": "\"Windows\"",
                "Sec-Fetch-Dest": "script",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "cross-site",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
            }

            async with aiohttp.ClientSession(headers=headers_cse) as aiohttp_session:
                async with aiohttp_session.get(url_cse, params=params_cse, ssl=ssl_context) as res:
                    response_cse_status = res.status
                    response_cse = await res.text()

            # response_cse = requests.get(url_cse,params=params_cse, headers=headers_cse,verify=False)

            match = re.search(r'"cse_token":\s*"([^"]+)"', response_cse)                    
            if match:
                cse_token = match.group(1)                        
            else:
                print("No se encontró el cse_token")               

            if response_status == 200 and response_cse_status == 200:                
                page_number = 1
                while 1:        
                    start_value = (page_number - 1) * 10
                    url_news = "https://cse.google.com/cse/element/v1"
                    params_news = {
                        "rsz": "10",
                        "num": "10",
                        "hl": "es",
                        "source": "gcsc",
                        "cselibv": "8fa85d58e016b414",
                        "cx": "7649cc61549d84f49",
                        "q": '"' + keyword_empresa + '"' + " " + '"' +keyword + '"',
                        "safe": "off",
                        "cse_tok": cse_token,
                        "lr": "",
                        "cr": "",
                        "gl": "",
                        "filter": "1",
                        "sort": "date",
                        "as_oq": "",
                        "as_sitesearch": "",
                        "exp": "cc",
                        "fexp": "72801196,72801194,72801195",
                        "callback": "google.search.cse.api5248",
                        "rurl": response_url,
                        "start": str(start_value)
                    }            
                    # response_news = session.get(url_news, params=params_news, headers=headers, verify=False)
                    response_news = await sess.get(url_news, params=params_news, headers=headers, ssl=ssl_context)
                    response_news_text = await response_news.text()
                    request_status_counter.agregar_o_actualizar(dict_tracker, str(response_news.status))
                    
                    if response_news.status == 200:                                    
                        match = re.search(r'google\.search\.cse\.api5248\((\{.*\})\)', response_news_text, re.DOTALL)
                        if match:
                            # Extraemos el JSON dentro de los paréntesis
                            json_data = match.group(1)
                            try:
                                # Convertir el JSON a un objeto de Python
                                data = json.loads(json_data)
                                try:
                                    result_list = data["results"]                                                        
                                    for result in result_list:                             
                                        match = re.match(r'^[^.]*', result["contentNoFormatting"])
                                        if match:
                                            date = match.group(0)
                                            date = date.strip()                                                                               
                                        else:
                                            print("No se encontró texto antes del primer punto.")
                                        try:
                                            title = result["richSnippet"]["metatags"]["twitterTitle"]
                                        except:
                                            title = result["title"] 
                                                            
                                        if "hace" in str(date):
                                            # si es dias o dia
                                            if "días" in str(date) or "día" in str(date):                                                        
                                                dias = int(str(date).split()[1])                                                        
                                                fecha_calculada = ahora_en_lima - timedelta(days=dias)     
                                                date = str(fecha_calculada.strftime('%d/%m/%Y'))                                                                                                           
                                                # si es horas o hora
                                            elif "horas" in str(date) or "hora" in str(date):                                                        
                                                horas = int(str(date).split()[1])
                                                fecha_calculada = ahora_en_lima - timedelta(hours=horas)
                                                date = str(fecha_calculada.strftime('%d/%m/%Y'))
                                            elif "minutos" in str(date) or "minuto" in str(date):                                                        
                                                minutos = int(str(date).split()[1])
                                                fecha_calculada = ahora_en_lima - timedelta(minutes=minutos)
                                                date = str(fecha_calculada.strftime('%d/%m/%Y'))                                            
                                        else:
                                            dia = str(date).split()[0]
                                            mes = str(date).split()[1]
                                            anho = str(date).split()[2]
                                            if mes in meses_es_a_en:
                                                mes = meses_es_a_en[mes]
                                            date = dia + "/" + mes + "/" + anho
                                                        
                                        form_data = {
                                            'Fuente': "Peru21",
                                            'Titulo': title,
                                            'Fecha': date,
                                            'URL': result["unescapedUrl"],                                             
                                            'Keyword': keyword,
                                            "RequestStatus": "200",
                                            "DescripcionStatus": "OK"                                                    
                                        }                                                
                                        if "hace" not in date and int(date[-4:]) < datetime.now().year - 5:                                                                             
                                            break
                                        else:                                                                           
                                            dataframe_global.append(form_data)                                      
                                except:
                                    print("No results: " + keyword + " " + str(page_number))                                            
                                    break                                        
                            except json.JSONDecodeError:
                                print("Error al decodificar el JSON.") 
                        page_number += 1                         
                    else:
                        print(keyword + " " + str(page_number))
                        break                        
        except aiohttp.client_exceptions.ClientError as e:
            print(f"POST request failed: {e}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return json.dumps({"error": f"An error occurred: {str(e)}"})

# Search a given entity by keyword concurrently
async def web_scraper_peru21_batch(sess: ClientSession, keyword_empresa: str, secondary_keyword_empresa: str, entityIdNumber: str):
    dict_tracker = defaultdict(request_status_counter.default_value)

    meses_es_a_en = {
        'ene': '01',
        'feb': '02',
        'mar': '03',
        'abr': '04',
        'may': '05',
        'jun': '06',
        'jul': '07',
        'ago': '08',
        'sept': '09',
        'oct': '10',
        'nov': '11',
        'dic': '12'
    }
    
    keywords_list = ['denuncia', 'delito', 'corrupción', 'soborno', 'lavado de activos', 'financiamiento del terrorismo', 'financiamiento a la proliferación de armas de destrucción masiva']
    
    lima_timezone = pytz.timezone('America/Lima')
    ahora_en_lima = datetime.now(lima_timezone)
    # keywords_list = keyword_empresa.split(',')
    # keywords_list = [item.strip() for item in keywords_list]
    dataframe_global = []
    try:
        logging.info(f'Starting "Peru21" search process for entity "{keyword_empresa}"')
        tasks = [web_scraper_peru21_by_keyword(sess, keyword_empresa, keyword, ahora_en_lima, meses_es_a_en, dataframe_global, dict_tracker) for keyword in keywords_list]
        await asyncio.gather(*tasks)
        logging.info(f'Successfully finishing "Peru21" search process for entity "{keyword_empresa}"')
        logging.info(f'Requests status counter in "Peru21" from "{keyword_empresa}": {dict_tracker}')
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


# Search a given entity by keyword one by one keyword
async def web_scraper_peru21(sess: ClientSession, keyword_empresa: str):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    meses_es_a_en = {
        'ene': '01',
        'feb': '02',
        'mar': '03',
        'abr': '04',
        'may': '05',
        'jun': '06',
        'jul': '07',
        'ago': '08',
        'sept': '09',
        'oct': '10',
        'nov': '11',
        'dic': '12'
    }
    keywords_list = ["Anticompetencia", 
                    "Arbitraje Comercial", 
                    "Arbitraje de inversión", 
                    "Ciberataque", 
                    "Cohecho", 
                    "Coima", 
                    "Colaborador eficaz", 
                    "Colusión", 
                    "Concesión", 
                    "Controversia arbitral", 
                    "Corrupción", 
                    "Demanda arbitral", 
                    "Disputa en arbitraje", 
                    "Extorsión", 
                    "Financiamiento y Terrorismo", 
                    "Fiscalía", 
                    "Fraude", 
                    "Fusión", 
                    "Ilícito", 
                    "Laudo", 
                    "Lavado de activos", 
                    "Libre Competencia", 
                    "Litigio arbitral", 
                    "Medida cautelar", 
                    "Organización criminal", 
                    "Proceso administrativo sancionador", 
                    "Resolución de contrato", 
                    "Soborno", 
                    "Trafico de influencias", 
                    "Tribunal CCI"] 
    
    lima_timezone = pytz.timezone('America/Lima')
    ahora_en_lima = datetime.now(lima_timezone)
    # keywords_list = keyword_empresa.split(',')
    # keywords_list = [item.strip() for item in keywords_list]
    dataframe_global = []

    for keyword in keywords_list:
        try:
            # URL
            url = "https://peru21.pe/buscar/"

            params = {
                "q": keyword_empresa + " " +keyword
            }

            # Headers
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
            }

            try:
                async with aiohttp.ClientSession(headers=headers) as aiohttp_session:
                    async with aiohttp_session.get(url, params=params, ssl=ssl_context) as res:
                        response_status = res.status
                        response_url = str(res.url)
                        response = await res.text()
                
                # Llamado para obtener el cse_tok
                url_cse =  "https://cse.google.com/cse.js"
                # Parámetros de la consulta
                params_cse = {
                    "cx": "7649cc61549d84f49"
                }

                headers_cse = {
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip, deflate, br, zstd",
                    "Accept-Language": "en-US,en;q=0.9,es-PE;q=0.8,es;q=0.7",
                    "Connection": "keep-alive",
                    "Cookie": "__Secure-3PSID=g.a000rAgp4orkFDJktu7LEE-VLTENwhy8ItWYrJCsHXex_muB97ly92wyeANklhaUTHcg7oNoowACgYKAW4SARASFQHGX2MicZ1hhqGkdtTMFB6qg0rkchoVAUF8yKo9aKMVTLWIJW3WCOukfOJd0076; __Secure-3PAPISID=PuMPbkD-eQL7PcvS/AaP4iU91o79PZGtpi; NID=520=ZApW0HBZ36Pfd9rt_NW1GGLTcD7rJ-cDfeNIeYWEMPrPeBBDdBjzDITnmWoY_UuP8Y2J8V9AXxK23CYqfZ5fzNb-wlWwNQgc89SFQ61ymob3gcSNkWOqmeNwRIhBdxDUQ4LtAg7VwVajjSNctnnzBvRCWPX1L5HrK4Pvy6AS8Gh7vKjAs9B6E0cYUomdT2rKnaj7yWSWZJs3nBIJbwx1xFkckz6f8c7MW_Fd__Mm9vHxTuDRk28s3zhjbMysTrDC0yZhuHTg3z_K4RFDnzE5CzYnlWUNU3lTjZD9tDLsvE8qojmEQ9vzVrVsQJNTfhHKNzFwpZhO2yEKAjUYcRUmHb0z-KzcVu4gSxyKR1350IFQc1KbGPumIoePX4B6OxdXjyaE_mUpNk5LSX3_ia9ecez9wc0znJek7bN395hJiESLHGJjL0Br6gapZMbHonChgCJRAx3PnlTGqIqhHVwHWlE9BOQTx63B-CYFsjwzlZzJ6sBgzojyhZR2wZbfXpzuJ_98erR6XzBnPLiisXj0dRpNdX7kYKncpsGzSTdLIvPO9mOmR3fyH5LJfmyGi-6JBF2KG3P2KhXAaBJzfgrtWoS1COkvzwFwjOUozRCYn_JG5nGB_WrR2jHkmqRn_6q2BzsW6SQKkMXzzNfrfTRswURCUNCf8d_JCbmGw-sjFqGjiWuLN-E; __Secure-3PSIDCC=AKEyXzVI0VmM4el_hON2bmxGtfP-XrhxjHhxUCroLr169ObjctwXnHxepRniqaxWwq8gPOBW_g",
                    "Host": "cse.google.com",
                    "Referer": "https://peru21.pe/",
                    "Sec-CH-UA": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
                    "Sec-CH-UA-Mobile": "?0",
                    "Sec-CH-UA-Platform": "\"Windows\"",
                    "Sec-Fetch-Dest": "script",
                    "Sec-Fetch-Mode": "no-cors",
                    "Sec-Fetch-Site": "cross-site",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
                }

                async with aiohttp.ClientSession(headers=headers_cse) as aiohttp_session:
                    async with aiohttp_session.get(url_cse, params=params_cse, ssl=ssl_context) as res:
                        response_cse_status = res.status
                        response_cse = await res.text()

                # response_cse = requests.get(url_cse,params=params_cse, headers=headers_cse,verify=False)

                match = re.search(r'"cse_token":\s*"([^"]+)"', response_cse)                    
                if match:
                    cse_token = match.group(1)                        
                else:
                    print("No se encontró el cse_token")               

                if response_status == 200 and response_cse_status == 200:                
                    page_number = 1
                    while 1:        
                        start_value = (page_number - 1) * 10
                        url_news = "https://cse.google.com/cse/element/v1"
                        params_news = {
                            "rsz": "10",
                            "num": "10",
                            "hl": "es",
                            "source": "gcsc",
                            "cselibv": "8fa85d58e016b414",
                            "cx": "7649cc61549d84f49",
                            "q": keyword_empresa + " " +keyword,
                            "safe": "off",
                            "cse_tok": cse_token,
                            "lr": "",
                            "cr": "",
                            "gl": "",
                            "filter": "1",
                            "sort": "date",
                            "as_oq": "",
                            "as_sitesearch": "",
                            "exp": "cc",
                            "fexp": "72801196,72801194,72801195",
                            "callback": "google.search.cse.api5248",
                            "rurl": response_url,
                            "start": str(start_value)
                        }            
                        # response_news = session.get(url_news, params=params_news, headers=headers, verify=False)
                        response_news = await sess.get(url_news, params=params_news, headers=headers, ssl=ssl_context)
                        response_news_text = await response_news.text()
                        
                        if response_news.status == 200:                                    
                            match = re.search(r'google\.search\.cse\.api5248\((\{.*\})\)', response_news_text, re.DOTALL)
                            if match:
                                # Extraemos el JSON dentro de los paréntesis
                                json_data = match.group(1)
                                try:
                                    # Convertir el JSON a un objeto de Python
                                    data = json.loads(json_data)
                                    try:
                                        result_list = data["results"]                                                        
                                        for result in result_list:                             
                                            match = re.match(r'^[^.]*', result["contentNoFormatting"])
                                            if match:
                                                date = match.group(0)
                                                date = date.strip()                                                                               
                                            else:
                                                print("No se encontró texto antes del primer punto.")
                                            try:
                                                title = result["richSnippet"]["metatags"]["twitterTitle"]
                                            except:
                                                title = result["title"] 
                                                                
                                            if "hace" in str(date):
                                                # si es dias o dia
                                                if "días" in str(date) or "día" in str(date):                                                        
                                                    dias = int(str(date).split()[1])                                                        
                                                    fecha_calculada = ahora_en_lima - timedelta(days=dias)     
                                                    date = str(fecha_calculada.strftime('%d/%m/%Y'))                                                                                                           
                                                    # si es horas o hora
                                                elif "horas" in str(date) or "hora" in str(date):                                                        
                                                    horas = int(str(date).split()[1])
                                                    fecha_calculada = ahora_en_lima - timedelta(hours=horas)
                                                    date = str(fecha_calculada.strftime('%d/%m/%Y'))
                                                elif "minutos" in str(date) or "minuto" in str(date):                                                        
                                                    minutos = int(str(date).split()[1])
                                                    fecha_calculada = ahora_en_lima - timedelta(minutes=minutos)
                                                    date = str(fecha_calculada.strftime('%d/%m/%Y'))                                            
                                            else:
                                                dia = str(date).split()[0]
                                                mes = str(date).split()[1]
                                                anho = str(date).split()[2]
                                                if mes in meses_es_a_en:
                                                    mes = meses_es_a_en[mes]
                                                date = dia + "/" + mes + "/" + anho
                                                            
                                            form_data = {
                                                'Fuente': "Peru21",
                                                'Titulo': title,
                                                'Fecha': date,
                                                'URL': result["unescapedUrl"],                                             
                                                'Keyword': keyword,
                                                "RequestStatus": "200",
                                                "DescripcionStatus": "OK"                                                    
                                            }                                                
                                            if "hace" not in date and int(date[-4:]) < datetime.now().year - 5:                                                                             
                                                break
                                            else:                                                                           
                                                dataframe_global.append(form_data)                                      
                                    except:
                                        print("No results: " + keyword + " " + str(page_number))                                            
                                        break                                        
                                except json.JSONDecodeError:
                                    print("Error al decodificar el JSON.") 
                            page_number += 1                         
                        else:
                            print(keyword + " " + str(page_number))
                            break                        
            except aiohttp.client_exceptions.ClientError as e:
                print(f"POST request failed: {e}")

        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            return json.dumps({"error": f"An error occurred: {str(e)}"})

        await asyncio.sleep(3)
    df = pd.DataFrame(dataframe_global)
    #df.to_csv('data.csv',index=False)
    data_list = df.to_dict(orient='records')  
    json_data = json.dumps(data_list, ensure_ascii=False)
    return json_data