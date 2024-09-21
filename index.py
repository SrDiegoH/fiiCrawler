from flask import Flask, jsonify
import requests
from requests import RequestException
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import os
import re

app = Flask(__name__)

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

FUNDSEXPLORER_SOURCE = 'fundsexplorer'
FIIS_SOURCE = 'fiis'

def request_fii_by(ticker, source):
    if source == FUNDSEXPLORER_SOURCE:
        return get_data_from_fundsexplorer_by(ticker)
    elif source == FIIS_SOURCE:
        return get_data_from_fiis_by(ticker)

    return get_data_from_fundamentus_by(ticker)

def get_data_from_fundamentus_by(ticker):
    url = f'https://fundamentus.com.br/detalhes.php?papel={ticker}'

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://fundamentus.com.br/index.php',
        'Origin': 'https://fundamentus.com.br/index.php',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0'
    }
    response = requests.get(url, headers=headers)

    html = response.text

    return html

def convert_fundamentus_data(data):
    return {
        "fundname": get_substring(data, 'Nome</span>', '</span>'),
        "setor_atuacao": None,
        "segmento_ambima":get_substring(data, 'Mandato</span>', '</span>'),
        "valor_caixa":  get_substring(get_substring(data, 'Caixa\',', '}', False), '[', ']', False),
        "valor": get_substring(data, 'Cotação</span>', '</span>'),
        "liquidezmediadiaria": get_substring(data, 'Vol $ méd (2m)</span>', '</span>'),
        "pvp": get_substring(data, 'P/VP</span>', '</span>'),
        "dy": get_substring(data, 'Div. Yield</span>', '</span>'),
        "dividendos_12_meses": get_substring(data, 'Rend. Distribuído</span>', '</span>')),
        "lastdividend": get_substring(data, 'Dividendo/cota</span>', '</span>'),
        "patrimonio": get_substring(data, 'Patrim Líquido</span>', '</span>'),
        "valorizacao_12_meses": get_substring(data, '12 meses</span>', '</span>'),
        "valorizacao_mes": get_substring(data, 'Mês</span>', '</span>'),
        "min_52_semanas": get_substring(data, 'Min 52 sem</span>', '</span>'),
        "max_52_semanas": get_substring(data, 'Max 52 sem</span>', '</span>'),
        "assets_number": get_substring(data, 'Qtd imóveis</span>', '</span>'),
        "taxas": None,
        "vacancia": get_substring(data, 'Vacância Média</span>', '</span>'),
        "firstdate": None,
        "numero_cotas": get_substring(data, 'Nro. Cotas</span>', '</span>'),
        "valormercado": get_substring(data, 'Valor de mercado</span>', '</span>'),
        "publicoalvo": None,
        "prazoduracao": None,
        "gestao": get_substring(data, 'Gestão</span>', '</span>'),
        "ffoy": get_substring(data, 'FFO Yield</span>', '</span>'),
        "link": get_substring(data, 'abrirGerenciadorDocumentosCVM?cnpjFundo=', '">Pesquisar Documentos', False),
        "ativos": get_substring(data, '>Ativos</span>', '</span>')
    }

def get_data_from_fundsexplorer_by(ticker):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'DNT': '1',
        'Priority': 'u=0, i',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0'
    }

    response = request_get(f'https://www.fundsexplorer.com.br/funds/{ticker}', headers)

    fii_as_text = get_substring(response, 'var dataLayer_content =', 'dataLayer.push(')

    fii_as_json = json.loads(fii_as_text.rstrip(';'))['pagePostTerms']['meta']  

    return convert_fund_data(fii_as_json)

def convert_fundsexplorer_data(data):
    return {
        "fundname": data["name"],
        "setor_atuacao": data["setor_atuacao"],
        "segmento_ambima": data["segmento_ambima"],
        "valor_caixa": data["valor_caixa"],
        "valor": data["valor"],
        "liquidezmediadiaria": data["liquidezmediadiaria"],
        "pvp": data["pvp"],
        "dy": data["dy"],
        "dividendos_12_meses": data["dividendos_12_meses"],
        "lastdividend": data["lastdividend"],
        "patrimonio": data["patrimonio"],
        "valorizacao_12_meses": data["valorizacao_12_meses"],
        "valorizacao_mes": data["valorizacao_12_meses"],
        "min_52_semanas": data["min_52_semanas"],
        "max_52_semanas": data["max_52_semanas"],
        "assets_number": data["assets_number"],
        "taxas": data["taxas"],
        "vacancia": data["vacancia"],
        "firstdate": data["firstdate"],
        "numero_cotas": data["numero_cotas"],
        "valormercado": data["valormercado"],
        "publicoalvo": data["publicoalvo"],
        "prazoduracao": data["prazoduracao"],
        "gestao": data["gestao"]
    }

def get_data_from_fiis_by(ticker):
    base_url = "www.fiis.com.br"
    url_path = f"/{ticker}"

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"
    }

    connection = httplib.HTTPSConnection(base_url)
    connection.request(method='POST', url=url_path, body=None, headers=headers)

    response = connection.getresponse().read()

    connection.close()

    web_page = BeautifulSoup(response)

    segment = web_page.select_one("#carbon_fields_fiis_informations-2 > div.moreInfo.wrapper > p:nth-child(7) > b").text.strip()

    return segment

def request_get(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text

def get_substring(text, start_text, end_text, should_remove_tags=True):
    new_text = cut_string_in_begin(text, text.index(start_text))
    end_index = get_index_of_end_text(new_text, start_text, end_text)
    cutted_text = cut_string_in_middle(new_text, len(start_text), end_index)
    return remove_tags(cutted_text, should_remove_tags).strip()

def cut_string_in_begin(text, start_index):
    return text[start_index:]

def cut_string_in_middle(text, start_index, end_index):
    return text[start_index:end_index]

def get_index_of_end_text(text, start_text, end_text):
    return cut_string_in_begin(text, len(start_text)).index(end_text) + len(start_text)

def remove_tags(text, should_remove_tags):
    return re.sub(r'<[^>]*>', '', text) if should_remove_tags else text

def equals_ignore_case(text, compare_text):
    return text.lower() == compare_text.lower()

def read_cache(ticker):
    if not os.path.exists(CACHE_FILE):
        return None

    with open(CACHE_FILE, 'r') as cache_file:
        for line in cache_file:
            if not line.startswith(ticker):
                continue

            cached_ticker, cached_time, data = line.strip().split('#@#')

            cached_datetime = datetime.strptime(cached_time, '%Y-%m-%d %H:%M:%S')

            if datetime.now() - cached_datetime <= CACHE_EXPIRY:
                return data

    return None

def write_to_cache(ticker, data):
    with open(CACHE_FILE, 'a') as cache_file:
        cache_file.write(f"{ticker}#@#{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}#@#{data}\n")

def clear_cache():
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)

@app.route('/fii/<ticker>', methods=['GET'])
def get_fii_data_by(ticker):
    true_values = ('true', '1', 't', 'y', 'yes', 's', 'sim')

    should_clear_cache = request.args.get('should_clear_cache', '0').lower() in true_values
    should_use_cache = request.args.get('should_use_cache', '1').lower() in true_values
    source = request.args.get('source', 'fundamentus').lower()

    if should_clear_cache in true_values:
        clear_cache()

    if should_use_cache:
        cached_data = read_cache(ticker)

        if cached_data:
            return jsonify({'data': cached_data, 'source': 'cache'}), 200

    data = request_fii_by(ticker, source)

    if should_use_cache:
        write_to_cache(ticker, data)

    return jsonify({'data': data, 'source': 'fresh'}), 200

if __name__ == '__main__':
    app.run(debug=True)
