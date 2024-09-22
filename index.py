import http.client as httplib
import json
import os
import re
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
import requests
from requests import RequestException

app = Flask(__name__)

VALID_BOOL_VALUES = ('true', '1', 't', 'y', 'yes', 's', 'sim')

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

FUNDSEXPLORER_SOURCE = 'fundsexplorer'
FIIS_SOURCE = 'fiis'
FUNDAMENTUS_SOURCE = 'fundamentus'

def request_fii_by(ticker, source):
    if source == FUNDSEXPLORER_SOURCE:
        return get_data_from_fundsexplorer_by(ticker)
    elif source == FIIS_SOURCE:
        return get_data_from_fiis_by(ticker)
    elif source == FUNDAMENTUS_SOURCE:
        return get_data_from_fundamentus_by(ticker)

    return get_data_from_all_by(ticker)

def get_data_from_fundamentus_by(ticker):
    url = f'https://fundamentus.com.br/detalhes.php?papel={ticker}'

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://fundamentus.com.br/index.php',
        'Origin': 'https://fundamentus.com.br/index.php',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0'
    }

    response = request_get(url, headers)

    return convert_fundamentus_data(response)

def convert_fundamentus_data(data):
    def generate_link(cnpj):
        return f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={cnpj}#'

    def get_dividends(distributed_dividends, total_quotas):
        return distributed_dividends / total_quotas / 12

    distributed_dividends = float(get_substring(data, 'Rend. Distribuído</span>', '</span>'))
    total_quotas = float(get_substring(data, 'Nro. Cotas</span>', '</span>'))

    return {
        "nome": get_substring(data, 'Nome</span>', '</span>'),
        "gestao": get_substring(data, 'Gestão</span>', '</span>'),
        "tipo": None,
        "segmento": get_substring(data, 'Mandato</span>', '</span>'),
        "atuacao": None,
        "valor_caixa": get_substring(get_substring(data, 'Caixa\',', '}', False), '[', ']', False),
        "valor_ativos": get_substring(data, '>Ativos</span>', '</span>'),
        "valor_mercado": get_substring(data, 'Valor de mercado</span>', '</span>'),
        "valor_patrimonio_liquido": get_substring(data, 'Patrim Líquido</span>', '</span>'),
        "valor_cotacao": get_substring(data, 'Cotação</span>', '</span>'),
        "liquidez": get_substring(data, 'Vol $ méd (2m)</span>', '</span>'),
        "pvp": get_substring(data, 'P/VP</span>', '</span>'),
        "ffoy": get_substring(data, 'FFO Yield</span>', '</span>'),
        "dy": get_substring(data, 'Div. Yield</span>', '</span>'),
        "dividendos_12_meses": get_dividends(distributed_dividends, total_quotas),
        "ultimo_dividendo": get_substring(data, 'Dividendo/cota</span>', '</span>'),
        "valorizacao_12_meses": get_substring(data, '12 meses</span>', '</span>'),
        "valorizacao_ultimo_mes": get_substring(data, 'Mês</span>', '</span>'),
        "min_52_semanas": get_substring(data, 'Min 52 sem</span>', '</span>'),
        "max_52_semanas": get_substring(data, 'Max 52 sem</span>', '</span>'),
        "qnt_imoveis": get_substring(data, 'Qtd imóveis</span>', '</span>'),
        "taxas": None,
        "vacancia": get_substring(data, 'Vacância Média</span>', '</span>'),
        "total_cotas_emitidas": total_quotas,
        "data_inicio": None,
        "publico_alvo": None,
        "prazo": None,
        "link": generate_link(get_substring(data, 'abrirGerenciadorDocumentosCVM?cnpjFundo=', '">Pesquisar Documentos', False))
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

    data_as_text = get_substring(response, 'var dataLayer_content =', 'dataLayer.push(')

    data_as_json = json.loads(data_as_text.rstrip(';'))['pagePostTerms']['meta']

    return convert_fundsexplorer_data(data_as_json)

def convert_fundsexplorer_data(data):
    return {
        "nome": data["name"],
        "gestao": data["gestao"],
        "tipo": data["setor_atuacao"],
        "segmento": data["segmento_ambima"],
        "atuacao": data["segmento_atuacao"],
        "valor_caixa": data["valor_caixa"],
        "valor_mercado": data["valormercado"],
        "valor_patrimonio_liquido": data["patrimonio"],
        "valor_cotacao": data["valor"],
        "liquidez": data["liquidezmediadiaria"],
        "pvp": data["pvp"],
        "ffoy": None,
        "dy": data["dy"],
        "dividendos_12_meses": data["dividendos_12_meses"],
        "ultimo_dividendo": data["lastdividend"],
        "valorizacao_12_meses": data["valorizacao_12_meses"],
        "valorizacao_ultimo_mes": data["valorizacao_mes"],
        "min_52_semanas": data["min_52_semanas"],
        "max_52_semanas": data["max_52_semanas"],
        "qnt_imoveis": data["assets_number"],
        "taxas": data["taxas"],
        "vacancia": data["vacancia"],
        "total_cotas_emitidas": data["numero_cotas"],
        "data_inicio": data["firstdate"],
        "publico_alvo": data["publicoalvo"],
        "prazo": data["prazoduracao"],
        "link": None
    }

def get_data_from_all_by(ticker):
    data_fundamentus = get_data_from_fundamentus_by(ticker)
    data_fundsexplorer = get_data_from_fundsexplorer_by(ticker)

    data_merge = {}

    for key, value in data_fundamentus.items():
        if key in data_fundsexplorer and not value:
            data_merge[key] = data_fundsexplorer[key]
            continue

        data_merge[key] = value

    return data_merge

def request_get(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text

def get_substring(text, start_text, end_text, should_remove_tags=True):
    start_index = text.find(start_text)
    new_text = text[start_index:]

    end_index = new_text[len(start_text):].find(end_text) + len(start_text)
    cutted_text = new_text[len(start_text):end_index].strip().replace('\n', '')

    return re.sub(r'<[^>]*>', '', cutted_text) if should_remove_tags else cutted_text

def read_cache(ticker, should_clear_cache):
    if not os.path.exists(CACHE_FILE):
        return None

    if should_clear_cache:
        clear_cache(ticker)
        return None

    control_clean_cache = False
    with open(CACHE_FILE, 'r') as cache_file:
        for line in cache_file:
            if not line.startswith(ticker):
                continue

            cached_ticker, cached_datetime, data = line.strip().split('#@#')

            cached_date = datetime.strptime(cached_datetime, '%Y-%m-%d %H:%M:%S')

            if datetime.now() - cached_date <= CACHE_EXPIRY:
                return data

            control_clean_cache = True
            break

    if control_clean_cache:
        clear_cache(ticker)

    return None

def clear_cache(ticker):
    with open(CACHE_FILE, 'r') as cache_file:
        lines = cache_file.readlines()

    with open(CACHE_FILE, 'w') as cache_file:
        for line in lines:
            if not line.startswith(ticker):
                cache_file.write(line)

def write_to_cache(ticker, data):
    with open(CACHE_FILE, 'a') as cache_file:
        cache_file.write(f"{ticker}#@#{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}#@#{data}\n")

def delete_cache():
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)

@app.route('/fii/<ticker>', methods=['GET'])
def get_fii_data_by(ticker):
    should_delete_cache = request.args.get('should_delete_cache', '0').lower() in VALID_BOOL_VALUES
    should_clear_cache = request.args.get('should_clear_cache', '0').lower() in VALID_BOOL_VALUES
    should_use_cache = request.args.get('should_use_cache', '1').lower() in VALID_BOOL_VALUES

    source = request.args.get('source', 'all').lower()

    if should_delete_cache:
        delete_cache()

    if should_use_cache and not should_delete_cache:
        cached_data = read_cache(ticker, should_clear_cache)

        if cached_data:
            return jsonify({'data': cached_data, 'source': 'cache'}), 200

    data = request_fii_by(ticker, source)

    if should_use_cache and not should_delete_cache and not should_clear_cache:
        write_to_cache(ticker, data)

    return jsonify({'data': data, 'source': 'fresh'}), 200

if __name__ == '__main__':
    app.run(debug=True)
