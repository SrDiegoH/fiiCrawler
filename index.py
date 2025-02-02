import ast
from datetime import datetime, timedelta
from hashlib import sha512
import json
import os
import re
import traceback

from flask import Flask, jsonify, request

import requests
from requests import RequestException

app = Flask(__name__)
app.json.sort_keys = False

TRUE_BOOL_VALUES = ('1', 's', 'sim', 'y', 'yes', 't', 'true')

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

VALID_SOURCES = {
    'FUNDAMENTUS_SOURCE': 'fundamentus',
    'INVESTIDOR10_SOURCE': 'investidor10',
    'FUNDSEXPLORER_SOURCE': 'fundsexplorer',
    'ALL_SOURCE': 'all'
}

VALID_INFOS = [ 'actuation', 'assets_value', 'cash_value', 'DY', 'equity_price', 'ffoy', 'initial_date', 'latest_dividend', 'latests_dividends', 'link', 'liquidity', 'management', 'market_value', 'max_52_weeks', 'min_52_weeks', 'name', 'net_equity_value', 'price', 'PVP', 'segment', 'target_public', 'term', 'total_issued_shares', 'total_real_state', 'type', 'vacancy', 'variation_12M', 'variation_30D' ]

def request_get(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    #print(f'Response from {url} : {response}')

    return response

def get_substring(text, start_text, end_text, replace_by_paterns=[], should_remove_tags=False):
    start_index = text.find(start_text)
    new_text = text[start_index:]

    end_index = new_text[len(start_text):].find(end_text) + len(start_text)
    cutted_text = new_text[len(start_text):end_index]

    if not cutted_text:
        return None

    clean_text = cutted_text.replace('\n', '').replace('\t', '')

    no_tags_text = re.sub(r'<[^>]*>', '', clean_text) if should_remove_tags else clean_text

    final_text = no_tags_text
    for pattern in replace_by_paterns:
        final_text = final_text.replace(pattern, '')

    return final_text.strip()

def text_to_number(text, should_convert_thousand_decimal_separators=True, convert_percent_to_decimal=False):
    try:
        if not text:
            raise Exception()

        if not isinstance(text, str):
            return text

        text = text.strip()

        if not text.strip():
            raise Exception()

        if should_convert_thousand_decimal_separators:
            text = text.replace('.','').replace(',','.')

        if '%' in text:
            return float(text.replace('%', '').strip()) / (100 if convert_percent_to_decimal else 1)

        if 'R$' in text:
            text = text.replace('R$', '')

        return float(text.strip())
    except:
        return 0

def delete_cache():
    if os.path.exists(CACHE_FILE):
        #print('Deleting cache')
        os.remove(CACHE_FILE)
        #print('Deleted')

def clear_cache(hash_id):
    #print('Cleaning cache')
    with open(CACHE_FILE, 'w+') as cache_file:
        lines = cache_file.readlines()

        for line in lines:
            if not line.startswith(hash_id):
                cache_file.write(line)
   #print('Cleaned')

def read_cache(hash_id, should_clear_cache):
    if not os.path.exists(CACHE_FILE):
        return None, None

    if should_clear_cache:
        clear_cache(hash_id)
        return None, None

    control_clean_cache = False

    #print('Reading cache')
    with open(CACHE_FILE, 'r') as cache_file:
        for line in cache_file:
            if not line.startswith(hash_id):
                continue

            _, cached_datetime, data = line.strip().split('#@#')

            cached_date = datetime.strptime(cached_datetime, '%Y-%m-%d %H:%M:%S')

            #print(f'Found value: Date: {cached_datetime} - Data: {data}')
            if datetime.now() - cached_date <= CACHE_EXPIRY:
                #print('Finished read')
                return ast.literal_eval(data), cached_date

            control_clean_cache = True
            break

    if control_clean_cache:
        clear_cache(hash_id)

    return None, None

def write_to_cache(hash_id, data):
    #print('Writing cache')
    with open(CACHE_FILE, 'a') as cache_file:
        #print(f'Writed value: {f'{hash_id}#@#{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}#@#{data}\n'}')
        cache_file.write(f'{hash_id}#@#{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}#@#{data}\n')
    #print('Writed')

def convert_fundamentus_data(data, info_names):
    patterns_to_remove = [
        '</span>',
        '<span class="txt">',
        '<span class="oscil">',
        '<font color="#F75D59">',
        '<font color="#306EFF">',
        '</td>',
        '<td class="data">',
        '<td class="data w1">',
        '<td class="data w2">',
        '<td class="data w3">',
        '<td class="data destaque w3">',
        '<a href="resultado.php?segmento='
    ]

    def get_vacancy():
        vacancy_as_text = get_substring(data, 'Vacância Média</span>', '</span>', patterns_to_remove)
        vacancy_as_text = vacancy_as_text.replace('-', '').strip()
        return text_to_number(vacancy_as_text) if vacancy_as_text else None

    def generate_link():
      cnpj = get_substring(data, 'abrirGerenciadorDocumentosCVM?cnpjFundo=', '">Pesquisar Documentos', '#')
      return f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={cnpj}'

    ALL_INFO = {
        'name': lambda: get_substring(data, 'Nome</span>', '</span>', patterns_to_remove),
        'type': lambda: None,
        'segment': lambda: get_substring(data, 'Mandato</span>', '</span>', patterns_to_remove),
        'actuation': lambda: None,
        'link': generate_link,
        'price': lambda: text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        'liquidity': lambda: text_to_number(get_substring(data, 'Vol $ méd (2m)</span>', '</span>', patterns_to_remove)),
        'total_issued_shares': lambda: text_to_number(get_substring(data, 'Nro. Cotas</span>', '</span>', patterns_to_remove)),
        'net_equity_value': lambda: text_to_number(get_substring(data, 'Patrim Líquido</span>', '</span>', patterns_to_remove)),
        'equity_price': lambda: text_to_number(get_substring(data, 'VP/Cota</span>', '</span>', patterns_to_remove)),
        'variation_12M': lambda: text_to_number(get_substring(data, '12 meses</span>', '</span>', patterns_to_remove)),
        'variation_30D': lambda: text_to_number(get_substring(data, 'Mês</span>', '</span>', patterns_to_remove)),
        'min_52_weeks': lambda: text_to_number(get_substring(data, 'Min 52 sem</span>', '</span>', patterns_to_remove)),
        'max_52_weeks': lambda: text_to_number(get_substring(data, 'Max 52 sem</span>', '</span>', patterns_to_remove)),
        'PVP': lambda: text_to_number(get_substring(data, 'P/VP</span>', '</span>', patterns_to_remove)),
        'DY': lambda: text_to_number(get_substring(data, 'Div. Yield</span>', '</span>', patterns_to_remove)),
        'latests_dividends': lambda: None,
        'latest_dividend': lambda: text_to_number(get_substring(data, 'Dividendo/cota</span>', '</span>', patterns_to_remove)),
        'ffoy': lambda: text_to_number(get_substring(data, 'FFO Yield</span>', '</span>', patterns_to_remove)),
        'vacancy': get_vacancy,
        'total_real_state': lambda: text_to_number(get_substring(data, 'Qtd imóveis</span>', '</span>', patterns_to_remove)),
        'management': lambda: get_substring(data, 'Gestão</span>', '</span>', patterns_to_remove),
        'cash_value': lambda: get_substring(data, 'Caixa\'', ']', [', data : [']),
        'assets_value': lambda: text_to_number(get_substring(data, '>Ativos</span>', '</span>', patterns_to_remove)),
        'market_value': lambda: text_to_number(get_substring(data, 'Valor de mercado</span>', '</span>', patterns_to_remove)),
        'initial_date': lambda: None,
        'target_public': lambda: None,
        'term': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def get_data_from_fundamentus(ticker, info_names):
    try:
        url = f'https://fundamentus.com.br/detalhes.php?papel={ticker}'

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://fundamentus.com.br/index.php',
            'Origin': 'https://fundamentus.com.br/index.php',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0'
        }

        response = request_get(url, headers)
        html_page = response.text

        #print(f'Converted Fundamentus data: {convert_fundamentus_data(html_page, info_names)}')
        return convert_fundamentus_data(html_page, info_names)
    except Exception as error:
        #print(f'Error on get Fundamentus data: {traceback.format_exc()}')
        return None

def convert_fundsexplorer_data(data, info_names):
    ALL_INFO = {
        'name': lambda: data['name'],
        'type': lambda: data['setor_atuacao'],
        'segment': lambda: data['segmento_ambima'],
        'actuation': lambda: data['segmento_atuacao'],
        'link': lambda: None,
        'price': lambda: data['valor'],
        'liquidity': lambda: data['liquidezmediadiaria'],
        'total_issued_shares': lambda: data['numero_cotas'],
        'net_equity_value': lambda: data['patrimonio'],
        'equity_price': lambda: data['valorpatrimonialcota'],
        'variation_12M': lambda: data['valorizacao_12_meses'],
        'variation_30D': lambda: data['valorizacao_mes'],
        'min_52_weeks': lambda: data['min_52_semanas'],
        'max_52_weeks': lambda: data['max_52_semanas'],
        'PVP': lambda: data['pvp'],
        'DY': lambda: data['dy'],
        'latests_dividends': lambda: data['dividendos_12_meses'],
        'latest_dividend': lambda: data['lastdividend'],
        'ffoy': lambda: None,
        'vacancy': lambda: data['vacancia'],
        'total_real_state': lambda: data['assets_number'],
        'management': lambda: data['gestao'],
        'cash_value': lambda: data['valor_caixa'],
        'assets_value': lambda: None,
        'market_value': lambda: data['valormercado'],
        'initial_date': lambda: data['firstdate'],
        'target_public': lambda: data['publicoalvo'],
        'term': lambda: data['prazoduracao']
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def get_data_from_fundsexplorer(ticker, info_names):
    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'DNT': '1',
            'Priority': 'u=0, i',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0'
        }
    
        response = request_get(f'https://www.fundsexplorer.com.br/funds/{ticker}', headers)
        html_page = response.text
    
        data_as_text = get_substring(html_page, 'var dataLayer_content', 'dataLayer.push')

        data_as_json = json.loads(data_as_text.strip(';= '))['pagePostTerms']['meta']

        #print(f"Converted Fundsexplorer data: {convert_fundsexplorer_data(data_as_json)}")
        return convert_fundsexplorer_data(data_as_json, info_names)
    except Exception as error:
        #print(f"Error on get Fundsexplorer data: {traceback.format_exc()}")
        return None

def convert_investidor10_data(data, info_names):
    patterns_to_remove = [
        '</div>',
        '<div>',
        '<div class="value">',
        '<div class="_card-body">',
        '</span>',
        '<span>',
        '<span class="value">'
    ]

    def multiply_by_unit(data):
        if not data:
            return None

        if 'K' in data:
            return text_to_number(data.replace('K', '')) * 1000
        elif 'M' in data:
            return text_to_number(data.replace('Milhões', '').replace('M', '')) * 1000000

        return text_to_number(data)

    count_pattern_on_text = lambda text, pattern: None if not text or not pattern else len(text.split(pattern))

    ALL_INFO = {
        'name': lambda: get_substring(data, 'Razão Social', '<div class=\'cell\'>', patterns_to_remove),
        'type': lambda: get_substring(data, 'TIPO DE FUNDO', '<div class=\'cell\'>', patterns_to_remove),
        'segment': lambda: get_substring(data, 'SEGMENTO', '<div class=\'cell\'>', patterns_to_remove),
        'actuation': lambda: None,
        'link': lambda: None,
        'price': lambda: text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        'liquidity': lambda: multiply_by_unit(get_substring(data, 'title="Liquidez Diária">Liquidez Diária</span>', '</span>', patterns_to_remove)),
        'total_issued_shares': lambda: text_to_number(get_substring(data, 'COTAS EMITIDAS', '<div class=\'cell\'>', patterns_to_remove)),
        'net_equity_value': lambda: multiply_by_unit(get_substring(data, 'VALOR PATRIMONIAL', '<div class=\'cell\'>', patterns_to_remove)),
        'equity_price': lambda: text_to_number(get_substring(data, 'VAL. PATRIMONIAL P/ COTA', '<div class=\'cell\'>', patterns_to_remove)),
        'variation_12M': lambda: text_to_number(get_substring(data, 'title="Variação (12M)">VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove)),
        'variation_30D': lambda: None,
        'min_52_weeks': lambda: None,
        'max_52_weeks': lambda: None,
        'PVP': lambda: text_to_number(get_substring(data, 'title="P/VP">P/VP</span>', '</span>', patterns_to_remove)),
        'DY':  lambda: text_to_number(get_substring(data, 'DY (12M)</span>', '</span>', patterns_to_remove)),
        'latests_dividends': lambda: text_to_number(get_substring(get_substring(data, 'YIELD 12 MESES', '</div>'), 'amount">', '</span>', patterns_to_remove)),
        'latest_dividend': lambda: text_to_number(get_substring(data, 'ÚLTIMO RENDIMENTO', '</div>', patterns_to_remove)),
        'ffoy': lambda: None,
        'vacancy': lambda: text_to_number(get_substring(data, 'VACÂNCIA', '<div class=\'cell\'>', patterns_to_remove)),
        'total_real_state': lambda: count_pattern_on_text(get_substring(data, 'Lista de Imóveis', '</section>'), 'card-propertie'),
        'management': lambda: get_substring(data, 'TIPO DE GESTÃO', '<div class=\'cell\'>', patterns_to_remove),
        'cash_value': lambda: None,
        'assets_value': lambda: None,
        'market_value': lambda: None,
        'initial_date': lambda: None,
        'target_public': lambda: get_substring(data, 'PÚBLICO-ALVO', '<div class=\'cell\'>', patterns_to_remove),
        'term': lambda: get_substring(data, 'PRAZO DE DURAÇÃO', '<div class=\'cell\'>', patterns_to_remove)
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def get_data_from_investidor10(ticker, info_names):
    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': 'https://investidor10.com.br/fiis/mxrf11/',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        }
    
        response = request_get(f'https://investidor10.com.br/fiis/{ticker}', headers)
        html_page = response.text[15898:]
    
        #print(f"Converted Investidor 10 data: {convert_investidor10_data(html_page)}")
        return convert_investidor10_data(html_page, info_names)
    except Exception as error:
        #print(f"Error on get Fundsexplorer data: {traceback.format_exc()}")
        return None

def get_data_from_all_sources(ticker, info_names):
    data_fundamentus = get_data_from_fundamentus(ticker, info_names)
    #print(f'Data from Fundamentus: {data_fundamentus}')

    blank_fundamentus_info_names = [ info for info in info_names if not data_fundamentus.get(info, False) ]
    #print(f'Blank fundamentus names: {blank_fundamentus_info_names}')

    if data_fundamentus and not blank_fundamentus_info_names:
        return data_fundamentus

    data_fundsexplorer = get_data_from_fundsexplorer(ticker, blank_fundamentus_info_names if blank_fundamentus_info_names else info_names)
    #print(f'Data from Funds Explorer: {data_fundsexplorer}')

    if data_fundamentus and data_fundsexplorer:
      data_fundamentus_or_fundsexplorer = { **data_fundamentus, **data_fundsexplorer }
      #print(f'From fundamentus and funds explorer: {data_fundamentus_or_fundsexplorer}')
    elif data_fundamentus and not data_fundsexplorer:
      data_fundamentus_or_fundsexplorer = data_fundamentus
      #print(f'From fundamentus: {data_fundamentus_or_fundsexplorer}')
    elif not data_fundamentus and data_fundsexplorer:
      data_fundamentus_or_fundsexplorer = data_fundsexplorer
      #print(f'From funds explorer: {data_fundamentus_or_fundsexplorer}')
    else:
      data_fundamentus_or_fundsexplorer = {}
      #print(f'From None: {data_fundamentus_or_fundsexplorer}')

    blank_fundamentus_or_fundsexplorer_info_names = [ info for info in info_names if not data_fundamentus_or_fundsexplorer.get(info, False) ]
    #print(f'Blank fudnamentus or funds explorer names: {blank_fundamentus_or_fundsexplorer_info_names}')

    if data_fundamentus_or_fundsexplorer and not blank_fundamentus_or_fundsexplorer_info_names:
        return data_fundamentus_or_fundsexplorer

    data_investidor_10 = get_data_from_investidor10(ticker, blank_fundamentus_or_fundsexplorer_info_names if blank_fundamentus_or_fundsexplorer_info_names else info_names)
    #print(f'Data from Investidor 10: {data_investidor_10}')

    if not data_investidor_10:
        return data_fundamentus_or_fundsexplorer

    return { **data_fundamentus_or_fundsexplorer, **data_investidor_10 }

def request_shares(ticker, source, info_names):
    if source == VALID_SOURCES['FUNDAMENTUS_SOURCE']:
        return get_data_from_fundamentus(ticker, info_names)
    elif source == VALID_SOURCES['FUNDSEXPLORER_SOURCE']:
        return get_data_from_fundsexplorer(ticker, info_names)
    elif source == VALID_SOURCES['INVESTIDOR10_SOURCE']:
        return get_data_from_investidor10(ticker, info_names)

    return get_data_from_all_sources(ticker, info_names)

@app.route('/fii/<ticker>', methods=['GET'])
def get_fii_data_by(ticker):
    should_delete_cache = request.args.get('should_delete_cache', '0').lower() in TRUE_BOOL_VALUES
    should_clear_cache = request.args.get('should_clear_cache', '0').lower() in TRUE_BOOL_VALUES
    should_use_cache = request.args.get('should_use_cache', '1').lower() in TRUE_BOOL_VALUES

    source = request.args.get('source', VALID_SOURCES['ALL_SOURCE']).replace(' ', '').lower()
    source = source if source in VALID_SOURCES.keys() else VALID_SOURCES['ALL_SOURCE']

    info_names = request.args.get('info_names', '').replace(' ', '').lower().split(',')
    info_names = [ info for info in info_names if info in VALID_INFOS ]
    info_names = info_names if len(info_names) else VALID_INFOS

    #print(f'Delete cache? {should_delete_cache}, Clear cache? {should_clear_cache}, Use cache? {should_use_cache}')
    #print(f'Ticker: {ticker}, Source: {source}, Info names: {info_names}')

    if should_delete_cache:
        delete_cache()

    should_use_and_not_delete_cache = should_use_cache and not should_delete_cache

    if should_use_and_not_delete_cache:
        id = f'{ticker}{source}{",".join(sorted(info_names))}'.encode('utf-8')
        hash_id = sha512(id).hexdigest()
        #print(f'Cache Hash ID: {hash_id}, From values: {id}')

        cached_data, cache_date = read_cache(hash_id, should_clear_cache)

        if cached_data:
            #print(f'Data from Cache: {cached_data}')
            return jsonify({'data': cached_data, 'source': 'cache', 'date': cache_date.strftime("%d/%m/%Y, %H:%M")}), 200

    data = request_shares(ticker, source, info_names)
    #print(f'Data from Source: {data}')

    if should_use_and_not_delete_cache and not should_clear_cache:
        write_to_cache(hash_id, data)

    return jsonify({'data': data, 'source': 'fresh', 'date': datetime.now().strftime("%d/%m/%Y, %H:%M")}), 200

if __name__ == '__main__':
    is_debug = os.getenv('IS_DEBUG', False)
    app.run(debug=is_debug)
