from datetime import datetime, timedelta
import http.client as httplib
import json
import os
import re
import traceback

from flask import Flask, jsonify, request

import requests
from requests import RequestException

app = Flask(__name__)
app.json.sort_keys = False

TRUE_BOOL_VALUES = ('true', '1', 't', 'y', 'yes', 's', 'sim')

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

FUNDAMENTUS_SOURCE = 'fundamentus'
FUNDSEXPLORER_SOURCE = 'fundsexplorer'
INVESTIDOR10_SOURCE = 'investidor10'

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
        print('------>Not fount for ', start_text)
        return None

    clean_text = cutted_text.replace('\n', '').replace('\t', '')

    no_tags_text = re.sub(r'<[^>]*>', '', clean_text) if should_remove_tags else clean_text

    final_text = no_tags_text
    for pattern in replace_by_paterns:
        final_text = final_text.replace(pattern, '')
    print('------>', start_text, '#@#', clean_text, '#@#', final_text)
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

def clear_cache(ticker):
    #print(f'Cleaning cache')
    with open(CACHE_FILE, 'r') as cache_file:
        lines = cache_file.readlines()

    with open(CACHE_FILE, 'w') as cache_file:
        for line in lines:
            if not line.startswith(ticker):
                cache_file.write(line)
   #print(f'Cleaned')

def read_cache(ticker, should_clear_cache):
    if not os.path.exists(CACHE_FILE):
        return None, None

    if should_clear_cache:
        clear_cache(ticker)
        return None, None

    control_clean_cache = False

    #print(f'Reading cache')
    with open(CACHE_FILE, 'r') as cache_file:
        for line in cache_file:
            if not line.startswith(ticker):
                continue

            _, cached_datetime, data = line.strip().split('#@#')

            cached_date = datetime.strptime(cached_datetime, '%Y-%m-%d %H:%M:%S')

            if datetime.now() - cached_date <= CACHE_EXPIRY:
                #print(f'Finished read')
                return json.loads(data.replace("'", '"')), cached_date

            control_clean_cache = True
            break

    if control_clean_cache:
        clear_cache(ticker)

    return None, None

def write_to_cache(ticker, data):
    #print(f'Writing cache')
    with open(CACHE_FILE, 'a') as cache_file:
        cache_file.write(f'{ticker}#@#{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}#@#{data}\n')
    #print(f'Writed')

def convert_fundamentus_data(data):
    patterns_to_remove = [
        '</span>',
        '<span class="txt">',
        '<span class="oscil">',
        '</td>',
        '<td class="data">',
        '<td class="data w1">',
        '<td class="data w2">',
        '<td class="data w3">',
        '<td class="data destaque w3">',
        '<a href="resultado.php?segmento=',
        '<font color="#306EFF">',
        '<font color="#F75D59">'
    ]

    vacancy_as_text = get_substring(data, 'Vacância Média</span>', '</span>', patterns_to_remove)
    vacancy_as_text = vacancy_as_text.replace('-', '').strip()
    vacancy_as_number = text_to_number(vacancy_as_text) if vacancy_as_text else None

    generate_link = lambda cnpj: f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={cnpj}'

    return {
        'name': get_substring(data, 'Nome</span>', '</span>', patterns_to_remove),
        'type': None,
        'segment': get_substring(data, 'Mandato</span>', '</span>', patterns_to_remove),
        'actuation': None,
        'link': generate_link(get_substring(data, 'abrirGerenciadorDocumentosCVM?cnpjFundo=', '">Pesquisar Documentos')),
        'price': text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        'liquidity': text_to_number(get_substring(data, 'Vol $ méd (2m)</span>', '</span>', patterns_to_remove)),
        'total_issued_shares': text_to_number(get_substring(data, 'Nro. Cotas</span>', '</span>', patterns_to_remove)),
        'net_equity_value': text_to_number(get_substring(data, 'Patrim Líquido</span>', '</span>', patterns_to_remove)),
        'equity_price': text_to_number(get_substring(data, 'VP/Cota</span>', '</span>', patterns_to_remove)),
        'variation_12M': text_to_number(get_substring(data, '12 meses</span>', '</span>', patterns_to_remove)),
        'variation_30D': text_to_number(get_substring(data, 'Mês</span>', '</span>', patterns_to_remove)),
        'min_52_weeks': text_to_number(get_substring(data, 'Min 52 sem</span>', '</span>', patterns_to_remove)),
        'max_52_weeks': text_to_number(get_substring(data, 'Max 52 sem</span>', '</span>', patterns_to_remove)),
        'PVP': text_to_number(get_substring(data, 'P/VP</span>', '</span>', patterns_to_remove)),
        'DY': text_to_number(get_substring(data, 'Div. Yield</span>', '</span>', patterns_to_remove)),
        'latests_dividends': None,
        'latest_dividend': text_to_number(get_substring(data, 'Dividendo/cota</span>', '</span>', patterns_to_remove)),
        'ffoy': text_to_number(get_substring(data, 'FFO Yield</span>', '</span>', patterns_to_remove)),
        'vacancy': vacancy_as_number,
        'total_real_state': text_to_number(get_substring(data, 'Qtd imóveis</span>', '</span>', patterns_to_remove)),
        'management': get_substring(data, 'Gestão</span>', '</span>', patterns_to_remove),
        'cash_value': get_substring(data, 'Caixa\'', ']', [', data : [']),
        'assets_value': text_to_number(get_substring(data, '>Ativos</span>', '</span>', patterns_to_remove)),
        'market_value': text_to_number(get_substring(data, 'Valor de mercado</span>', '</span>', patterns_to_remove)),
        'initial_date': None,
        'target_public': None,
        'term': None
    }

def get_data_from_fundamentus_by(ticker):
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
    
        #print(f"Converted Fundamentus data: {convert_fundamentus_data(html_page)}")
        return convert_fundamentus_data(html_page)
    except:
        #print(f"Error on get Fundamentus data: {traceback.format_exc()}")
        return None

def convert_fundsexplorer_data(data):
    return {
        'name':  data['name'],
        'type': data['setor_atuacao'],
        'segment': data['segmento_ambima'],
        'actuation': data['segmento_atuacao'],
        'link': None,
        'price': data['valor'],
        'liquidity': data['liquidezmediadiaria'],
        'total_issued_shares': data['numero_cotas'],
        'net_equity_value': data['patrimonio'],
        'equity_price': data['valorpatrimonialcota'],
        'variation_12M': data['valorizacao_12_meses'],
        'variation_30D': data['valorizacao_mes'],
        'min_52_weeks': data['min_52_semanas'],
        'max_52_weeks': data['max_52_semanas'],
        'PVP': data['pvp'],
        'DY': data['dy'],
        'latests_dividends': data['dividendos_12_meses'],
        'latest_dividend': data['lastdividend'],
        'ffoy': None,
        'vacancy': data['vacancia'],
        'total_real_state': data['assets_number'],
        'management': data['gestao'],
        'cash_value': data['valor_caixa'],
        'assets_value': None,
        'market_value': data['valormercado'],
        'initial_date': data['firstdate'],
        'target_public': data['publicoalvo'],
        'term': data['prazoduracao']
    }

def get_data_from_fundsexplorer_by(ticker):
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

        return convert_fundsexplorer_data(data_as_json)
    except Exception as error:
        #print(f"Error on get Fundsexplorer data: {traceback.format_exc()}")
        return None

def convert_investidor10_data(data):
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

        value = text_to_number(data.replace('k|K|Mil|m|M|Milhões', ''))

        if 'k' in data or 'K' in data or 'Mil' in data:
            return value * 1000
        elif 'm' in data or 'M' in data or 'Milhões' in data:
            return value * 1000000

        return value

    def count_repetitions(data, pattern):
        if not data:
            return None

        count = 0
        index = -1
        
        while True:
            index = data.find(pattern, index +1)

            if index == -1:
                break

            count += 1

        return count

    def count_pattern_on_text(text, pattern):
        if not text or not pattern:
            return None

        return text.lower().split().count(pattern.lower())

    return {
        'name':  get_substring(data, 'Razão Social', '<div class=\'cell\'>', patterns_to_remove),
        'type': get_substring(data, 'TIPO DE FUNDO', '<div class=\'cell\'>', patterns_to_remove),
        'segment': get_substring(data, 'SEGMENTO', '<div class=\'cell\'>', patterns_to_remove),
        'actuation': None,
        'link': None,
        'price': text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        'liquidity': multiply_by_unit(get_substring(data, 'title="Liquidez Diária">Liquidez Diária</span>', '</span>', patterns_to_remove)),
        'total_issued_shares': text_to_number(get_substring(data, 'COTAS EMITIDAS', '<div class=\'cell\'>', patterns_to_remove)),
        'net_equity_value': multiply_by_unit(get_substring(data, 'VALOR PATRIMONIAL', '<div class=\'cell\'>', patterns_to_remove)),
        'equity_price': text_to_number(get_substring(data, 'VAL. PATRIMONIAL P/ COTA', '<div class=\'cell\'>', patterns_to_remove)),
        'variation_12M': text_to_number(get_substring(data, 'title="Variação (12M)">VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove)),
        'variation_30D': None,
        'min_52_weeks': None,
        'max_52_weeks': None,
        'PVP': text_to_number(get_substring(data, 'title="P/VP">P/VP</span>', '</span>', patterns_to_remove)),
        'DY':  text_to_number(get_substring(data, 'DY (12M)</span>', '</span>', patterns_to_remove)),
        'latests_dividends': text_to_number(get_substring(get_substring(data, 'YIELD 6 MESES', '<div class="content--info--item">', patterns_to_remove), 'content--info--item--value amount">', '</span>')),
        'latest_dividend': text_to_number(get_substring(data, 'ÚLTIMO RENDIMENTO', '</span>', patterns_to_remove)),
        'ffoy': None,
        'vacancy': text_to_number(get_substring(data, 'VACÂNCIA', '<div class=\'cell\'>', patterns_to_remove)),
        'total_real_state': count_pattern_on_text(get_substring(data, 'Lista de Imóveis', '<button data-id="read-more-action'), 'card-propertie'),
        'management': get_substring(data, 'TIPO DE GESTÃO', '<div class=\'cell\'>', patterns_to_remove),
        'cash_value': None,
        'assets_value': None,
        'market_value': None,
        'initial_date': None,
        'target_public': get_substring(data, 'PÚBLICO-ALVO', '<div class=\'cell\'>', patterns_to_remove),
        'term': get_substring(data, 'PRAZO DE DURAÇÃO', '<div class=\'cell\'>', patterns_to_remove)
    }

def get_data_from_investidor10_by(ticker):
    try:
        headers = {
            'accept': 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            #'referer': 'https://investidor10.com.br/fiis/mxrf11/',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        }
    
        response = request_get(f'https://investidor10.com.br/fiis/{ticker}', headers)
        html_page = response.text#[15898:]
    
        #print(f"Converted Investidor 10 data: {convert_investidor10_data(html_page)}")
        return convert_investidor10_data(html_page)
    except Exception as error:
        #print(f"Error on get Fundsexplorer data: {traceback.format_exc()}")
        return None

def get_data_from_all_by(ticker):
    data_fundamentus = get_data_from_fundamentus_by(ticker)
    data_fundsexplorer = get_data_from_fundsexplorer_by(ticker)
    data_investidor10 = get_data_from_investidor10_by(ticker)

    if not data_fundamentus:
        return data_fundsexplorer

    if not data_fundsexplorer:
        return data_investidor10

    data_merge = {}

    for key, value in data_fundamentus.items():
        if not value:
            #print(f'Key: {key}')
            if key in data_fundsexplorer and data_fundsexplorer[key]:
               # print(f'Found data on Fundsexplorer: {data_fundsexplorer[key]}')
                data_merge[key] = data_fundsexplorer[key]
            elif key in data_investidor10 and data_investidor10[key]:
                #print(f'Found data on Investidor 10: {data_investidor10[key]}')
                data_merge[key] = data_investidor10[key]

            continue

        #print(f'Using Fundamentus data: {value}')
        data_merge[key] = value

    return data_merge

def request_shares_by(ticker, source):
    if source == FUNDAMENTUS_SOURCE:
        return get_data_from_fundamentus_by(ticker)
    elif source == FUNDSEXPLORER_SOURCE:
        return get_data_from_fundsexplorer_by(ticker)
    elif source == INVESTIDOR10_SOURCE:
        return get_data_from_investidor10_by(ticker)

    return get_data_from_all_by(ticker)

@app.route('/fii/<ticker>', methods=['GET'])
def get_fii_data_by(ticker):
    should_delete_cache = request.args.get('should_delete_cache', '0').lower() in TRUE_BOOL_VALUES
    should_clear_cache = request.args.get('should_clear_cache', '0').lower() in TRUE_BOOL_VALUES
    should_use_cache = request.args.get('should_use_cache', '1').lower() in TRUE_BOOL_VALUES

    source = request.args.get('source', 'all').lower()

    #print(f'Delete cache? {should_delete_cache}, Clear cache? {should_clear_cache}, Use cache? {should_use_cache}')
    #print(f'Ticker: {ticker}, Source: {source}')

    if should_delete_cache:
        delete_cache()

    if should_use_cache and not should_delete_cache:
        cached_data , cache_date = read_cache(ticker, should_clear_cache)

        if cached_data:
            #print(f'Data from Cache: {cached_data}')
            return jsonify({'data': cached_data, 'source': 'cache', 'date': cache_date.strftime("%d/%m/%Y, %H:%M")}), 200

    data = request_shares_by(ticker, source)
    #print(f'Data from Source: {data}')

    if should_use_cache and not should_delete_cache and not should_clear_cache:
        write_to_cache(ticker, data)

    return jsonify({'data': data, 'source': 'fresh', 'date': datetime.now().strftime("%d/%m/%Y, %H:%M")}), 200

if __name__ == '__main__':
    is_debug = os.getenv('IS_DEBUG', False)
    app.run(debug=is_debug)
