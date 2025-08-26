import ast
import base64
from datetime import datetime, timedelta
from html import unescape
import json
import os
import re
import traceback

from flask import Flask, jsonify, request

import requests

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

DATE_FORMAT = '%d-%m-%Y %H:%M:%S'

DEBUG_LOG_LEVEL = 'DEBUG'
ERROR_LOG_LEVEL = 'ERROR'
INFO_LOG_LEVEL = 'INFO'
LOG_LEVEL = os.environ.get('LOG_LEVEL', ERROR_LOG_LEVEL)

SEPARATOR = '#@#'

VALID_SOURCES = {
    'ALL_SOURCE': 'all',
    'BMFBOVESPA_SOURCE': 'bmfbovespa',
    'FIIS_SOURCE': 'fiis',
    'FUNDAMENTUS_SOURCE': 'fundamentus',
    'FUNDSEXPLORER_SOURCE': 'fundsexplorer',
    'INVESTIDOR10_SOURCE': 'investidor10',
    'INFOMONEY_SOURCE': 'infomoney'
}

VALID_INFOS = [
    'actuation',
    'assets_value',
    'cash_value',
    'debit_by_real_state_acquisition',
    'debit_by_securitization_receivables_acquisition',
    'dy',
    'equity_price',
    'ffoy',
    'initial_date',
    'latest_dividend',
    'latests_dividends',
    'link',
    'liquidity',
    'management',
    'market_value',
    'max_52_weeks',
    'min_52_weeks',
    'name',
    'net_equity_value',
    'price',
    'pvp',
    'segment',
    'target_public',
    'term',
    'total_issued_shares',
    'total_mortgage',
    'total_mortgage_value',
    'total_real_state',
    'total_real_state_value',
    'total_stocks_fund_others',
    'total_stocks_fund_others_value',
    'type',
    'vacancy',
    'variation_12m',
    'variation_30d',
    'avg_price',
    'mayer_multiple'
]

investidor_10_preloaded_data = (None, None)
fundamentus_preloaded_data = (None, None)
fiis_preloaded_data = (None, None)

app = Flask(__name__)
app.json.sort_keys = False

def log_error(message):
    if LOG_LEVEL == ERROR_LOG_LEVEL or LOG_LEVEL == INFO_LOG_LEVEL or LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {ERROR_LOG_LEVEL} - {message}')

def log_info(message):
    if LOG_LEVEL == INFO_LOG_LEVEL or LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {INFO_LOG_LEVEL} - {message}')

def log_debug(message):
    if LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {DEBUG_LOG_LEVEL} - {message}')

def cache_exists():
    if os.path.exists(CACHE_FILE):
        return True

    log_info('No cache file found')
    return False

def upsert_cache(id, data):
    lines = []
    updated = False

    if cache_exists():
        with open(CACHE_FILE, 'r') as cache_file:
            lines = cache_file.readlines()

    with open(CACHE_FILE, 'w') as cache_file:
        for line in lines:
            if not line.startswith(id):
                cache_file.write(line)
                continue

            _, old_cached_date_as_text, old_data_as_text = line.strip().split(SEPARATOR)
            old_data = ast.literal_eval(old_data_as_text)

            combined_data = { **old_data, **data }
            updated_line = f'{id}{SEPARATOR}{old_cached_date_as_text}{SEPARATOR}{combined_data}\n'
            cache_file.write(updated_line)
            updated = True

        if not updated:
            new_line = f'{id}{SEPARATOR}{datetime.now().strftime(DATE_FORMAT)}{SEPARATOR}{data}\n'
            cache_file.write(new_line)
            log_info(f'New cache entry created for "{id}"')

    if updated:
        log_info(f'Cache updated for "{id}"')


def clear_cache(id):
    if not cache_exists():
        return

    log_debug('Cleaning cache')

    with open(CACHE_FILE, 'r') as cache_file:
        lines = cache_file.readlines()

    with open(CACHE_FILE, 'w') as cache_file:
        cache_file.writelines(line for line in lines if not line.startswith(id))

    log_info(f'Cache cleaning completed for "{id}"')

def read_cache(id):
    if not cache_exists():
        return None

    log_debug('Reading cache')

    clear_cache_control = False

    with open(CACHE_FILE, 'r') as cache_file:
        for line in cache_file:
            if not line.startswith(id):
                continue

            _, cached_date_as_text, data = line.strip().split(SEPARATOR)
            cached_date = datetime.strptime(cached_date_as_text, DATE_FORMAT)

            if datetime.now() - cached_date <= CACHE_EXPIRY:
                log_debug(f'Cache hit for "{id}" (Date: {cached_date_as_text})')
                return ast.literal_eval(data)

            log_debug(f'Cache expired for "{id}" (Date: {cached_date_as_text})')
            clear_cache_control = True
            break

    if clear_cache_control:
        clear_cache(id)

    log_info(f'No cache entry found for "{id}"')
    return None

def delete_cache():
    if not cache_exists():
        return

    log_debug('Deleting cache')

    os.remove(CACHE_FILE)

    log_info('Cache deletion completed')

def preprocess_cache(id, should_delete_all_cache, should_clear_cached_data, should_use_cache):
    if should_delete_all_cache:
        delete_cache()
    elif should_clear_cached_data:
        clear_cache(id)

    can_use_cache = should_use_cache and not (should_delete_all_cache or should_clear_cached_data)

    return can_use_cache

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

def request_get(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    log_debug(f'Response from {url} : {response}')

    return response

def request_post(url, headers=None, data=None):
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()

    log_debug(f'Response from {url} : {response}')

    return response

def convert_bmfbovespa_data(IME_doc, ITE_doc, RA_docs, cnpj, info_names):
    patterns_to_remove = [
        '</b>',
        '</span>',
        '</td>',
        '<b>',
        '<center>',
        '<span class="dado-cabecalho">',
        '<span class="dado-valores">',
        '<span>',
        '<td align="center">',
        '<td>'
    ]

    def count_total_real_state_value():
        return text_to_number(get_substring(IME_doc[0], 'Direitos reais sobre bens im&oacute;veis ', '</span>', patterns_to_remove))

    def count_total_mortgage_value():
        return (
            text_to_number(get_substring(IME_doc[0], 'Certificados de Dep&oacute;sitos de Valores Mobili&aacute;rios', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'Notas Promiss&oacute;rias', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'Notas Comerciais', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'CRI" (se FIAGRO, Certificado de Receb&iacute;veis do Agroneg&oacute;cio "CRA")', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'Hipotec&aacute;rias', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'LCI" (se FIAGRO, Letras de Cr&eacute;dito do Agroneg&oacute;cio "LCA")', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(IME_doc[0], 'LIG)', '</span>', patterns_to_remove))
        )

    def count_total_stocks_fund_others_value():
        return (
                text_to_number(get_substring(IME_doc[0], 'A&ccedil;&otilde;es', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'Deb&ecirc;ntures', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'certificados de desdobramentos', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'FIA)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'FIP)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'FII)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'FIDC)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'Outras cotas de Fundos de Investimento', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'A&ccedil;&otilde;es de Sociedades cujo &uacute;nico prop&oacute;sito se enquadra entre as atividades permitidas aos FII', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'Cotas de Sociedades que se enquadre entre as atividades permitidas aos FII', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'CEPAC)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(IME_doc[0], 'Outros Valores Mobili&aacute;rios', '</span>', patterns_to_remove))
            )

    def fii_type():
        fii_type = {
            'Outro': count_total_stocks_fund_others_value(),
            'Papel': count_total_mortgage_value(),
            'Tijolo': count_total_real_state_value()
        }

        return max(fii_type, key=fii_type.get)

    ALL_INFO = {
        'actuation': lambda: None,
        'assets_value': lambda: text_to_number(get_substring(IME_doc[0], 'Ativo &ndash; R$', '</span>', patterns_to_remove)),
        'cash_value': lambda: text_to_number(get_substring(IME_doc[0], 'Total mantido para as Necessidades de Liquidez (art. 46, &sect; &uacute;nico, ICVM 472/08) </b>', '</span>', patterns_to_remove)),
        'debit_by_real_state_acquisition': lambda: text_to_number(get_substring(IME_doc[0], 'Obriga&ccedil;&otilde;es por aquisi&ccedil;&atilde;o de im&oacute;veis', '</span>', patterns_to_remove)),
        'debit_by_securitization_receivables_acquisition': lambda: text_to_number(get_substring(IME_doc[0], 'Obriga&ccedil;&otilde;es por securitiza&ccedil;&atilde;o de receb&iacute;veis', '</span>', patterns_to_remove)),
        'dy': lambda: None,
        'equity_price': lambda: text_to_number(get_substring(IME_doc[0], 'Valor Patrimonial das Cotas &ndash; R$', '</span>', patterns_to_remove)),
        'ffoy': lambda: None,
        'initial_date': lambda: get_substring(IME_doc[0], 'doc de Funcionamento:', '</span>', patterns_to_remove),
        'latest_dividend': lambda: RA_docs[max(RA_docs.keys(), key=lambda date: datetime.strptime(date, "%d%m%Y"))] if len(RA_docs) else None,
        'latests_dividends': lambda: sum(RA_docs.values()),
        'link': lambda: f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={cnpj}#',
        'liquidity': lambda: None,
        'management': lambda: get_substring(IME_doc[0], 'Tipo de Gest&atilde;o:', '</span>', patterns_to_remove),
        'market_value': lambda: None,
        'max_52_weeks': lambda: None,
        'min_52_weeks': lambda: None,
        'name': lambda: unescape(get_substring(IME_doc[0], 'Nome do Fundo/Classe: </span>', '</span>', patterns_to_remove)),
        'net_equity_value': lambda: text_to_number(get_substring(IME_doc[0], 'Patrim&ocirc;nio L&iacute;quido &ndash; R$', '</span>', patterns_to_remove)),
        'price': lambda: None,
        'pvp': lambda: None,
        'segment': lambda: unescape(get_substring(IME_doc[0], 'Segmento de Atua&ccedil;&atilde;o:', '</span>', patterns_to_remove)),
        'target_public': lambda: get_substring(IME_doc[0], 'P&uacute;blico Alvo: </span>', '</span>', patterns_to_remove),
        'term': lambda: get_substring(IME_doc[0], '>Prazo de Dura&ccedil;&atilde;o: </span>', '</span>', patterns_to_remove),
        'total_issued_shares': lambda: text_to_number(get_substring(IME_doc[0], 'Quantidade de cotas emitidas: </span>', '</span>', patterns_to_remove)),
        'total_mortgage': lambda: get_substring(ITE_doc[0], ' 1.2.2', '1.2.6', patterns_to_remove).count('</tr>') - 8,
        'total_mortgage_value': count_total_mortgage_value,
        'total_real_state': lambda: ITE_doc[0].count('&Aacute;rea (m2):') + (get_substring(ITE_doc[0], '1.1.1', '>1.1.2<', patterns_to_remove).count('</tr>') - 2),
        'total_real_state_value': count_total_real_state_value,
        'total_stocks_fund_others': lambda: (get_substring(ITE_doc[0], ' 1.2.1', ' 1.2.2', patterns_to_remove).count('</tr>') - 2) + (get_substring(ITE_doc[0], ' 1.2.6', '>1.3<', patterns_to_remove).count('</tr>') - 16),
        'total_stocks_fund_others_value': count_total_stocks_fund_others_value,
        'type': fii_type,
        'vacancy': lambda: None,
        'variation_12m': lambda: None,
        'variation_30d': lambda: None,
        'avg_price': lambda: None,
        'mayer_multiple': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def fetch_documents(cnpj, document_configs, info_selector=None):
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01, text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
        'Referer': 'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0',
        'X-Requested-With': 'XMLHttpRequest'
    }

    def fetch_document_by_id(document):
        try:
            response = request_get(f'https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id={document["id"]}&cvm=true&#toolbar=0', headers=headers)

            html_cropped_body = base64.b64decode(response.text).decode('utf-8')[1050:]

            return html_cropped_body
        except:
            log_error(f'Error fetching document from id {document["id"]} document for CNPJ {cnpj}: {traceback.format_exc()}')
            return None

    try:
        base_url = f'https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d={document_configs["d"]}&s=0&l={document_configs.get("num_results", 10)}&o%5B0%5D%5BdataEntrega%5D=desc&tipoFundo=1&idCategoriaDocumento={document_configs["idCategoriaDocumento"]}&idTipoDocumento={document_configs["idTipoDocumento"]}&idEspecieDocumento=0&situacao=A&cnpj={cnpj}&cnpjFundo={cnpj}&isSession=false&_=1754204469153'

        day_month = document_configs.get('day_month')
        date_limitter_path = f'&dataInicial={day_month}%2F{document_configs["year"] -1}&dataFinal={day_month}%2F{document_configs["year"]}' if day_month else '&ultimaDataReferencia=true'

        response = request_get(f'{base_url}{date_limitter_path}', headers=headers)
        documents = response.json()

        final_documents = [ fetch_document_by_id(document) for document in documents['data'] ]

        return final_documents
    except:
        log_error(f'Error fetching all {document_configs["type"]} document for CNPJ {cnpj}: {traceback.format_exc()}')
        return None

def get_informe_mensal_estruturado_docs(cnpj):
    doc_configs = {
        'd': 3,
        'idCategoriaDocumento': 6,
        'idTipoDocumento': 40,
        'type': 'Informe Mensal Estruturado'
    }
    return fetch_documents(cnpj, doc_configs)

def get_informe_trimestral_estruturado_docs(cnpj):
    doc_configs = {
        'd': 4,
        'idCategoriaDocumento': 6,
        'idTipoDocumento': 45,
        'type': 'Informe Trimestral Estruturado'
    }
    return fetch_documents(cnpj, doc_configs)

def get_rendimentos_amortizacoes_docs(cnpj):
    today = datetime.now()
    today_day_month = f'{today.day if today.day >= 10 else f"0{today.day}"}%2F{today.month if today.month >= 10 else f"0{today.month}"}'

    doc_configs = {
        'd': 5,
        'day_month': today_day_month,
        'idCategoriaDocumento': 14,
        'idTipoDocumento': 41,
        'num_results': 25,
        'type': 'Rendimentos Amortizacoes',
        'year': today.year
    }

    RA_docs = fetch_documents(cnpj, doc_configs)

    pattern_to_remove = '</td><td><span class="dado-valores">'

    simplified_RA_doc = { get_substring(doc, 'Data do pagamento', '</span>', pattern_to_remove): text_to_number(get_substring(doc, 'Valor do provento (R$/unidade)', '</span>', pattern_to_remove)) for doc in RA_docs }

    return simplified_RA_doc

def get_cnpj_from_investidor10(ticker):
    global investidor_10_preloaded_data

    patterns_to_remove = [ '<span>', '</span>', '<div class="value">' ]

    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': 'https://investidor10.com.br/fiis/mxrf11/',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0'
        }

        response = request_get(f'https://investidor10.com.br/fiis/{ticker}', headers)
        html_cropped_body = response.text[15898:]

        cnpj = get_substring(html_cropped_body, 'CNPJ', '</div>', patterns_to_remove)

        if cnpj:
          investidor_10_preloaded_data = (ticker, html_cropped_body)

        return cnpj
    except:
        investidor_10_preloaded_data = (None, None)
        log_error(f'Error fetching CNPJ on Investidor 10 "{ticker}": {traceback.format_exc()}')
        return None

def get_cnpj_from_fiis(ticker):
    global fiis_preloaded_data

    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://fiis.com.br',
            'Referer': 'https://fiis.com.br/lupa-de-fiis/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0'
        }

        response = request_get(f'https://fiis.com.br/{ticker}', headers=headers)
        html_page = response.text

        cnpj = get_substring(html_page, 'cnpj":"', '"', '\\')

        if cnpj:
          fiis_preloaded_data = (ticker, html_page)

        return cnpj
    except:
        fiis_preloaded_data = (None, None)
        log_error(f'Error fetching CNPJ on FIIs "{ticker}": {traceback.format_exc()}')
        return None

def get_cnpj_from_fundamentus(ticker):
    global fundamentus_preloaded_data

    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://fundamentus.com.br/index.php',
            'Referer': 'https://fundamentus.com.br/index.php',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0'
        }

        response = request_get(f'https://fundamentus.com.br/detalhes.php?papel={ticker}', headers)
        html_page = response.text

        if 'Nenhum papel encontrado' in html_page:
            raise

        cnpj = get_substring(html_page, 'abrirGerenciadorDocumentosCVM?cnpjFundo=', '">Pesquisar Documentos', '#')

        if cnpj:
          fundamentus_preloaded_data = (ticker, html_page)

        return cnpj
    except:
        fundamentus_preloaded_data = (None, None)
        log_error(f'Error fetching CNPJ on Fundamentus for "{ticker}": {traceback.format_exc()}')
        return None

def get_data_from_bmfbovespa(ticker, info_names):
    try:
        cnpj = (
            get_cnpj_from_fundamentus(ticker) or
            get_cnpj_from_fiis(ticker) or
            get_cnpj_from_investidor10(ticker)
        )

        if not cnpj:
            log_error(f'No CNPJ found for "{ticker}"')
            return None

        informe_mensal_estruturado_docs = get_informe_mensal_estruturado_docs(cnpj)
        informe_trimestral_estruturado_docs = get_informe_trimestral_estruturado_docs(cnpj)
        rendimentos_amortizacoes_docs = get_rendimentos_amortizacoes_docs(cnpj)

        converted_data = convert_bmfbovespa_data(
            informe_mensal_estruturado_docs,
            informe_trimestral_estruturado_docs,
            rendimentos_amortizacoes_docs,
            cnpj,
            info_names
        )
        log_debug(f'Converted BM & FBovespa data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching BM & FBovespa data for "{ticker}": {traceback.format_exc()}')
        return None

def convert_fundamentus_data(data, info_names):
    patterns_to_remove = [
        '</font>',
        '</span>',
        '</td>',
        '<a href="resultado.php?segmento=',
        '<font color="#306EFF">',
        '<font color="#F75D59">',
        '<span class="oscil">',
        '<span class="txt">',
        '<td class="data destaque w3">',
        '<td class="data w1">',
        '<td class="data w2">',
        '<td class="data w3">',
        '<td class="data">'
    ]

    def get_vacancy():
        vacancy_as_text = get_substring(data, 'Vacância Média</span>', '</span>', patterns_to_remove)
        vacancy_as_text = vacancy_as_text.replace('-', '').strip()
        return text_to_number(vacancy_as_text) if vacancy_as_text else None

    ALL_INFO = {
        'actuation': lambda: None,
        'assets_value': lambda: text_to_number(get_substring(data, '>Ativos</span>', '</span>', patterns_to_remove)),
        'cash_value': lambda: text_to_number(get_substring(data, 'Caixa\'', ']', [', data : ['])),
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None,
        'dy': lambda: text_to_number(get_substring(data, 'Div. Yield</span>', '</span>', patterns_to_remove)),
        'equity_price': lambda: text_to_number(get_substring(data, 'VP/Cota</span>', '</span>', patterns_to_remove)),
        'ffoy': lambda: text_to_number(get_substring(data, 'FFO Yield</span>', '</span>', patterns_to_remove)),
        'initial_date': lambda: None,
        'latest_dividend': lambda: text_to_number(get_substring(data, 'Dividendo/cota</span>', '</span>', patterns_to_remove)),
        'latests_dividends': lambda: None,
        'link': lambda: get_substring(data, '<a target="_blank" href="', '">Pesquisar', '#'),
        'liquidity': lambda: text_to_number(get_substring(data, 'Vol $ méd (2m)</span>', '</span>', patterns_to_remove)),
        'management': lambda: get_substring(data, 'Gestão</span>', '</span>', patterns_to_remove),
        'market_value': lambda: text_to_number(get_substring(data, 'Valor de mercado</span>', '</span>', patterns_to_remove)),
        'max_52_weeks': lambda: text_to_number(get_substring(data, 'Max 52 sem</span>', '</span>', patterns_to_remove)),
        'min_52_weeks': lambda: text_to_number(get_substring(data, 'Min 52 sem</span>', '</span>', patterns_to_remove)),
        'name': lambda: get_substring(data, 'Nome</span>', '</span>', patterns_to_remove),
        'net_equity_value': lambda: text_to_number(get_substring(data, 'Patrim Líquido</span>', '</span>', patterns_to_remove)),
        'price': lambda: text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        'pvp': lambda: text_to_number(get_substring(data, 'P/VP</span>', '</span>', patterns_to_remove)),
        'segment': lambda: get_substring(data, 'Mandato</span>', '</span>', patterns_to_remove),
        'target_public': lambda: None,
        'term': lambda: None,
        'total_issued_shares': lambda: text_to_number(get_substring(data, 'Nro. Cotas</span>', '</span>', patterns_to_remove)),
        'total_mortgage': lambda: None,
        'total_mortgage_value': lambda: None,
        'total_real_state': lambda: text_to_number(get_substring(data, 'Qtd imóveis</span>', '</span>', patterns_to_remove)),
        'total_real_state_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'total_stocks_fund_others_value': lambda: None,
        'type': lambda: None,
        'vacancy': get_vacancy,
        'variation_12m': lambda: text_to_number(get_substring(data, '12 meses</span>', '</span>', patterns_to_remove)),
        'variation_30d': lambda: text_to_number(get_substring(data, 'Mês</span>', '</span>', patterns_to_remove)),
        'avg_price': lambda: None,
        'mayer_multiple': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def get_data_from_fundamentus(ticker, info_names):
    global fundamentus_preloaded_data

    try:
        if fundamentus_preloaded_data[1] and ticker == fundamentus_preloaded_data[0]:
            converted_data = convert_fundamentus_data(fundamentus_preloaded_data[1], info_names)
            log_debug(f'Converted preloaded Fundamentus data: {converted_data}')
            return converted_data

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://fundamentus.com.br/index.php',
            'Referer': 'https://fundamentus.com.br/index.php',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0'
        }

        response = request_get(f'https://fundamentus.com.br/detalhes.php?papel={ticker}', headers)
        html_page = response.text

        if 'Nenhum papel encontrado' in html_page:
            log_error(f'No data found on Fundamentus data for "{ticker}"')
            return None

        converted_data = convert_fundamentus_data(html_page, info_names)
        log_debug(f'Converted fresh Fundamentus data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching Fundamentus data for "{ticker}": {traceback.format_exc()}')
        return None

def convert_infomoney_data(data, info_names):
    prices = [ text_to_number(price[2]) for price in data ]
    price = text_to_number(prices[0])
    avg_price = sum(prices) / len(prices)

    ALL_INFO = {
        'actuation': lambda: None,
        'assets_value': lambda: None,
        'cash_value': lambda: None,
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None,
        'dy': lambda: None,
        'equity_price': lambda: None,
        'ffoy': lambda: None,
        'initial_date': lambda: None,
        'latest_dividend': lambda: None,
        'latests_dividends': lambda: None,
        'link': lambda: f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={data["meta"]["cnpj"]}#',
        'liquidity': lambda: None,
        'management': lambda: None,
        'market_value': lambda: None,
        'max_52_weeks': lambda: max(prices),
        'min_52_weeks': lambda: min(prices),
        'name': lambda: None,
        'net_equity_value': lambda: None,
        'price': lambda: price,
        'pvp': lambda: None,
        'segment': lambda: None,
        'target_public': lambda: None,
        'term': lambda: None,
        'total_issued_shares': lambda: None,
        'total_mortgage': lambda: None,
        'total_mortgage_value': lambda: None,
        'total_real_state': lambda: None,
        'total_real_state_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'total_stocks_fund_others_value': lambda: None,
        'type': lambda: None,
        'vacancy': lambda: None,
        'variation_12m': lambda: None,
        'variation_30d': lambda: None,
        'avg_price': lambda: avg_price,
        'mayer_multiple': lambda: price / avg_price
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def get_data_from_infomoney(ticker, info_names):
    try:
        headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'pt-BR,pt;q=0.9,en;q=0.8,ko;q=0.7',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'dnt': '1',
            'origin': 'https://www.infomoney.com.br',
            'priority': 'u=1, i',
            'referer': 'https://www.infomoney.com.br/cotacoes/b3/acao/banco-do-brasil-bbas3/historico/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0',
            'x-requested-with': 'XMLHttpRequest'
        }

        base_url = 'https://www.infomoney.com.br/wp-json/infomoney/v1/quotes/history'

        today = datetime.now()
        today_day_month = f'{today.day if today.day >= 10 else f"0{today.day}"}%2F{today.month if today.month >= 10 else f"0{today.month}"}'

        past_200_days = today - timedelta(days=365)
        past_200_days_day_month = f'{past_200_days.day if past_200_days.day >= 10 else f"0{past_200_days.day}"}%2F{past_200_days.month if past_200_days.month >= 10 else f"0{past_200_days.month}"}'

        form_data = f'page=0&numberItems=200&initialDate={past_200_days_day_month}%2F{past_200_days.year}&finalDate={today_day_month}%2F{today.year}&symbol={ticker}'

        response = request_post(base_url, headers=headers, data=form_data)
        prices = response.json()

        converted_data = convert_infomoney_data(prices, info_names)
        log_debug(f'Converted fresh InfoMoney data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching InfoMoney data for "{ticker}": {traceback.format_exc()}')
        return None

def convert_fiis_data(data, info_names):
    ALL_INFO = {
        'actuation': lambda: data['category'][0] if 'valor' in data['meta'] else None,
        'assets_value': lambda: None,
        'cash_value': lambda: data['meta']['valor_caixa'] if 'gestao' in data['meta'] else None,
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None,
        'dy': lambda: data['meta']['dy'] if 'dy' in data['meta'] else None,
        'equity_price': lambda: data['meta']['valorpatrimonialcota'] if 'valorpatrimonialcota' in data['meta'] else None,
        'ffoy': lambda: None,
        'initial_date': lambda: data['meta']['firstdate'] if 'firstdate' in data['meta'] else None,
        'latest_dividend': lambda: data['meta']['lastdividend'] if 'lastdividend' in data['meta'] else None,
        'latests_dividends': lambda: data['meta']['currentsumdividends'] if 'avgdividend' in data['meta'] else None,
        'link': lambda: f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={data["meta"]["cnpj"]}#',
        'liquidity': lambda: data['meta']['liquidezmediadiaria'] if 'liquidezmediadiaria' in data['meta'] else None,
        'management': lambda: data['meta']['gestao'] if 'valor_caixa' in data['meta'] else None,
        'market_value': lambda: data['meta']['valormercado'] if 'valormercado' in data['meta'] else None,
        'max_52_weeks': lambda: data['meta']['max_52_semanas'] if 'max_52_semanas' in data['meta'] else None,
        'min_52_weeks': lambda: data['meta']['min_52_semanas'] if 'min_52_semanas' in data['meta'] else None,
        'name': lambda: data['meta']['name'] if 'name' in data['meta'] else None,
        'net_equity_value': lambda: data['meta']['patrimonio'] if 'patrimonio' in data['meta'] else None,
        'price': lambda: data['meta']['valor'] if 'valor' in data['meta'] else None,
        'pvp': lambda: data['meta']['pvp'] if 'pvp' in data['meta'] else None,
        'segment': lambda: data['meta']['segmento_ambima'] if 'segmento_ambima' in data['meta'] else None,
        'target_public': lambda: data['meta']['publicoalvo'] if 'publicoalvo' in data['meta'] else None,
        'term': lambda: data['meta']['prazoduracao'] if 'prazoduracao' in data['meta'] else None,
        'total_issued_shares': lambda: data['meta']['numero_cotas'] if 'numero_cotas' in data['meta'] else None,
        'total_mortgage': lambda: None,
        'total_mortgage_value': lambda: None,
        'total_real_state': lambda: data['meta']['assets_number'] if 'assets_number' in data['meta'] else None,
        'total_real_state_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'total_stocks_fund_others_value': lambda: None,
        'type': lambda: data['meta']['setor_atuacao'] if 'setor_atuacao' in data['meta'] else None,
        'vacancy': lambda: data['meta']['vacancia'] if 'vacancia' in data['meta'] else None,
        'variation_12m': lambda: data['meta']['valorizacao_12_meses'] if 'valorizacao_12_meses' in data['meta'] else None,
        'variation_30d': lambda: data['meta']['valorizacao_mes'] if 'valorizacao_mes' in data['meta'] else None,
        'avg_price': lambda: None,
        'mayer_multiple': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def get_data_from_fiis(ticker, info_names):
    global fiis_preloaded_data

    try:
        if fiis_preloaded_data[1] and ticker == fiis_preloaded_data[0]:
            converted_data = convert_fiis_data(fiis_preloaded_data[1], info_names)
            log_debug(f'Converted preloaded FIIs data: {converted_data}')
            return converted_data

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://fiis.com.br',
            'Referer': 'https://fiis.com.br/lupa-de-fiis/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0'
        }

        response = request_get(f'https://fiis.com.br/{ticker}', headers=headers)
        html_page = response.text

        raw_data = get_substring(html_page, 'var dataLayer_content', 'dataLayer.push')

        json_data = json.loads(raw_data.strip(';= '))['pagePostTerms']

        converted_data = convert_fiis_data(json_data, info_names)
        log_debug(f'Converted fresh FIIs data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching FIIs data for "{ticker}": {traceback.format_exc()}')
        return None

def convert_fundsexplorer_data(data, info_names):
    ALL_INFO = {
        'actuation': lambda: data['category'][0] if 'valor' in data['meta'] else None,
        'assets_value': lambda: None,
        'cash_value': lambda: data['meta']['valor_caixa'] if 'gestao' in data['meta'] else None,
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None,
        'dy': lambda: data['meta']['dy'] if 'dy' in data['meta'] else None,
        'equity_price': lambda: data['meta']['valorpatrimonialcota'] if 'valorpatrimonialcota' in data['meta'] else None,
        'ffoy': lambda: None,
        'initial_date': lambda: data['meta']['firstdate'] if 'firstdate' in data['meta'] else None,
        'latest_dividend': lambda: data['meta']['lastdividend'] if 'lastdividend' in data['meta'] else None,
        'latests_dividends': lambda: data['meta']['dividendos_12_meses'] if 'dividendos_12_meses' in data['meta'] else None,
        'link': lambda: f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={data["meta"]["cnpj"]}#',
        'liquidity': lambda: data['meta']['liquidezmediadiaria'] if 'liquidezmediadiaria' in data['meta'] else None,
        'management': lambda: data['meta']['gestao'] if 'valor_caixa' in data['meta'] else None,
        'market_value': lambda: data['meta']['valormercado'] if 'valormercado' in data['meta'] else None,
        'max_52_weeks': lambda: data['meta']['max_52_semanas'] if 'max_52_semanas' in data['meta'] else None,
        'min_52_weeks': lambda: data['meta']['min_52_semanas'] if 'min_52_semanas' in data['meta'] else None,
        'name': lambda: data['meta']['name'] if 'name' in data['meta'] else None,
        'net_equity_value': lambda: data['meta']['patrimonio'] if 'patrimonio' in data['meta'] else None,
        'price': lambda: data['meta']['valor'] if 'valor' in data['meta'] else None,
        'pvp': lambda: data['meta']['pvp'] if 'pvp' in data['meta'] else None,
        'segment': lambda: data['meta']['segmento_ambima'] if 'segmento_ambima' in data['meta'] else None,
        'target_public': lambda: data['meta']['publicoalvo'] if 'publicoalvo' in data['meta'] else None,
        'term': lambda: data['meta']['prazoduracao'] if 'prazoduracao' in data['meta'] else None,
        'total_issued_shares': lambda: data['meta']['numero_cotas'] if 'numero_cotas' in data['meta'] else None,
        'total_mortgage_value': lambda: None,
        'total_mortgage': lambda: None,
        'total_real_state_value': lambda: None,
        'total_real_state': lambda: data['meta']['assets_number'] if 'assets_number' in data['meta'] else None,
        'total_stocks_fund_others_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'type': lambda: data['meta']['setor_atuacao'] if 'setor_atuacao' in data['meta'] else None,
        'vacancy': lambda: data['meta']['vacancia'] if 'vacancia' in data['meta'] else None,
        'variation_12m': lambda: data['meta']['valorizacao_12_meses'] if 'valorizacao_12_meses' in data['meta'] else None,
        'variation_30d': lambda: data['meta']['valorizacao_mes'] if 'valorizacao_mes' in data['meta'] else None,
        'avg_price': lambda: None,
        'mayer_multiple': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

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
        raw_data = get_substring(html_page, 'var dataLayer_content', 'dataLayer.push')

        json_data = json.loads(raw_data.strip(';= '))['pagePostTerms']

        converted_data = convert_fundsexplorer_data(json_data, info_names)
        log_debug(f'Converted Fundsexplorer: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data from Fundsexplorer for "{ticker}": {traceback.format_exc()}')
        return None

def convert_investidor10_data(data, info_names):
    patterns_to_remove = [
        '</div>',
        '</span>',
        '<div class="_card-body">',
        '<div class="value">',
        '<div>',
        '<span class="content--info--item--value">',
        '<span class="value">',
        '<span>'
    ]

    def multiply_by_unit(data):
        if not data:
            return None

        if 'K' in data:
            return text_to_number(data.replace('Mil', '').replace('K', '')) * 1_000
        elif 'M' in data:
            return text_to_number(data.replace('Milhão', '').replace('Milhões', '').replace('M', '')) * 1_000_000
        elif 'B' in data:
            return text_to_number(data.replace('Bilhão', '').replace('Bilhões', '').replace('B', '')) * 1_000_000_000

        return text_to_number(data)

    count_pattern_on_text = lambda text, pattern: None if not text or not pattern else len(text.split(pattern))

    ALL_INFO = {
        'actuation': lambda: None,
        'assets_value': lambda: None,
        'cash_value': lambda: None,
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None,
        'dy': lambda: text_to_number(get_substring(data, 'DY (12M)</span>', '</span>', patterns_to_remove)),
        'equity_price': lambda: text_to_number(get_substring(data, 'VAL. PATRIMONIAL P/ COTA', '<div class=\'cell\'>', patterns_to_remove)),
        'ffoy': lambda: None,
        'initial_date': lambda: None,
        'latest_dividend': lambda: text_to_number(get_substring(data, 'ÚLTIMO RENDIMENTO', '</div>', patterns_to_remove)),
        'latests_dividends': lambda: text_to_number(get_substring(get_substring(data, 'YIELD 12 MESES', '</div>'), 'amount">', '</span>', patterns_to_remove)),
        'link': lambda: f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={get_substring(data, "CNPJ", "</div>", patterns_to_remove)}#',
        'liquidity': lambda: multiply_by_unit(get_substring(data, 'title="Liquidez Diária">Liquidez Diária</span>', '</span>', patterns_to_remove)),
        'management': lambda: get_substring(data, 'TIPO DE GESTÃO', '<div class=\'cell\'>', patterns_to_remove),
        'market_value': lambda: None,
        'max_52_weeks': lambda: None,
        'min_52_weeks': lambda: None,
        'name': lambda: get_substring(data, 'Razão Social', '<div class=\'cell\'>', patterns_to_remove),
        'net_equity_value': lambda: multiply_by_unit(get_substring(data, 'VALOR PATRIMONIAL</span>', '</span>', patterns_to_remove)),
        'price': lambda: text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        'pvp': lambda: text_to_number(get_substring(data, 'title="P/VP">P/VP</span>', '</span>', patterns_to_remove)),
        'segment': lambda: get_substring(data, 'SEGMENTO', '<div class=\'cell\'>', patterns_to_remove),
        'target_public': lambda: get_substring(data, 'PÚBLICO-ALVO', '<div class=\'cell\'>', patterns_to_remove),
        'term': lambda: get_substring(data, 'PRAZO DE DURAÇÃO', '<div class=\'cell\'>', patterns_to_remove),
        'total_issued_shares': lambda: text_to_number(get_substring(data, 'COTAS EMITIDAS', '<div class=\'cell\'>', patterns_to_remove)),
        'total_mortgage': lambda: None,
        'total_mortgage_value': lambda: None,
        'total_real_state': lambda: count_pattern_on_text(get_substring(data, 'Lista de Imóveis', '</section>'), 'card-propertie'),
        'total_real_state_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'total_stocks_fund_others_value': lambda: None,
        'type': lambda: get_substring(data, 'TIPO DE FUNDO', '<div class=\'cell\'>', patterns_to_remove),
        'vacancy': lambda: text_to_number(get_substring(data, 'VACÂNCIA', '<div class=\'cell\'>', patterns_to_remove)),
        'variation_12m': lambda: text_to_number(get_substring(data, 'title="Variação (12M)">VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove)),
        'variation_30d': lambda: None,
        'avg_price': lambda: None,
        'mayer_multiple': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def get_data_from_investidor10(ticker, info_names):
    global investidor_10_preloaded_data

    try:
        if investidor_10_preloaded_data[1] and ticker == investidor_10_preloaded_data[0]:
            converted_data = convert_investidor10_data(investidor_10_preloaded_data[1], info_names)
            log_debug(f'Converted preloaded Investidor 10 data: {converted_data}')
            return converted_data

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://investidor10.com.br/fiis/mxrf11/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0'
        }

        response = request_get(f'https://investidor10.com.br/fiis/{ticker}', headers)
        html_cropped_body = response.text[15898:]

        converted_data = convert_investidor10_data(html_cropped_body, info_names)
        log_debug(f'Converted fresh Investidor 10 data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data from Investidor 10 for "{ticker}": {traceback.format_exc()}')
        return None

def filter_remaining_infos(data, info_names, default_info_names=None):
    if not data:
        return info_names

    missing_info = [ info for info in info_names if info in data and data[info] is None ]

    return missing_info if missing_info else default_info_names

def combine_data(first_dict, second_dict, info_names):
    if first_dict and second_dict:
        combined_dict = {**first_dict, **second_dict}
        log_debug(f'Data from combined Frist and Second Dictionaries: {combined_dict}')
    elif first_dict:
        combined_dict = first_dict
        log_debug(f'Data from First Dictionary only: {combined_dict}')
    elif second_dict:
        combined_dict = second_dict
        log_debug(f'Data from Second Dictionary only: {combined_dict}')
    else:
        combined_dict = {}
        log_debug('No combined data')

    missing_combined_infos = filter_remaining_infos(combined_dict, info_names)
    log_debug(f'Missing info from Combined data: {missing_combined_infos}')
    return combined_dict, missing_combined_infos

def get_data_from_all_sources(ticker, info_names):
    data_bmfbovespa = get_data_from_bmfbovespa(ticker, info_names)
    log_info(f'Data from BM & FBovespa: {data_bmfbovespa}')

    missing_bmfbovespa_infos = filter_remaining_infos(data_bmfbovespa, info_names)
    log_debug(f'Missing info from BM & FBovespa: {missing_bmfbovespa_infos}')

    if data_bmfbovespa and not missing_bmfbovespa_infos:
        return data_bmfbovespa

    data_fundamentus = get_data_from_fundamentus(ticker, missing_bmfbovespa_infos or info_names)
    log_info(f'Data from Fundamentus: {data_fundamentus}')

    combined_data, missing_combined_infos = combine_data(data_bmfbovespa, data_fundamentus, info_names)
    log_debug(f'Missing info from BM & FBovespa or Fundamentus: {missing_combined_infos}')

    if combined_data and not missing_combined_infos:
        return combined_data

    data_infomoney = get_data_from_infomoney(ticker, missing_combined_infos or info_names)
    log_info(f'Data from InfoMoney: {data_infomoney}')

    combined_data, missing_combined_infos = combine_data(combined_data, data_infomoney, info_names)
    log_debug(f'Missing info from BM & FBovespa, Fundamentus or InfoMoney: {missing_combined_infos}')

    if combined_data and not missing_combined_infos:
        return combined_data

    data_fiis = get_data_from_fiis(ticker, missing_combined_infos or info_names)
    log_info(f'Data from FIIs: {data_fiis}')

    combined_data, missing_combined_infos = combine_data(combined_data, data_fiis, info_names)
    log_debug(f'Missing info from BM & FBovespa, Fundamentus, InfoMoney or FIIs: {missing_combined_infos}')

    if combined_data and not missing_combined_infos:
        return combined_data

    data_investidor_10 = get_data_from_investidor10(ticker, missing_combined_infos or info_names)
    log_info(f'Data from Investidor 10: {data_investidor_10}')

    if not data_investidor_10:
        return combined_data

    return { **combined_data, **data_investidor_10 }

def get_data_from_sources(ticker, source, info_names):
    SOURCES = {
        VALID_SOURCES['BMFBOVESPA_SOURCE']: get_data_from_bmfbovespa,
        VALID_SOURCES['FIIS_SOURCE']: get_data_from_fiis,
        VALID_SOURCES['FUNDAMENTUS_SOURCE']: get_data_from_fundamentus,
        VALID_SOURCES['FUNDSEXPLORER_SOURCE']: get_data_from_fundsexplorer,
        VALID_SOURCES['INVESTIDOR10_SOURCE']: get_data_from_investidor10,
        VALID_SOURCES['INFOMONEY_SOURCE']: get_data_from_infomoney
    }

    fetch_function = SOURCES.get(source, get_data_from_all_sources)
    return fetch_function(ticker, info_names)

def get_data_from_cache(ticker, info_names, can_use_cache):
    if not can_use_cache:
        return None

    cached_data = read_cache(ticker)
    if not cached_data:
        return None

    filtered_data = { key: cached_data[key] for key in info_names if key in cached_data }
    log_info(f'Data from Cache: {filtered_data}')

    return filtered_data

def get_data(ticker, source, info_names, can_use_cache):
    cached_data = get_data_from_cache(ticker, info_names, can_use_cache)

    SHOULD_UPDATE_CACHE = True

    if not can_use_cache:
        return not SHOULD_UPDATE_CACHE, get_data_from_sources(ticker, source, info_names)

    missing_cache_info_names = filter_remaining_infos(cached_data, info_names)

    if not missing_cache_info_names:
        return not SHOULD_UPDATE_CACHE, cached_data

    source_data = get_data_from_sources(ticker, source, missing_cache_info_names)

    if cached_data and source_data:
        return SHOULD_UPDATE_CACHE, { **cached_data, **source_data }
    elif cached_data and not source_data:
        return not SHOULD_UPDATE_CACHE, cached_data
    elif not cached_data and source_data:
        return SHOULD_UPDATE_CACHE, source_data

    return not SHOULD_UPDATE_CACHE, None

def get_parameter_info(params, name, default=None):
    return params.get(name, default).replace(' ', '').lower()

def get_cache_parameter_info(params, name, default='0'):
    return get_parameter_info(params, name, default) in { '1', 's', 'sim', 't', 'true', 'y', 'yes' }

@app.route('/fii/<ticker>', methods=['GET'])
def get_fii_data(ticker):
    should_delete_all_cache = get_cache_parameter_info(request.args, 'should_delete_all_cache')
    should_clear_cached_data = get_cache_parameter_info(request.args, 'should_clear_cached_data')
    should_use_cache = get_cache_parameter_info(request.args, 'should_use_cache', '1')

    ticker = ticker.upper()

    raw_source = get_parameter_info(request.args, 'source', VALID_SOURCES['ALL_SOURCE'])
    source = raw_source if raw_source in VALID_SOURCES.values() else VALID_SOURCES['ALL_SOURCE']

    raw_info_names = [ info for info in get_parameter_info(request.args, 'info_names', '').split(',') if info in VALID_INFOS ]
    info_names = raw_info_names if len(raw_info_names) else VALID_INFOS

    log_debug(f'Should Delete cache? {should_delete_all_cache} - Should Clear cache? {should_clear_cached_data} - Should Use cache? {should_use_cache}')
    log_debug(f'Ticker: {ticker} - Source: {source} - Info names: {info_names}')

    can_use_cache = preprocess_cache(ticker, should_delete_all_cache, should_clear_cached_data, should_use_cache)

    should_update_cache, data = get_data(ticker, source, info_names, can_use_cache)

    log_debug(f'Final Data: {data}')

    if not data:
        return jsonify({ 'error': 'No data found' }), 404

    if can_use_cache and should_update_cache:
        upsert_cache(ticker, data)

    return jsonify(data), 200

if __name__ == '__main__':
    log_debug('Starting fiiCrawler API')
    app.run(debug=LOG_LEVEL == 'DEBUG')
