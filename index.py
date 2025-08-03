import ast
import base64
from datetime import datetime, timedelta
from hashlib import sha512
import json
from logging import CRITICAL, DEBUG, ERROR, Formatter, getLevelName, getLogger, INFO, StreamHandler
import os
import re
import traceback

from flask import Flask, jsonify, request

import requests

DATE_FORMAT = '%d-%m-%Y %H:%M:%S'

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

VALID_SOURCES = {
    'FUNDAMENTUS_SOURCE': 'fundamentus',
    'INVESTIDOR10_SOURCE': 'investidor10',
    'FUNDSEXPLORER_SOURCE': 'fundsexplorer',
    'FIIS_SOURCE': 'fiis',
    'ALL_SOURCE': 'all'
}

VALID_INFOS = [
    'actuation',
    'assets_value',
    'cash_value',
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
    'total_real_state',
    'total_real_state_value',
    'total_mortgage',
    'total_mortgage_value',
    'total_stocks_fund_others',
    'total_stocks_fund_others_value',
    'type',
    'vacancy',
    'variation_12m',
    'variation_30d',
    'debit_by_real_state_acquisition',
    'debit_by_securitization_receivables_acquisition'
]

LOG_RUNNER = os.environ.get('LOG_RUNNER', 'LOGGING')
LOG_LEVEL = os.environ.get('LOG_LEVEL', ERROR)

if LOG_RUNNER == 'LOGGING':
    logger = getLogger(__name__)
    logger.setLevel(LOG_LEVEL)
    logger_handler = StreamHandler()
    logger_handler.setFormatter(Formatter(fmt='%(asctime)s - %(levelname)s - %(message)s', datefmt=DATE_FORMAT))
    logger.addHandler(logger_handler)

app = Flask(__name__)
app.json.sort_keys = False

def print_runner(message, level):
    if LOG_RUNNER == 'PRINTER' and level == LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {getLevelName(level)} - {message}')

def log_runner(message, level):
    if LOG_RUNNER == 'LOGGING':
        if level == ERROR:
            logger.error(message)
        elif level == INFO:
            logger.info(message)
        else:
            logger.debug(message)

def log(message, level=DEBUG):
      log_runner(message, level)
      print_runner(message, level)

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

    log(f'Response from {url} : {response}')

    return response

def convert_fundamentus_data(data, doc_IME, doc_ITE, cnpj, info_names):
    patterns_to_remove = [
        '</span>',
        '<span class="txt">',
        '<span class="oscil">',
        '<span class="dado-valores">',
        '<span class="dado-cabecalho">',
        '<font color="#F75D59">',
        '<font color="#306EFF">',
        '<td>',
        '</td>',
        '<td align="center">',
        '<td class="data">',
        '<td class="data w1">',
        '<td class="data w2">',
        '<td class="data w3">',
        '<td class="data destaque w3">',
        '<center>',
        '<a href="resultado.php?segmento='
    ]

    def count_total_real_state_value():
        return text_to_number(get_substring(doc_IME, 'Direitos reais sobre bens im&oacute;veis ', '</span>', patterns_to_remove))

    def count_total_mortgage_value():
        return (
            text_to_number(get_substring(doc_IME, 'Certificados de Dep&oacute;sitos de Valores Mobili&aacute;rios', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(doc_IME, 'Notas Promiss&oacute;rias', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(doc_IME, 'Notas Comerciais', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(doc_IME, 'CRI" (se FIAGRO, Certificado de Receb&iacute;veis do Agroneg&oacute;cio "CRA")', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(doc_IME, 'Hipotec&aacute;rias', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(doc_IME, 'LCI" (se FIAGRO, Letras de Cr&eacute;dito do Agroneg&oacute;cio "LCA")', '</span>', patterns_to_remove)) +
            text_to_number(get_substring(doc_IME, 'LIG)', '</span>', patterns_to_remove))
        )

    def count_total_stocks_fund_others_value():
        return (
                text_to_number(get_substring(doc_IME, 'A&ccedil;&otilde;es', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(doc_IME, 'Deb&ecirc;ntures', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(doc_IME, 'certificados de desdobramentos', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(doc_IME, 'FIA)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(doc_IME, 'FIP)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(doc_IME, 'FII)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(doc_IME, 'FIDC)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(doc_IME, 'Outras cotas de Fundos de Investimento', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(doc_IME, 'A&ccedil;&otilde;es de Sociedades cujo &uacute;nico prop&oacute;sito se enquadra entre as atividades permitidas aos FII ', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(doc_IME, 'Cotas de Sociedades que se enquadre entre as atividades permitidas aos FII', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(doc_IME, 'CEPAC)', '</span>', patterns_to_remove)) +
                text_to_number(get_substring(doc_IME, 'Outros Valores Mobili&aacute;rios ', '</span>', patterns_to_remove))
            )

    def fii_type():
        fii_type = {
            'Tijolo': count_total_real_state_value(),
            'Papel': count_total_mortgage_value(),
            'Outro': count_total_stocks_fund_others_value()
        }

        return max(fii_type, key=fii_type.get)

    ALL_INFO = {
        #'name': lambda: get_substring(data, 'Nome</span>', '</span>', patterns_to_remove),
        'name': lambda: get_substring(doc_IME, 'Nome do Fundo/Classe: </span></td><td><span class="dado-cabecalho">', '</span>', patterns_to_remove),
        'type': fii_type,
        #'segment': lambda: get_substring(data, 'Mandato</span>', '</span>', patterns_to_remove),
        'segment': lambda: get_substring(doc_IME, 'Segmento de Atua&ccedil;&atilde;o: </b>', '</span>', patterns_to_remove),
        'actuation': lambda: None,
        'link': lambda: f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={cnpj}#',
        'price': lambda: text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        'liquidity': lambda: text_to_number(get_substring(data, 'Vol $ méd (2m)</span>', '</span>', patterns_to_remove)),
        #'total_issued_shares': lambda: text_to_number(get_substring(data, 'Nro. Cotas</span>', '</span>', patterns_to_remove)),
        'total_issued_shares': lambda: text_to_number(get_substring(doc_IME, 'Quantidade de cotas emitidas: </span></td><td><span class="dado-cabecalho">', '</span>', patterns_to_remove)),
        #'net_equity_value': lambda: text_to_number(get_substring(data, 'Patrim Líquido</span>', '</span>', patterns_to_remove)),
        'net_equity_value': lambda: text_to_number(get_substring(doc_IME, 'Patrim&ocirc;nio L&iacute;quido &ndash; R$ </b></td><td><span class="dado-valores">', '</span>', patterns_to_remove)),
        #'equity_price': lambda: text_to_number(get_substring(data, 'VP/Cota</span>', '</span>', patterns_to_remove)),
        'equity_price': lambda: text_to_number(get_substring(doc_IME, 'Valor Patrimonial das Cotas &ndash; R$ </b></td><td><span class="dado-valores">', '</span>', patterns_to_remove)),
        'variation_12m': lambda: text_to_number(get_substring(data, '12 meses</span>', '</span>', patterns_to_remove)),
        'variation_30d': lambda: text_to_number(get_substring(data, 'Mês</span>', '</span>', patterns_to_remove)),
        'min_52_weeks': lambda: text_to_number(get_substring(data, 'Min 52 sem</span>', '</span>', patterns_to_remove)),
        'max_52_weeks': lambda: text_to_number(get_substring(data, 'Max 52 sem</span>', '</span>', patterns_to_remove)),
        'pvp': lambda: text_to_number(get_substring(data, 'P/VP</span>', '</span>', patterns_to_remove)),
        'dy': lambda: text_to_number(get_substring(data, 'Div. Yield</span>', '</span>', patterns_to_remove)),
        'latests_dividends': lambda: None,
        'latest_dividend': lambda: text_to_number(get_substring(data, 'Dividendo/cota</span>', '</span>', patterns_to_remove)),
        'ffoy': lambda: text_to_number(get_substring(data, 'FFO Yield</span>', '</span>', patterns_to_remove)),
        'vacancy': lambda: text_to_number(get_substring(data, 'Vacância Média</span>', '</span>', patterns_to_remove).replace('-', '').strip()),
        #'total_real_state': lambda: text_to_number(get_substring(data, 'Qtd imóveis</span>', '</span>', patterns_to_remove)),
        'total_real_state': lambda: doc_ITE.count('&Aacute;rea (m2):') + (get_substring(doc_ITE, '1.1.1', '>1.1.2<', patterns_to_remove).count('</tr>') - 2),
        'total_real_state_value': count_total_real_state_value,
        'total_mortgage': lambda: get_substring(doc_ITE, ' 1.2.2', '1.2.6', patterns_to_remove).count('</tr>') - 8,
        'total_mortgage_value': count_total_mortgage_value,
        'total_stocks_fund_others': lambda: (get_substring(doc_ITE, ' 1.2.1', ' 1.2.2', patterns_to_remove).count('</tr>') - 2) + (get_substring(doc_ITE, ' 1.2.6', '>1.3<', patterns_to_remove).count('</tr>') - 16),
        'total_stocks_fund_others_value': count_total_stocks_fund_others_value,
        #'management': lambda: get_substring(data, 'Gestão</span>', '</span>', patterns_to_remove),
        'management': lambda: get_substring(doc_IME, 'Tipo de Gest&atilde;o: </b>', '</span>', patterns_to_remove),
        #'cash_value': lambda: get_substring(data, 'Caixa\'', ']', [', data : [']),
        'cash_value': lambda: text_to_number(get_substring(doc_IME, 'Total mantido para as Necessidades de Liquidez (art. 46, &sect; &uacute;nico, ICVM 472/08) </b></td><td><b><span class="dado-valores">', '</span>', patterns_to_remove)),
        #'assets_value': lambda: text_to_number(get_substring(data, '>Ativos</span>', '</span>', patterns_to_remove)),
        'assets_value': lambda: text_to_number(get_substring(doc_IME, 'Ativo &ndash; R$ </b></td><td>', '</span>', patterns_to_remove)),
        'market_value': lambda: text_to_number(get_substring(data, 'Valor de mercado</span>', '</span>', patterns_to_remove)),
        'initial_date': lambda: get_substring(doc_IME, 'doc de Funcionamento: </span></td><td><span class="dado-cabecalho">', '</span>', patterns_to_remove),
        'target_public': lambda: get_substring(doc_IME, 'P&uacute;blico Alvo: </span></td><td><span class="dado-cabecalho">', '</span>', patterns_to_remove),
        'term': lambda: get_substring(doc_IME, '>Prazo de Dura&ccedil;&atilde;o: </span></td>', '</span>', patterns_to_remove),
        'debit_by_real_state_acquisition': lambda: text_to_number(get_substring(doc_IME, 'Obriga&ccedil;&otilde;es por aquisi&ccedil;&atilde;o de im&oacute;veis </b></td><td><span class="dado-valores">', '</span>', patterns_to_remove)),
        'debit_by_securitization_receivables_acquisition': lambda: text_to_number(get_substring(doc_IME, 'Obriga&ccedil;&otilde;es por securitiza&ccedil;&atilde;o de receb&iacute;veis </b></td><td><span class="dado-valores">', '</span>', patterns_to_remove))
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def get_informe_mensal_estruturado_doc(cnpj):
    try:
        url = f'https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d=3&s=0&l=10&o%5B0%5D%5BdataEntrega%5D=desc&tipoFundo=1&idCategoriaDocumento=6&idTipoDocumento=40&idEspecieDocumento=0&situacao=A&cnpj={cnpj}&cnpjFundo={cnpj}&ultimaDataReferencia=true&isSession=false&_=1753033563564'

        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01, text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Referer': 'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0',
            'X-Requested-With': 'XMLHttpRequest'
        }

        response = request_get(url, headers)
        documents_json = response.json()

        document_id = documents_json['data'][0]['id']

        url = f'https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id={document_id}&cvm=true&#toolbar=0'

        response = requests.get(url, headers)
        html_doc = base64.b64decode(response.text).decode('utf-8')[1050:]

        return html_doc
    except Exception as error:
        log(f'Error on get Informe Mensal doc: {traceback.format_exc()}', ERROR)

    return None

def get_informe_trimestral_estruturado_doc(cnpj):
    try:
        url = f'https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d=4&s=0&l=10&o%5B0%5D%5BdataEntrega%5D=desc&tipoFundo=1&idCategoriaDocumento=6&idTipoDocumento=45&idEspecieDocumento=0&cnpj={cnpj}&cnpjFundo={cnpj}&ultimaDataReferencia=true&isSession=false&_=1754091003852'

        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01, text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Referer': 'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0',
            'X-Requested-With': 'XMLHttpRequest'
        }

        response = request_get(url, headers)
        documents_json = response.json()

        document_id = documents_json['data'][0]['id']

        url = f'https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id={document_id}&cvm=true&#toolbar=0'

        response = requests.get(url, headers)
        html_doc = base64.b64decode(response.text).decode('utf-8')[1050:]

        return html_doc
    except Exception as error:
        log(f'Error on get Informe Trimestral doc: {traceback.format_exc()}', ERROR)

    return None

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

        cnpj = get_substring(html_page, 'abrirGerenciadorDocumentosCVM?cnpjFundo=', '">Pesquisar Documentos', '#')
        informe_mensal_estruturado_doc = get_informe_mensal_estruturado_doc(cnpj)
        informe_trimestral_estruturado_doc = get_informe_trimestral_estruturado_doc(cnpj)

        log(f'Converted Fundamentus data: {convert_fundamentus_data(html_page, informe_mensal_estruturado_doc, informe_trimestral_estruturado_doc, cnpj, info_names)}')
        return convert_fundamentus_data(html_page, informe_mensal_estruturado_doc, informe_trimestral_estruturado_doc, cnpj, info_names)
    except Exception as error:
        log(f'Error on get Fundamentus data: {traceback.format_exc()}', ERROR)

    return None


def convert_fiis_data(data, info_names):
    ALL_INFO = {
        'name': lambda: data['meta']['name'] if 'name' in data['meta'] else None,
        'type': lambda: data['meta']['setor_atuacao'] if 'setor_atuacao' in data['meta'] else None,
        'segment': lambda: data['meta']['segmento_ambima'] if 'segmento_ambima' in data['meta'] else None,
        'actuation': lambda: data['category'][0] if 'valor' in data['meta'] else None,
        'link': lambda: None,
        'price': lambda: data['meta']['valor'] if 'valor' in data['meta'] else None,
        'liquidity': lambda: data['meta']['liquidezmediadiaria'] if 'liquidezmediadiaria' in data['meta'] else None,
        'total_issued_shares': lambda: data['meta']['numero_cotas'] if 'numero_cotas' in data['meta'] else None,
        'net_equity_value': lambda: data['meta']['patrimonio'] if 'patrimonio' in data['meta'] else None,
        'equity_price': lambda: data['meta']['valorpatrimonialcota'] if 'valorpatrimonialcota' in data['meta'] else None,
        'variation_12m': lambda: data['meta']['valorizacao_12_meses'] if 'valorizacao_12_meses' in data['meta'] else None,
        'variation_30d': lambda: data['meta']['valorizacao_mes'] if 'valorizacao_mes' in data['meta'] else None,
        'min_52_weeks': lambda: data['meta']['min_52_semanas'] if 'min_52_semanas' in data['meta'] else None,
        'max_52_weeks': lambda: data['meta']['max_52_semanas'] if 'max_52_semanas' in data['meta'] else None,
        'pvp': lambda: data['meta']['pvp'] if 'pvp' in data['meta'] else None,
        'dy': lambda: data['meta']['dy'] if 'dy' in data['meta'] else None,
        'latests_dividends': lambda: data['meta']['currentsumdividends'] if 'avgdividend' in data['meta'] else None,
        'latest_dividend': lambda: data['meta']['lastdividend'] if 'lastdividend' in data['meta'] else None,
        'ffoy': lambda: None,
        'vacancy': lambda: data['meta']['vacancia'] if 'vacancia' in data['meta'] else None,
        'total_real_state': lambda: data['meta']['assets_number'] if 'assets_number' in data['meta'] else None,
        'total_real_state_value': lambda: None,
        'total_mortgage': lambda: None,
        'total_mortgage_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'total_stocks_fund_others_value': lambda: None,
        'management': lambda: data['meta']['gestao'] if 'valor_caixa' in data['meta'] else None,
        'cash_value': lambda: data['meta']['valor_caixa'] if 'gestao' in data['meta'] else None,
        'assets_value': lambda: None,
        'market_value': lambda: data['meta']['valormercado'] if 'valormercado' in data['meta'] else None,
        'initial_date': lambda: data['meta']['firstdate'] if 'firstdate' in data['meta'] else None,
        'target_public': lambda: data['meta']['publicoalvo'] if 'publicoalvo' in data['meta'] else None,
        'term': lambda: data['meta']['prazoduracao'] if 'prazoduracao' in data['meta'] else None,
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def get_data_from_fiis(ticker, info_names):
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

        data_as_text = get_substring(html_page, 'var dataLayer_content', 'dataLayer.push')

        data_as_json = json.loads(data_as_text.strip(';= '))['pagePostTerms']

        log(f'Converted FIIs data: {convert_fiis_data(data_as_json, info_names)}')
        return convert_fiis_data(data_as_json, info_names)
    except Exception as error:
        log(f'Error on get FIIs data: {traceback.format_exc()}', ERROR)

    return None

def convert_fundsexplorer_data(data, info_names):
    ALL_INFO = {
        'name': lambda: data['meta']['name'] if 'name' in data['meta'] else None,
        'type': lambda: data['meta']['setor_atuacao'] if 'setor_atuacao' in data['meta'] else None,
        'segment': lambda: data['meta']['segmento_ambima'] if 'segmento_ambima' in data['meta'] else None,
        'actuation': lambda: data['category'][0] if 'valor' in data['meta'] else None,
        'link': lambda: None,
        'price': lambda: data['meta']['valor'] if 'valor' in data['meta'] else None,
        'liquidity': lambda: data['meta']['liquidezmediadiaria'] if 'liquidezmediadiaria' in data['meta'] else None,
        'total_issued_shares': lambda: data['meta']['numero_cotas'] if 'numero_cotas' in data['meta'] else None,
        'net_equity_value': lambda: data['meta']['patrimonio'] if 'patrimonio' in data['meta'] else None,
        'equity_price': lambda: data['meta']['valorpatrimonialcota'] if 'valorpatrimonialcota' in data['meta'] else None,
        'variation_12m': lambda: data['meta']['valorizacao_12_meses'] if 'valorizacao_12_meses' in data['meta'] else None,
        'variation_30d': lambda: data['meta']['valorizacao_mes'] if 'valorizacao_mes' in data['meta'] else None,
        'min_52_weeks': lambda: data['meta']['min_52_semanas'] if 'min_52_semanas' in data['meta'] else None,
        'max_52_weeks': lambda: data['meta']['max_52_semanas'] if 'max_52_semanas' in data['meta'] else None,
        'pvp': lambda: data['meta']['pvp'] if 'pvp' in data['meta'] else None,
        'dy': lambda: data['meta']['dy'] if 'dy' in data['meta'] else None,
        'latests_dividends': lambda: data['meta']['dividendos_12_meses'] if 'dividendos_12_meses' in data['meta'] else None,
        'latest_dividend': lambda: data['meta']['lastdividend'] if 'lastdividend' in data['meta'] else None,
        'ffoy': lambda: None,
        'vacancy': lambda: data['meta']['vacancia'] if 'vacancia' in data['meta'] else None,
        'total_real_state': lambda: data['meta']['assets_number'] if 'assets_number' in data['meta'] else None,
        'total_real_state_value': lambda: None,
        'total_mortgage': lambda: None,
        'total_mortgage_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'total_stocks_fund_others_value': lambda: None,
        'management': lambda: data['meta']['gestao'] if 'valor_caixa' in data['meta'] else None,
        'cash_value': lambda: data['meta']['valor_caixa'] if 'gestao' in data['meta'] else None,
        'assets_value': lambda: None,
        'market_value': lambda: data['meta']['valormercado'] if 'valormercado' in data['meta'] else None,
        'initial_date': lambda: data['meta']['firstdate'] if 'firstdate' in data['meta'] else None,
        'target_public': lambda: data['meta']['publicoalvo'] if 'publicoalvo' in data['meta'] else None,
        'term': lambda: data['meta']['prazoduracao'] if 'prazoduracao' in data['meta'] else None,
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None
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

        log(f"Converted Fundsexplorer data: {convert_fundsexplorer_data(data_as_json)}")
        return convert_fundsexplorer_data(data_as_json, info_names)
    except Exception as error:
        log(f"Error on get Fundsexplorer data: {traceback.format_exc()}", ERROR)

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
        'variation_12m': lambda: text_to_number(get_substring(data, 'title="Variação (12M)">VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove)),
        'variation_30d': lambda: None,
        'min_52_weeks': lambda: None,
        'max_52_weeks': lambda: None,
        'pvp': lambda: text_to_number(get_substring(data, 'title="P/VP">P/VP</span>', '</span>', patterns_to_remove)),
        'dy': lambda: text_to_number(get_substring(data, 'DY (12M)</span>', '</span>', patterns_to_remove)),
        'latests_dividends': lambda: text_to_number(get_substring(get_substring(data, 'YIELD 12 MESES', '</div>'), 'amount">', '</span>', patterns_to_remove)),
        'latest_dividend': lambda: text_to_number(get_substring(data, 'ÚLTIMO RENDIMENTO', '</div>', patterns_to_remove)),
        'ffoy': lambda: None,
        'vacancy': lambda: text_to_number(get_substring(data, 'VACÂNCIA', '<div class=\'cell\'>', patterns_to_remove)),
        'total_real_state': lambda: count_pattern_on_text(get_substring(data, 'Lista de Imóveis', '</section>'), 'card-propertie'),
        'total_real_state_value': lambda: None,
        'total_mortgage': lambda: None,
        'total_mortgage_value': lambda: None,
        'total_stocks_fund_others': lambda: None,
        'total_stocks_fund_others_value': lambda: None,
        'management': lambda: get_substring(data, 'TIPO DE GESTÃO', '<div class=\'cell\'>', patterns_to_remove),
        'cash_value': lambda: None,
        'assets_value': lambda: None,
        'market_value': lambda: None,
        'initial_date': lambda: None,
        'target_public': lambda: get_substring(data, 'PÚBLICO-ALVO', '<div class=\'cell\'>', patterns_to_remove),
        'term': lambda: get_substring(data, 'PRAZO DE DURAÇÃO', '<div class=\'cell\'>', patterns_to_remove),
        'debit_by_real_state_acquisition': lambda: None,
        'debit_by_securitization_receivables_acquisition': lambda: None
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

        log(f"Converted Investidor 10 data: {convert_investidor10_data(html_page, info_names)}")
        return convert_investidor10_data(html_page, info_names)
    except Exception as error:
        log(f"Error on get Investidor 10 data: {traceback.format_exc()}", ERROR)

    return None

def get_data_from_all_sources(ticker, info_names):
    data_fundamentus = get_data_from_fundamentus(ticker, info_names)
    log(f'Data from Fundamentus: {data_fundamentus}', INFO)

    blank_fundamentus_info_names = [ info for info in info_names if not data_fundamentus.get(info, False) ]
    log(f'Blank Fundamentus names: {blank_fundamentus_info_names}')

    if data_fundamentus and not blank_fundamentus_info_names:
        return data_fundamentus

    data_fiis = get_data_from_fiis(ticker, blank_fundamentus_info_names if blank_fundamentus_info_names else info_names)
    log(f'Data from FIISs: {data_fiis}', INFO)

    if data_fundamentus and data_fiis:
        data_fundamentus_or_fiis = { **data_fundamentus, **data_fiis }
        log(f'From Fundamentus and FIIs: {data_fundamentus_or_fiis}')
    elif data_fundamentus and not data_fiis:
        data_fundamentus_or_fiis = data_fundamentus
        log(f'From Fundamentus: {data_fundamentus_or_fiis}')
    elif not data_fundamentus and data_fiis:
        data_fundamentus_or_fiis = data_fiis
        log(f'From FIIs: {data_fundamentus_or_fiis}')
    else:
        data_fundamentus_or_fiis = {}
        log(f'From None: {data_fundamentus_or_fiis}')

    blank_fundamentus_or_fiis_info_names = [ info for info in info_names if not data_fundamentus_or_fiis.get(info, False) ]
    log(f'Blank Fundamentus or FIIs names: {blank_fundamentus_or_fiis_info_names}')

    if data_fundamentus_or_fiis and not blank_fundamentus_or_fiis_info_names:
        return data_fundamentus_or_fiis

    data_investidor_10 = get_data_from_investidor10(ticker, blank_fundamentus_or_fiis_info_names if blank_fundamentus_or_fiis_info_names else info_names)
    log(f'Data from Investidor 10: {data_investidor_10}', INFO)

    if not data_investidor_10:
        return data_fundamentus_or_fiis

    return { **data_fundamentus_or_fiis, **data_investidor_10 }

def request_shares(ticker, source, info_names, cached_data):
    blank_cached_data_info_names = info_names

    if cached_data:
        blank_cached_data_info_names = [ info for info in info_names if not cached_data.get(info, False) ]
        log(f'Blank cache names: {blank_cached_data_info_names}')

        if not blank_cached_data_info_names:
            return cached_data

    if source == VALID_SOURCES['FUNDAMENTUS_SOURCE']:
        return get_data_from_fundamentus(ticker, blank_cached_data_info_names)
    elif source == VALID_SOURCES['FIIS_SOURCE']:
        return get_data_from_fiis(ticker, blank_cached_data_info_names)
    elif source == VALID_SOURCES['FUNDSEXPLORER_SOURCE']:
        return get_data_from_fundsexplorer(ticker, blank_cached_data_info_names)
    elif source == VALID_SOURCES['INVESTIDOR10_SOURCE']:
        return get_data_from_investidor10(ticker, blank_cached_data_info_names)

    return get_data_from_all_sources(ticker, blank_cached_data_info_names)

def do_cache_exists():
    return os.path.exists(CACHE_FILE)

def delete_cache():
    if not do_cache_exists():
        log('No cache to delete', INFO)
        return

    log('Deleting cache')

    os.remove(CACHE_FILE)

    log('Cache deletion completed', INFO)

def clear_cache(ticker):
    if not do_cache_exists():
        log('No cache to clean', INFO)
        return

    log('Cleaning cache')

    with open(CACHE_FILE, 'w+') as cache_file:
        lines = cache_file.readlines()

        for line in lines:
            if not line.startswith(ticker):
                cache_file.write(line)

    log('Cache cleaning completed', INFO)

def read_cache(ticker):
    if not do_cache_exists():
        log('No cache to read', INFO)
        return None

    log('Reading cache')

    with open(CACHE_FILE, 'r') as cache_file:
        for line in cache_file:
            if line.startswith(ticker):
                _, cached_datetime, data = line.strip().split('#@#')

                cached_date = datetime.strptime(cached_datetime, DATE_FORMAT)

                if datetime.now() - cached_date <= CACHE_EXPIRY:
                    log(f'Cache reading completed, cache found: Date: {cached_datetime}', INFO)
                    return ast.literal_eval(data)
                else:
                    log(f'Cache reading completed, cache expired: Date: {cached_datetime}', INFO)                    
                    clear_cache(ticker)
                    return None

        log('Cache reading completed, cache not found')
        return None

def write_to_cache(ticker, data):
    log('Writing cache')

    with open(CACHE_FILE, 'a') as cache_file:
        cache_data = f'{ticker}#@#{datetime.now().strftime(DATE_FORMAT)}#@#{data}'
        cache_file.write(f'{cache_data}\n')
        log(f'Writed cache data: {cache_data}')

    log('Cache writing completed', INFO)

def get_from_cache(ticker, should_delete_all_cache, should_clear_cached_data, should_use_cache):
    if should_delete_all_cache:
        delete_cache()

    if should_clear_cached_data and not should_delete_all_cache:
        clear_cache(ticker)

    can_use_cache = should_use_cache and not (should_delete_all_cache or should_clear_cached_data)

    cached_data = None

    if can_use_cache:
        cached_data = read_cache(ticker)
        log(f'Data from Cache: {cached_data}')

    return cached_data, can_use_cache

def get_parameter_info(request_parameters, parameter_name, dafault=None):
    return request_parameters.get(parameter_name, dafault).replace(' ', '').lower()

def get_cache_parameter_info(request_parameters, cache_parameter_name, dafault='0'):
    return get_parameter_info(request_parameters, cache_parameter_name, dafault) in ('1', 's', 'sim', 'y', 'yes', 't', 'true')

@app.route('/fii/<ticker>', methods=['GET'])
def get_fii_data(ticker):
    should_delete_all_cache = get_cache_parameter_info(request.args, 'should_delete_all_cache')
    should_clear_cached_data = get_cache_parameter_info(request.args, 'should_clear_cached_data')
    should_use_cache = get_cache_parameter_info(request.args, 'should_use_cache', '1')

    raw_source = get_parameter_info(request.args, 'source', VALID_SOURCES['ALL_SOURCE'])
    source = raw_source if raw_source in VALID_SOURCES.values() else VALID_SOURCES['ALL_SOURCE']

    raw_info_names = [ info for info in get_parameter_info(request.args, 'info_names', '').split(',') if info in VALID_INFOS ]
    info_names = raw_info_names if len(raw_info_names) else VALID_INFOS

    log(f'Should Delete cache? {should_delete_all_cache} - Should Clear cache? {should_clear_cached_data} - Should Use cache? {should_use_cache}')
    log(f'Ticker: {ticker} - Source: {source} - Info names: {info_names}')

    cached_data, can_use_cache = get_from_cache(ticker, should_delete_all_cache, should_clear_cached_data, should_use_cache)

    data = request_shares(ticker, source, info_names, cached_data)

    log(f'Final Data: {data}', INFO)

    if can_use_cache:
        write_to_cache(ticker, data)

    return jsonify({'data': data, 'source': 'fresh', 'date': datetime.now().strftime("%d/%m/%Y, %H:%M")}), 200

if __name__ == '__main__':
    log('Starting fiiCrawler API', INFO)
    app.run(debug=LOG_LEVEL == DEBUG)
