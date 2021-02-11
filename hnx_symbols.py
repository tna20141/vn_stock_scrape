from lxml import html
import requests
import datetime
import utils

def _getPage(page, token):
    data = {
        'p_issearch': 0,
        'p_orderby': 'STOCK_CODE',
        'p_ordertype': 'ASC',
        'p_currentpage': page,
        'p_record_on_page': 50,
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0',
        'X-Requested-With': 'XMLHttpRequest',
        '__RequestVerificationToken': token,
    }
    page = requests.post('https://www.hnx.vn/ModuleIssuer/List/ListSearch_Datas', data=data, headers=headers)
    content = page.json()['Content']
    tree = html.fromstring(content)
    rows = tree.xpath('//tbody/tr')
    return list(map(_getSecurityInfoFromRow, rows))

def _getSecurityInfoFromRow(row):
    return {
        'symbol': row.xpath('td[2]/a/text()')[0].strip(),
        'name': row.xpath('td[3]/a/text()')[0].strip(),
        'category': row.xpath('td[4]/text()')[0].strip(),
        'start_date': datetime.datetime.strptime(row.xpath('td[5]/text()')[0].strip(), '%d/%m/%Y'),
        # TODO: clarify the meaning of the below 2 fields
        'total_listing_quantity': int(utils.strip(row.xpath('td[6]/text()')[0], ' .,')),
        'total_listing_value': int(utils.strip(row.xpath('td[7]/text()')[0], ' .,')),
    }

def _getToken():
    page = requests.get('https://www.hnx.vn/cophieu-etfs/chung-khoan-ny.html')
    cookies = requests.utils.dict_from_cookiejar(page.cookies)
    return cookies['__RequestVerificationToken']

def get():
    token = _getToken()
    def __getPage(page):
        return _getPage(page, token)
    return utils.scanPages(__getPage)
