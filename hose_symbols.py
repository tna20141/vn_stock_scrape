import requests
import datetime
import utils
from urllib.parse import urlencode

def _getPage(page):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0',
    }
    query = {
        'pageFieldName1': 'Code',
        'pageFieldValue1': '',
        'pageFieldOperator1': 'eq',
        'pageFieldName2': 'Sectors',
        'pageFieldValue2': '',
        'pageFieldOperator2': '',
        'pageFieldName3': 'Sector',
        'pageFieldValue3': '00000000-0000-0000-0000-000000000000',
        'pageFieldOperator3': '',
        'pageFieldName4': 'StartWith',
        'pageFieldValue4': '',
        'pageFieldOperator4': '',
        '_search': 'false',
        'rows': 30,
        'page': page,
        'sidx': 'id',
        'sord': 'desc',
        'pageCriteriaLength': 4,
    }
    queryString = urlencode(query)
    page = requests.get('https://www.hsx.vn/Modules/Listed/Web/SymbolList?' + queryString, headers=headers)
    content = page.json()
    return list(map(_getSecurityFromRow, content['rows']))

def _getSecurityFromRow(row):
    return {
        'symbol': row['cell'][1],
        'isin': row['cell'][2],
        'figi': row['cell'][3],
        'name': row['cell'][4],
        'start_date': datetime.datetime.strptime(row['cell'][7], '%d/%m/%Y'),
        'total_listing_quantity': _getNumber(row['cell'][5]),
        'total_listing_value': _getNumber(row['cell'][6]),
    }

def _getNumber(string):
    return int(utils.strip(string.split(',')[0], '.,'))

def get():
    return utils.scanPages(_getPage)
