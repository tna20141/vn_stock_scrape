from lxml import html
import asyncio
import aiohttp
import datetime
import utils

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://www.stockbiz.vn',
    'Referer': 'https://www.stockbiz.vn/Stocks/AAA/HistoricalQuotes.aspx',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'TE': 'Trailers',
}
clientSession = aiohttp.ClientSession(headers=headers)

def cleanup():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(clientSession.close())

async def _getPage(symbol, fromTime, toTime, page):
    toTime = (toTime or datetime.datetime.now())
    data = {
        'Cart_ctl00_webPartManager_wp1770166562_wp1427611561_callbackData_Callback_Param': [
            fromTime.strftime('%Y-%-m-%-d'),
            toTime.strftime('%Y-%-m-%-d'),
            page,
        ],
    }
    async with clientSession.post(
        'https://www.stockbiz.vn/Stocks/{symbol}/HistoricalQuotes.aspx'.format(symbol=symbol),
        data=data,
    ) as response:
        tree = html.fromstring(await response.text())
        rows = tree.xpath('//tr[position() > 1]')
        return list(map(_getQuoteFromRow, rows))

def _getQuoteFromRow(row):
    return {
        'date': datetime.datetime.strptime(row.xpath('td[1]/text()')[0].strip(), '%d/%m/%Y'),
        'opening_price': int(utils.strip(row.xpath('td[3]/text()')[0], ' ,')) * 10,
        'max_price': int(utils.strip(row.xpath('td[4]/text()')[0], ' ,')) * 10,
        'min_price': int(utils.strip(row.xpath('td[5]/text()')[0], ' ,')) * 10,
        'closing_price': int(utils.strip(row.xpath('td[6]/text()')[0], ' ,')) * 10,
        'avg_price': int(utils.strip(row.xpath('td[7]/text()')[0], ' ,')) * 10,
        'volume': int(utils.strip(row.xpath('td[9]/text()')[0], ' .')),
    }

async def get(symbol, fromTime, toTime):
    async def __getPage(page):
        return await _getPage(symbol, fromTime, toTime, page)
    return await utils.scanPagesA(__getPage)

# print(utils.eachLimit(
#     2,
#     [
#         'AAA',
#         'AAV',
#         'AAM',
#     ],
#     lambda symbol: get(symbol, datetime.datetime(2021, 1, 1), datetime.datetime(2021, 2, 10))
# ))

# import asyncio
# async def a(page):
#     return await _getPage('AAA', datetime.datetime(2021, 1, 1), datetime.datetime(2021, 2, 10), page)
# print(asyncio.run(utils.scanPagesA(a)))
# # print(asyncio.run(_getPage('AAA', datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 10), 1)))
