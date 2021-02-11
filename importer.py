import datetime

import storage
import ticker
import utils

def doImport(fromTime=None, toTime=None):
    importSymbols()
    symbols = storage.getAll('symbols')
    count = 0
    async def _importTicks(symbolRecord):
        try:
            step = 0
            nonlocal count
            count += 1
            _fromTime = fromTime or symbolRecord['start_date']
            print('Importing ' + str(symbolRecord['symbol']) + ' (' + str(count) + '/' + str(len(symbols))  + ') ' + str(_fromTime))
            step = 1
            ticks = await ticker.get(symbolRecord['symbol'], _fromTime, toTime)
            step = 2
            records = list(map(lambda tick: _tickToRecord(symbolRecord['symbol'], tick), ticks))
            step = 3
            await storage.bulkUpsertA('ticks', records, False)
            step = 4
            print('done ' + symbolRecord['symbol'] + ' (' + str(len(ticks)) + ')')
        except Exception as e:
            print('Error importing ' + symbolRecord['symbol'] + ' at step ' + str(step) + ': ' + str(e))
            raise e
    utils.eachLimit(
        5,
        symbols,
        _importTicks,
    )

def importSymbols():
    _importSymbols('hnx')
    _importSymbols('hose')
    _importSymbols('upcom')

def _importSymbols(exchange):
    if exchange == 'hnx':
        import hnx_symbols as symbolsModule
        symbolToRecord = _hnxSymbolToRecord
    elif exchange == 'upcom':
        import upcom_symbols as symbolsModule
        symbolToRecord = _upcomSymbolToRecord
    elif exchange == 'hose':
        import hose_symbols as symbolsModule
        symbolToRecord = _hoseSymbolToRecord
    symbols = symbolsModule.get()
    records = list(map(symbolToRecord, symbols))
    return storage.bulkUpsert('symbols', records, False)

def _hnxSymbolToRecord(item):
    return {
        'filter': {
            'symbol': item['symbol'],
            'exchange': 'hnx',
        },
        'update': {
            'name': item['name'],
            'category': item['category'],
            'start_date': item['start_date'],
            'total_listing_quantity': item['total_listing_quantity'],
            'total_listing_value': item['total_listing_value'],
        }
    }

def _upcomSymbolToRecord(item):
    return {
        'filter': {
            'symbol': item['symbol'],
            'exchange': 'upcom',
        },
        'update': {
            'name': item['name'],
            'start_date': item['start_date'],
            'total_listing_quantity': item['total_listing_quantity'],
            'total_listing_value': item['total_listing_value'],
        }
    }

def _hoseSymbolToRecord(item):
    return {
        'filter': {
            'symbol': item['symbol'],
            'exchange': 'hose',
        },
        'update': {
            'name': item['name'],
            'isin': item['isin'],
            'figi': item['figi'],
            'start_date': item['start_date'],
            'total_listing_quantity': item['total_listing_quantity'],
            'total_listing_value': item['total_listing_value'],
        }
    }

def _tickToRecord(symbol, tick):
    return {
        'filter': {
            'symbol': symbol,
            'date': tick['date'],
        },
        'update': {
            'opening_price': tick['opening_price'],
            'max_price': tick['max_price'],
            'min_price': tick['min_price'],
            'closing_price': tick['closing_price'],
            'avg_price': tick['avg_price'],
            'volume': tick['volume'],
        }
    }

# doImport()
