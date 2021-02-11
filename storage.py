import pymongo
import datetime
import asyncio
import motor.motor_asyncio

connectionString = 'mongodb://localhost:27017/'

myclient = pymongo.MongoClient(connectionString)
asynclient = motor.motor_asyncio.AsyncIOMotorClient(connectionString)

mydb = myclient['vn_stock_scrape']
asyncdb = asynclient['vn_stock_scrape']

def upsert(collection, filter, update=None):
    if (update == None):
        update = { '$set': filter }
    else:
        update = { '$set': update }
    return mydb[collection].update_one(filter, update, upsert=True)

def _transformUpdateOne(specs):
    if (specs['update'] == None):
        update = { '$set': specs['filter'] }
    else:
        update = { '$set': specs['update'] }
    return pymongo.UpdateOne(specs['filter'], update, upsert=True)

def bulkUpsert(collection, arrays, simple=True):
    return mydb[collection].bulk_write(_getBulkUpsertOperations(arrays, simple))

async def bulkUpsertA(collection, arrays, simple=True):
    await asyncdb[collection].bulk_write(_getBulkUpsertOperations(arrays, simple), ordered=False)

def _getBulkUpsertOperations(arrays, simple):
    if simple:
        return list(map(lambda item: pymongo.UpdateOne(item, { '$set': item }, upsert=True), arrays))
    return list(map(_transformUpdateOne, arrays))

def getAll(collection):
    return list(mydb[collection].find({}))

# import utils
# utils.eachLimit(1, [
#     [
#         {
#             'filter':{ 'x': 2, 'y': 'aa', 'l': datetime.datetime(2021, 1, 30, 9, 36, 52, 149116) },
#             'update': { 'z': 1 },
#         },
#         {
#             'filter':{ 'x': 2, 'y': 'bb', 'l': datetime.datetime(2021, 1, 31, 9, 36, 52, 149116) },
#             'update': { 'z': 2 },
#         }
#     ],
#     [
#         {
#             'filter':{ 'x': 2, 'y': 'c', 'l': datetime.datetime(2021, 1, 30, 9, 36, 52, 149116) },
#             'update': { 'z': 3 },
#         },
#         {
#             'filter':{ 'x': 2, 'y': 'd', 'l': datetime.datetime(2021, 1, 31, 9, 36, 52, 149116) },
#             'update': { 'z': 4 },
#         }
#     ],
#     [
#         {
#             'filter':{ 'x': 2, 'y': 'e', 'l': datetime.datetime(2021, 1, 30, 9, 36, 52, 149116) },
#             'update': { 'z': 5 },
#         },
#         {
#             'filter':{ 'x': 2, 'y': 'f', 'l': datetime.datetime(2021, 1, 31, 9, 36, 52, 149116) },
#             'update': { 'z': 6 },
#         }
#     ],
# ], lambda item: bulkUpsertA('test', item, False))

# loop = asyncio.get_event_loop()
# loop.run_until_complete(bulkUpsertA(
#     'test',
#     [
#         {
#             'filter':{ 'x': 2, 'y': 'aa', 'l': datetime.datetime(2021, 1, 30, 9, 36, 52, 149116) },
#             'update': { 'zzz': 4 },
#         },
#         {
#             'filter':{ 'x': 2, 'y': 'zz', 'l': datetime.datetime(2021, 1, 31, 9, 36, 52, 149116) },
#             'update': { 'zzz': 3 },
#         }
#     ],
#     simple=False,
# ))

# bulkUpsert('test', [
#     {
#         'filter':{ 'x': 1, 'y': 'aa', 'l': datetime.datetime(2021, 1, 30, 9, 36, 52, 149116) },
#         'update': { 'zzz': 4 },
#     },
#     {
#         'filter':{ 'x': 1, 'y': 'aa', 'l': datetime.datetime(2021, 1, 31, 9, 36, 52, 149116) },
#         'update': { 'zzz': 3 },
#     }
# ], simple=False)

#upsert('test', { 'x': 1, 'y': 'aa', 'l': datetime.datetime(2021, 1, 30, 9, 36, 52, 149116) }, { 'z': { 'm': 4 } })
