import asyncio
from asyncio_pool import AioPool

def strip(string, chars):
    return string.translate({ord(i): None for i in chars})

def scanPages(getPage):
    page = 1
    data = []
    while True:
        dataThisPage = getPage(page)
        if (len(dataThisPage)) == 0:
            break
        data.extend(dataThisPage)
        page += 1
    return data

def eachLimit(limit, items, func):
    async def _eachLimit():
        pool = AioPool(size=limit)
        await pool.map(func, items)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_eachLimit())

async def scanPagesA(getPage):
    page = 1
    data = []
    while True:
        dataThisPage = await getPage(page)
        if (len(dataThisPage)) == 0:
            break
        data.extend(dataThisPage)
        page += 1
    return data
