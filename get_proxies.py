# https://proxybroker.readthedocs.io/en/latest/examples.html

import asyncio
from proxybroker import Broker

proxy_list = []

async def show(proxies):
    while True:
        proxy = await proxies.get()
        if proxy is None: break
        print('Found proxy: %s' % proxy)
        proxy_list.append(proxy)

proxies = asyncio.Queue()
broker = Broker(proxies)
tasks = asyncio.gather(
    broker.find(types=['HTTP', 'HTTPS'], limit=10),
    show(proxies))

loop = asyncio.get_event_loop()
loop.run_until_complete(tasks)

proxy_json_0 = proxy_list[0].as_json()
proxy_string = proxy_json_0['host'] + ':' + str(proxy_json_0['port'])
