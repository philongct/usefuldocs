import datetime
from urllib.request import urlopen
import json
import threading
import os
import shutil

# params
save_folder = 'C:/Stocks'
from_date = datetime.datetime(2010, 1, 1)
to_date = datetime.datetime(2018, 8, 7, 23, 59)

# constants
STOCK_SYMBOLS_URL = 'https://finfoapi-hn.vndirect.com.vn/stocks'
STOCK_DATA_URL = 'https://dchart-api.vndirect.com.vn/dchart/history?symbol={SYM}&resolution=D&from={from}&to={to}'
STORAGE = save_folder + '/' + from_date.strftime("%Y-%m-%d_%H%M") + ' ' + to_date.strftime("%Y-%m-%d_%H%M")

N_THREADS = 4

def group_by_floors(stocks):
    floors = {}
    for stock in stocks:
        floor = floors.get(stock['floor'], [])
        floor.append(stock['symbol'])
        floors[stock['floor']] = floor

    for floor, symbols in floors.items():
        with open(STORAGE + '/' + floor, 'w') as outfile:
            outfile.write('<TICKER>,<DTYYYYMMDD>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>\n')
            for symbol in symbols:
                with open(STORAGE + '/' + symbol) as infile:
                    outfile.write(infile.read())
                os.remove(STORAGE + '/' + symbol)

def save_symbol(symbol, data):
    json_data = json.loads(data)
    linef = '{},{},{:.3f},{:.3f},{:.3f},{:.3f},{:.0f}\n'

    with open(STORAGE + '/' + symbol, 'w') as f:
        length = len(json_data['t'])
        for i in range(length):
            time = datetime.datetime.fromtimestamp(json_data['t'][i])
            f.write(linef.format(symbol, time.strftime("%Y%m%d"), json_data['o'][i], json_data['h'][i], json_data['l'][i], json_data['c'][i], json_data['v'][i]))


def download_symbol(symbol):    
    from_ts = "{0:.0f}".format(from_date.timestamp())
    to_ts = "{0:.0f}".format(to_date.timestamp())

    url = STOCK_DATA_URL.replace('{SYM}', symbol).replace('{from}', from_ts).replace('{to}', to_ts)
    with urlopen(url) as symbol_res:
        print('{} {}'.format(url, symbol_res.status))
        if symbol_res.status == 200:
            save_symbol(symbol, symbol_res.read().decode('utf-8'))


def download_stocks(stocks):
    lock = threading.RLock()
    counter = {}
    counter['stocks'] = stocks
    counter['idx'] = len(stocks)

    def download_stock():
        while counter['idx'] > 0:
            lock.acquire()
            counter['idx'] -= 1
            lock.release()

            stock = counter['stocks'][counter['idx']]
            download_symbol(stock['symbol'])

    threads = []
    for i in range(N_THREADS):
        t = threading.Thread(target=download_stock)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print('Grouping...')
    group_by_floors(stocks)

if __name__== "__main__":
    shutil.rmtree(STORAGE, True)
    os.makedirs(STORAGE)

    with urlopen(STOCK_SYMBOLS_URL) as stocks_res:
        if stocks_res.status == 200:
            data = stocks_res.read().decode('utf-8')
            stocks = json.loads(data)['data']
            #print(stocks)
            download_stocks(stocks)

