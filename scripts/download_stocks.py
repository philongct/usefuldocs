from datetime import datetime
from urllib.request import urlopen
import json
import threading
import os
import shutil

########################## params ############################
save_folder = 'C:/Stocks'
# source vndirect or ssi
# - vndirect contains data from {from_date} to {to_date}
# - ssi contains data from 2009 to current date
source = 'ssi'
from_date = datetime(2010, 1, 1)
to_date = datetime(2018, 8, 7, 23, 59)
###############################################################

# constants
STOCK_SYMBOLS_URL = 'https://finfoapi-hn.vndirect.com.vn/stocks'

STOCK_DATA_VNDIRECT_URL = 'https://dchart-api.vndirect.com.vn/dchart/history?symbol={}&resolution=D&from={}&to={}'
STOCK_DATA_SSI_URL = 'http://ivt.ssi.com.vn/Handlers/DownloadHandler.ashx?Download=1&Ticker={}'

STORAGE = save_folder + '/' + from_date.strftime("%Y-%m-%d_%H%M") + ' ' + to_date.strftime("%Y-%m-%d_%H%M")

N_THREADS = 4

def group_by_floors(stocks):
    floors = {}
    for stock in stocks:
        floor = floors.get(stock['floor'], [])
        floor.append(stock['symbol'])
        floors[stock['floor']] = floor

    for floor, symbols in floors.items():
        with open(STORAGE + '/' + floor + '.csv', 'w') as outfile:
            outfile.write('<TICKER>,<DTYYYYMMDD>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>\n')
            for symbol in symbols:
                with open(STORAGE + '/' + symbol) as infile:
                    outfile.write(infile.read())
                os.remove(STORAGE + '/' + symbol)

def save_symbol_vndirect(symbol, data):
    json_data = json.loads(data)
    linef = '{},{:%Y%m%d},{:.3f},{:.3f},{:.3f},{:.3f},{:.0f}\n'

    with open(STORAGE + '/' + symbol, 'w') as f:
        length = len(json_data['t'])
        for i in range(length):
            time = datetime.fromtimestamp(json_data['t'][i])
            f.write(linef.format(symbol, time, json_data['o'][i], json_data['h'][i], json_data['l'][i], json_data['c'][i], json_data['v'][i]))


def download_symbol_vndirect(symbol):    
    from_ts = "{0:.0f}".format(from_date.timestamp())
    to_ts = "{0:.0f}".format(to_date.timestamp())

    url = STOCK_DATA_VNDIRECT_URL.format(symbol, from_ts, to_ts)
    with urlopen(url) as symbol_res:
        print('{} {}'.format(url, symbol_res.status))
        if symbol_res.status == 200:
            save_symbol_vndirect(symbol, symbol_res.read())

def save_symbol_ssi(symbol, res_data):
    linef = '{},{:%Y%m%d},{},{},{},{},{}\n'

    with open(STORAGE + '/' + symbol, 'w') as f:
        count = 0
        for line in res_data.decode('utf-8').splitlines():
            if count == 0:
                count += 1
                continue
                
            elements = line.split(',')
            time = datetime.strptime(elements[0], '%d/%m/%Y')
            f.write(linef.format(symbol, time, elements[2], elements[4], elements[5], elements[3], elements[6]))
                

def download_symbol_ssi(symbol):
    url = STOCK_DATA_SSI_URL.format(symbol)
    with urlopen(url) as symbol_res:
        print('{} {}'.format(url, symbol_res.status))
        if symbol_res.status == 200:
            save_symbol_ssi(symbol, symbol_res.read())

def download_stocks(stocks, download_function):
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
            download_function(stock['symbol'])

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

    download_functions = {
        'vndirect': download_symbol_vndirect,
        'ssi': download_symbol_ssi
    }

    download_f = download_functions.get(source, lambda s: 'Not implemented {}'.format(s))

    with urlopen(STOCK_SYMBOLS_URL) as stocks_res:
        if stocks_res.status == 200:
            data = stocks_res.read()
            stocks = json.loads(data)['data']

            download_stocks(stocks, download_f)

