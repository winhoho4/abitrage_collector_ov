import ccxt.pro as ccxtpro
import asyncio
import os
import zmq
import zmq.asyncio
import json
import signal
import zstandard as zstd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

log_pre_fix = f'./data/{os.path.splitext(os.path.basename(__file__))[0]}'

# Ensure the data directory exists
os.makedirs('./data', exist_ok=True)

# Global variables for signal handling
log_file_handler = None
cctx = None
running = True

def get_current_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

async def watch_tickers(exchange, symbol, exchange_name, ticker_data, coin):
    while running:
        try:
            order_book = await exchange.watch_order_book(symbol)
            ticker_data[coin][exchange_name] = {
                'datetime': order_book['datetime'],
                'timestamp': order_book['timestamp'],
                'bid': order_book['bids'][0][0] if order_book['bids'] else None,
                'ask': order_book['asks'][0][0] if order_book['asks'] else None
            }
        except Exception as e:
            print(get_current_datetime(), 'error', symbol, exchange_name, coin, e)

async def publish_ticker_data(ticker_data, exchanges, coins, publisher):
    global log_file_handler, cctx, running
    current_date = datetime.now().strftime('%Y%m%d')
    log_file = f"{log_pre_fix}_sum_{current_date}.log.zst"
    dctx = zstd.ZstdCompressor()

    def open_log_file(date):
        log_file_name = f"{log_pre_fix}_sum_{date}.log.zst"
        f = open(log_file_name, 'ab')  # 'ab' 모드로 파일을 열어 추가 기록 가능하도록 설정
        return f, dctx.stream_writer(f)

    log_file_handler, cctx = open_log_file(current_date)

    while running:
        await asyncio.sleep(1)  # Adjust the interval as needed

        # Check if the date has changed
        new_date = datetime.now().strftime('%Y%m%d')
        if new_date != current_date:
            cctx.close()
            log_file_handler.close()
            current_date = new_date
            log_file_handler, cctx = open_log_file(current_date)

        for coin in coins:
            for base_exchange in exchanges:
                for compare_exchange in exchanges:
                    if base_exchange != compare_exchange:
                        base_timestamp = ticker_data[coin][base_exchange]['timestamp']
                        compare_timestamp = ticker_data[coin][compare_exchange]['timestamp']
                        base_bid = ticker_data[coin][base_exchange]['bid']
                        compare_ask = ticker_data[coin][compare_exchange]['ask']
                        
                        if base_bid is not None and compare_ask is not None:
                            diff = ((base_bid - compare_ask) / compare_ask) * 100
                            prefix = f"{coin}:{base_exchange}:{compare_exchange}"
                            message = {
                                'symbol': coin,
                                'base_exchange': base_exchange,
                                'compare_exchange': compare_exchange,
                                'base_bid': base_bid,
                                'compare_ask': compare_ask,
                                'base_time': base_timestamp,
                                'compare_time': compare_timestamp,
                            }
                            publisher.send_string(str(message))
                            cctx.write((str(message) + '\n').encode('utf-8'))

                            # Check timestamp difference and print warning if needed
                            if abs(base_timestamp - compare_timestamp) > 30000:
                                warning_message = f"WARNING: Time difference between {base_exchange} and {compare_exchange} for {coin} is more than 30 seconds."
                                print(get_current_datetime(), warning_message)
                                #cctx.write((warning_message + '\n').encode('utf-8'))

async def relay_messages(publisher):
    context = zmq.asyncio.Context()
    receiver = context.socket(zmq.PULL)
    receiver.bind("tcp://*:6001")

    while True:
        message = await receiver.recv_string()
        publisher.send_string(message)

def handle_signal(signal, frame):
    global running, log_file_handler, cctx
    print(get_current_datetime(), "Signal received, shutting down...")
    running = False
    if cctx:
        cctx.close()
    if log_file_handler:
        log_file_handler.close()
    asyncio.get_event_loop().stop()

def setup_signal_handlers():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

def create_exchange_instance(exchange_name):
    try:
        exchange_class = getattr(ccxtpro, exchange_name)
        return exchange_class({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future'
            }
        })
    except AttributeError:
        print(get_current_datetime(), f"Exchange {exchange_name} not found in ccxtpro.")
        return None

async def main_loop():
    setup_signal_handlers()
    #context = zmq.Context()
    context = zmq.asyncio.Context()
    publisher = context.socket(zmq.PUB)
    publisher.bind("tcp://*:6000")

    coins = os.getenv('OV_COINS', 'BTC,ETH,BCH,ETC,SOL').split(',')
    exchanges = os.getenv('OV_EXCHANGES', 'binance,bybit,okx,bitget,hyperliquid').split(',')

    exchange_instances = {exchange: create_exchange_instance(exchange) for exchange in exchanges if create_exchange_instance(exchange)}

    ticker_data = {coin: {exchange: {'bid': None, 'ask': None, 'timestamp': None} for exchange in exchanges} for coin in coins}

    tasks = []
    for coin in coins:
        symbol = f"{coin}/USDT"
        hyperliquid_symbol = f"{coin}/USDC:USDC"
        for exchange_name, exchange in exchange_instances.items():
            if exchange_name == 'hyperliquid':
                tasks.append(watch_tickers(exchange, hyperliquid_symbol, exchange_name, ticker_data, coin))
            else:
                tasks.append(watch_tickers(exchange, symbol, exchange_name, ticker_data, coin))

    tasks.append(publish_ticker_data(ticker_data, exchanges, coins, publisher))
    tasks.append(relay_messages(publisher))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main_loop())

