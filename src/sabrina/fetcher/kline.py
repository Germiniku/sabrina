import asyncio
from ccxt.pro.binance import binance
from questdb.ingress import Sender, TimestampNanos
from loguru import logger
from datetime import datetime
conf = f'http::addr=localhost:9000;'


class KlineWs:
    timeframe = "1m"

    def __init__(self):
        pass

    async def start(self):
        return asyncio.create_task(self._start_ws())

    async def _start_ws(self):
        ex = binance()
        ex.options["defaultType"] = "swap"
        await ex.load_markets()
        symbols = await self.filter_futures_symbols(ex)
        subscriptions = [[symbol, self.timeframe] for symbol in symbols[:40]]
        #subscriptions = [['BTC/USDT','1m']]
        logger.info(f"订阅K线币种 {subscriptions}")
        while True:
            ohlcvs = await ex.watch_ohlcv_for_symbols(subscriptions, limit=50)
            #print("======>",ohlcvs)
            await self.consume(ohlcvs)
            await ex.sleep(1000)

    async def filter_futures_symbols(self,ex: binance):
        symbols = []
        for market in ex.markets.values():
            if market["type"] == "swap" and market["swap"] is True and market["subType"] == "linear":
                symbols.append(f"{market['base']}/USDT")
        return symbols

    async def consume(self, ohlcvs):
        with Sender.from_conf(conf) as sender:
            for symbol, klines in ohlcvs.items():
                for timeframe, items in klines.items():
                    for item in items:
                        ts, open, high, low, close, volume = (
                            item[0],
                            item[1],
                            item[2],
                            item[3],
                            item[4],
                            item[5]
                        )
                        sender.row(
                            "kline",
                            symbols={
                                "symbol": symbol,
                                "timeframe": timeframe,
                            },
                            columns={
                                "open": open,
                                "high": high,
                                "low": low,
                                "close": close,
                                "volume": volume,
                            },
                            at=datetime.fromtimestamp(ts/1000),
                        )

            sender.flush()
