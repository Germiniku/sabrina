import asyncio
from ccxt.pro.binance import binance
from questdb.ingress import Sender, TimestampNanos

conf = ""


class KlineWs:
    timeframe = "1m"

    def __init__(self):
        pass

    async def start(self):
        return asyncio.create_task(self._start_ws())

    async def _start_ws(self):
        ex = binance()
        await ex.load_markets()
        symbols = await self.filter_futures_symbols(ex, self.paris)
        subscriptions = [[symbol, self.timeframe] for symbol in symbols]
        while True:
            ohlcvs = await ex.watch_ohlcv_for_symbols(subscriptions, limit=5)
            self.consume(ohlcvs)
            await ex.sleep(1000 * 60)

    async def filter_futures_symbols(ex: binance):
        symbols = []
        for market in ex.markets.values():
            if market["type"] != "swap":
                continue
            symbols.append(market["symbol"])
        return symbols

    async def consume(self, ohlcvs):
        with Sender.from_conf(conf) as sender:
            for symbol, klines in ohlcvs.items():
                for timeframe, items in klines.items():
                    for item in items:
                        open, high, low, close, volume = (
                            item[0],
                            item[1],
                            item[2],
                            item[3],
                            item[4],
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
                            at=TimestampNanos.now(),
                        )

            sender.flush()
