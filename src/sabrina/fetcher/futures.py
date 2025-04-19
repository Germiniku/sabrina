from datetime import datetime
from ccxt import binance
from loguru import logger
from questdb.ingress import Sender
from concurrent.futures import ThreadPoolExecutor
conf = f'http::addr=localhost:9000;'

def fetch_ratio(symbol, ex):
    logger.info(f"采集币种：{symbol}")
    try:
        return ex.fapiDataGetTopLongShortPositionRatio(
            params={"symbol": symbol, "period": "5m", "limit": 3}
        )
    except Exception as e:
        logger.warning(f"{symbol} 获取失败: {e}")
        return []

def top_long_short_position_ratio():
    logger.info("采集合约市场大户持仓量")
  
    ex = binance()
    ex.enableRateLimit = False
    ex.options["maxRetriesOnFailure"] = 3
    ex.options["maxRetriesOnFailureDelay"] = 500
    symbols = [
        symbol.get("symbol") for symbol in
        ex.fapiPublicGetExchangeInfo().get("symbols")
    ]
    rows = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_ratio, symbol, ex) for symbol in symbols]
        for f in futures:
            rows.extend(f.result())
    
    with Sender.from_conf(conf) as sender:
        for row in rows:
            sender.row(
                "top_long_short_position_ratio",
                symbols={"symbol": row.get("symbol")},
                columns={
                    "logShortRatio": row.get("longShortRatio"),
                    "longAccount": row.get("longAccount"),
                    "shortAccount": row.get("shortAccount"),
                },
                at=datetime.fromtimestamp(int(row.get("timestamp")) / 1000),
            )
        sender.flush()




def fetch_account_ratio(symbol, ex: binance):
    logger.info(f"采集币种：{symbol}")
    try:
        return ex.fapiDataGetTopLongShortAccountRatio(
            params={"symbol": symbol, "period": "5m", "limit": 3}
        )
    except Exception as e:
        logger.warning(f"{symbol} 获取失败: {e}")
        return []

def top_long_short_account_ratio():
    logger.info("采集合约市场大户账户数多空比")
    ex = binance()
    ex.enableRateLimit = True
    symbols = [
        symbol.get("symbol") for symbol in
        ex.fapiPublicGetExchangeInfo().get("symbols")
    ]
    rows = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_account_ratio, symbol, ex) for symbol in symbols]
        for f in futures:
            rows.extend(f.result())

    with Sender.from_conf(conf) as sender:
        for row in rows:
            sender.row(
                "top_long_short_account_ratio",
                symbols={"symbol": row.get("symbol")},
                columns={
                    "logShortRatio": row.get("longShortRatio"),
                    "longAccount": row.get("longAccount"),
                    "shortAccount": row.get("shortAccount"),
                },
                at=datetime.fromtimestamp(int(row.get("timestamp")) / 1000),
            )
        sender.flush()




if  __name__ == '__main__':
    top_long_short_account_ratio()
